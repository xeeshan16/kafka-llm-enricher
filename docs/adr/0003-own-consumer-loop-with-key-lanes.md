# ADR 0003: An own consumer loop with key lanes instead of Spring Kafka listener containers

- Status: accepted
- Date: 2026-10-23

## Context

Days 1 and 2 ran every pipeline in a Spring Kafka `ConcurrentMessageListenerContainer` with a
record listener and one Kafka transaction per record. That made exactly-once easy, but an LLM call
blocks the listener thread, which is also the poll thread. Three problems follow:

- **Throughput is one call at a time per consumer.** A 1 s call caps a consumer at about 1 record
  per second. Adding consumers needs more partitions than most teams have.
- **A slow call holds up the whole partition.** One stuck call delays every record behind it, also
  those of unrelated keys.
- **A slow call can trigger a rebalance.** If a call (plus retries) outlasts
  `max.poll.interval.ms`, the consumer is expelled, its partitions move, and the work is done
  again elsewhere. Day 2's chaos test provoked exactly that.

What teams need is per-entity order (all events of one card or account in order), not
per-partition order, and a poll loop that keeps polling while calls run.

Options considered:

- **Spring Kafka batch listener plus an executor.** The listener would fan a batch out to threads
  and wait for all of it. It still blocks the poll thread for the slowest record of the batch, so
  the rebalance problem stays.
- **Spring Kafka async listener return types (`CompletableFuture`).** They release the poll
  thread, but Spring Kafka does not support them together with container-managed transactions,
  which is the exactly-once mechanism.
- **Confluent Parallel Consumer.** It solves this problem well and supports key ordering and
  transactions. It is a large dependency with its own configuration model and offset encoding,
  and it would have to be wrapped anyway to add ledger-aware retries, retry topics and the
  starter's configuration. Its transactional mode is marked experimental in its docs.
- **An own loop on plain `kafka-clients`.** More code to own, but small (one class of about 500
  lines plus a scheduler and per-partition bookkeeping), fully testable with `MockConsumer` and
  `MockProducer`, and every decision is visible.

## Decision

A new module, `enricher-kafka`, with no Spring dependency:

- **One poll thread per consumer** owns the consumer, a producer and all bookkeeping. It polls
  every iteration (10 ms while work is running), so the consumer never hits
  `max.poll.interval.ms` because of a slow call.
- **Lanes.** Each record gets a lane: its key within its partition by default (`ordering: key`),
  the partition (`partition`), or itself (`unordered`). A lane runs one record at a time in offset
  order; different lanes run in parallel on virtual threads, up to `max-in-flight` per consumer.
- **Commit the finished prefix.** Output is written in offset order and an offset is committed only
  when every earlier record of its partition has finished. Every `commit-interval` (100 ms) the
  loop writes the outputs of all finished prefixes and their offsets in one Kafka transaction
  (EOS v2 with `sendOffsetsToTransaction` and the consumer's group metadata), or, without
  transactions, waits for the broker's acknowledgements and then commits offsets.
- **Backpressure by pausing.** A partition with `max-pending-per-partition` uncommitted records is
  paused and resumed at half, which bounds memory without stopping `poll()`.
- **Failure handling.**
  - A failed commit aborts the transaction (or replaces a fenced producer) and seeks the
    affected partitions back to their last committed offset.
  - On revocation the loop commits what has finished and forgets the revoked partitions. Work
    still running for them is not interrupted: it finishes, stores its result in the ledger where
    the new owner finds it, and its output is discarded.
- **Retry topics.** Retryable failures that survive the provider's in-place attempts go to
  `<input>.enricher-retry-N` with a `enricher-retry-not-before` header. The same loop subscribes to
  the retry topics and holds a retry record (and only its lane) until it is due, without pausing
  anything else.

The ledger from ADR 0002 is what makes the simple failure handling affordable: rewinds,
rebalances and abandoned in-flight work all lead to records being processed again, and a
processed-again record is a ledger hit, not a paid call.

## Consequences

- A slow call costs latency for its own key and holds back the commit point of its partition, but
  does not stop other keys or cause rebalances. `ConcurrencyAndRetryIT` checks this with a 5 s call
  and a 3 s `max.poll.interval.ms`.
- Fewer, larger transactions: one per commit interval instead of one per record.
- Output for a partition is held until its prefix is finished, so one stuck record delays the
  *visibility* of later results on that partition by up to the call timeout times attempts. A
  per-partition offset map in commit metadata (as Parallel Consumer does) would remove this; it is
  on the roadmap.
- Records that move to a retry topic no longer block later records of the same key, so per-key
  order is guaranteed only for records that succeed on their first stage. Pipelines that need
  strict order can configure no retry delays, which sends exhausted records to the dead-letter
  topic instead.
- The starter no longer uses Spring Kafka's listener containers, error handlers or
  `KafkaTransactionManager`; it still takes all client settings from `spring.kafka.*`.
- The poll thread does Kafka I/O for commits; a commit that hangs (broker unavailable) stalls the
  loop, bounded by the client's own timeouts.
