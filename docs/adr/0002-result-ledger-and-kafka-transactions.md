# ADR 0002: A PostgreSQL result ledger next to Kafka transactions

- Status: accepted
- Date: 2026-10-22

## Context

An enrichment pipeline has two different duplicate problems:

1. **Duplicate output.** A crash or rebalance between publishing an envelope and committing the
   input offset makes the record come back, and the envelope is published again.
2. **Duplicate paid calls.** Every time the record comes back, the naive pipeline calls the model
   again. With LLMs this is the expensive part: money, rate-limit budget and latency. Replays of
   a topic (new consumer group, reprocessing after a bug fix) multiply it.

Kafka transactions (read-process-write with `sendOffsetsToTransaction`, EOS v2) solve problem 1:
the envelope and the offset commit or abort together, and `read_committed` consumers never see the
aborted attempt. They do not solve problem 2. An aborted transaction aborts the output, but the
model call that produced it has already been paid for, and the retry pays again. A zombie worker
that was expelled from the group keeps calling the provider for the record it was stuck on, while
the new owner of the partition calls it too.

Options considered for problem 2:

- **Kafka only (a compacted "results" topic keyed by input hash).** Writes can be part of the same
  transaction, but then an aborted transaction also discards the stored result, which is exactly
  the case we want to make free. Reads need a local materialised view per instance that lags the
  topic, so two instances can still race on the same key. There is no lease, so concurrent callers
  cannot be told to wait.
- **Redis with `SET NX PX` leases.** Fast, but most payments teams treat Redis as a cache, not as a
  record of money spent. Durability depends on persistence settings, and it is one more stateful
  system to run.
- **PostgreSQL table with leases.** Teams in this stack already run PostgreSQL, back it up, and can
  query it. One row per key gives a lease (`IN_FLIGHT` with an expiry), a stored result
  (`COMPLETED`) and counters for hits, all with plain SQL.

## Decision

- Delivery to the output topic uses Kafka transactions whenever Spring Boot has a Kafka
  transaction manager (`spring.kafka.producer.transaction-id-prefix` set, unique per instance).
  Failures are handled by `DefaultAfterRollbackProcessor`, which also publishes dead-letter records
  and commits their offsets in a transaction. `enricher.delivery` can demand exactly-once or force
  at-least-once; both fail at startup if the Kafka setup contradicts them.
- A `ResultLedger` keyed by `(pipeline, prompt version, requested model, canonical input hash)`
  sits in front of the provider:
  - `claim` returns a stored result (hit), a lease, or "busy" when another worker holds a live
    lease. A busy key is re-checked in place for a short `busy-wait`, then retried by the
    container on a back-off that spans about one lease lifetime.
  - `complete` stores the validated, canonical answer. It is idempotent and the first completed
    result wins: a worker whose lease lapsed and who completes late gets the winner back and
    publishes that, so every envelope for one key carries the same answer.
  - `release` drops a lease after an unusable answer or a provider error, so the next attempt can
    claim at once instead of waiting for expiry.
- The PostgreSQL implementation runs every operation as one auto-committed statement, outside the
  Kafka transaction on purpose: a result stored just before a crash survives the aborted Kafka
  transaction, so the retry is a ledger hit. Lease expiry uses the database clock. The table is
  created by the module's own Flyway migrations with a separate history table.
- Enrichments are stored and published in canonical JSON, so an envelope rebuilt from the ledger is
  byte-identical to the original. Whether a delivery paid for a call is carried in the
  `enricher-result-source` header, not in the envelope.

## Consequences

- Output is exactly-once for `read_committed` consumers, and paid calls are once per key except in
  one window: a worker that dies (or whose lease lapses) after the provider answered and before
  `complete` stored the answer. Keeping `lease-ttl` above the provider timeout plus retries keeps
  that window to genuine crashes.
- Each record costs one to three small SQL statements before the model call. That is negligible
  next to an LLM call, but the ledger database must be sized for the pipeline's record rate. Day 3
  adds batching and per-key parallelism on top.
- Stored results are data: they contain model output derived from events. Retention is the
  operator's choice (`purgeCompletedBefore`, a scheduled purge is on the roadmap), and PII masking
  (day 5) applies before the prompt, so masked inputs produce masked results.
- Changing the prompt or schema without bumping `prompt-version` would serve stale answers. The
  version is part of the key for that reason, and the README says so.
- Teams that cannot store results can set `enricher.ledger.store=none` and keep transactions;
  single-instance setups can use the in-memory ledger.
