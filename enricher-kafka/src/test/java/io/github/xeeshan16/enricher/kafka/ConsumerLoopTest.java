package io.github.xeeshan16.enricher.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The consumer loop against Kafka's {@link MockConsumer} and {@link MockProducer}: ordering,
 * parallelism, backpressure, transactional commits, rewinds and rebalances, without a broker.
 */
class ConsumerLoopTest {

  private static final TopicPartition P0 = new TopicPartition("in", 0);
  private static final TopicPartition P1 = new TopicPartition("in", 1);
  private static final Duration WAIT = Duration.ofSeconds(20);

  /** A transactional mock producer whose next commits can be made to fail. */
  static final class FlakyProducer extends MockProducer<String, String> {
    final AtomicInteger failCommits = new AtomicInteger();

    FlakyProducer() {
      super(true, new StringSerializer(), new StringSerializer());
    }

    @Override
    public synchronized void commitTransaction() throws ProducerFencedException {
      if (failCommits.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
        throw new KafkaException("injected commit failure");
      }
      super.commitTransaction();
    }
  }

  /** Echoes each record to "out"; a hook runs first and may block, sleep or throw. */
  static class Echo implements RecordProcessor {
    final Consumer<ConsumerRecord<String, String>> hook;
    final AtomicInteger calls = new AtomicInteger();
    final List<String> started = new CopyOnWriteArrayList<>();

    Echo(Consumer<ConsumerRecord<String, String>> hook) {
      this.hook = hook;
    }

    @Override
    public List<ProducerRecord<String, String>> process(ConsumerRecord<String, String> record) {
      calls.incrementAndGet();
      started.add(record.value());
      hook.accept(record);
      return List.of(new ProducerRecord<>("out", record.key(), record.value()));
    }

    @Override
    public List<ProducerRecord<String, String>> onFailure(
        ConsumerRecord<String, String> record, Throwable error) {
      return List.of(new ProducerRecord<>("dlq", record.key(), record.value()));
    }
  }

  private final MockConsumer<String, String> consumer =
      new MockConsumer<>(OffsetResetStrategy.EARLIEST);
  private final FlakyProducer producer = new FlakyProducer();
  private ConsumerLoop loop;
  private Thread thread;

  @AfterEach
  void stopLoop() throws InterruptedException {
    if (loop != null) {
      loop.stop();
      assertThat(loop.awaitTermination(WAIT)).isTrue();
    }
  }

  private void start(RecordProcessor processor, EngineSettings settings, String transactionalId) {
    KafkaClientFactory clients =
        new KafkaClientFactory() {
          @Override
          public org.apache.kafka.clients.consumer.Consumer<String, String> createConsumer(
              String groupId, String clientId) {
            return consumer;
          }

          @Override
          public Producer<String, String> createProducer(String clientId, String txId) {
            return producer;
          }
        };
    consumer.updateBeginningOffsets(Map.of(P0, 0L, P1, 0L));
    consumer.schedulePollTask(() -> consumer.rebalance(List.of(P0, P1)));
    loop =
        new ConsumerLoop(
            "test",
            List.of("in"),
            "group",
            transactionalId,
            clients,
            processor,
            settings,
            Clock.systemUTC());
    thread = new Thread(loop, "consumer-loop-test");
    thread.start();
    await("partitions assigned", () -> assignment().size() == 2);
  }

  private Set<TopicPartition> assignment() {
    synchronized (consumer) {
      return Set.copyOf(consumer.assignment());
    }
  }

  private void add(TopicPartition tp, long offset, String key, String value) {
    consumer.addRecord(new ConsumerRecord<>(tp.topic(), tp.partition(), offset, key, value));
  }

  private Map<TopicPartition, Long> committed() {
    Map<TopicPartition, Long> result = new HashMap<>();
    for (Map<String, Map<TopicPartition, OffsetAndMetadata>> commit :
        producer.consumerGroupOffsetsHistory()) {
      for (Map<TopicPartition, OffsetAndMetadata> offsets : commit.values()) {
        offsets.forEach((tp, o) -> result.merge(tp, o.offset(), Math::max));
      }
    }
    return result;
  }

  private List<String> outputs(String topic) {
    List<String> values = new ArrayList<>();
    for (ProducerRecord<String, String> r : producer.history()) {
      if (r.topic().equals(topic)) {
        values.add(r.value());
      }
    }
    return values;
  }

  private static void await(String what, BooleanSupplier condition) {
    long deadline = System.nanoTime() + WAIT.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("timed out waiting for: " + what);
      }
      sleep(5);
    }
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(WAIT.toMillis(), TimeUnit.MILLISECONDS)) {
        throw new AssertionError("latch not released");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Test
  void keyLanesRunInParallelButNeverTwoRecordsOfOneKeyAtOnce() {
    Map<String, AtomicInteger> running = new ConcurrentHashMap<>();
    AtomicInteger overlaps = new AtomicInteger();
    Echo echo =
        new Echo(
            r -> {
              if (running.computeIfAbsent(r.key(), k -> new AtomicInteger()).incrementAndGet()
                  > 1) {
                overlaps.incrementAndGet();
              }
              sleep(ThreadLocalRandom.current().nextLong(1, 8));
              running.get(r.key()).decrementAndGet();
            });
    start(echo, EngineSettings.defaults().withMaxInFlight(8), "tx-test");

    for (int i = 0; i < 40; i++) {
      add(P0, i, "k" + (i % 4), "p0-" + i);
      add(P1, i, "k" + (i % 4), "p1-" + i);
    }

    await("all committed", () -> committed().equals(Map.of(P0, 40L, P1, 40L)));
    assertThat(overlaps).as("two records of one key ran at once").hasValue(0);
    assertThat(loop.stats().snapshot().maxInFlightSeen()).isBetween(2, 8);
    // Output follows input offset order within each partition.
    List<String> out = outputs("out");
    assertThat(out).hasSize(80);
    assertThat(out.stream().filter(v -> v.startsWith("p0-")).toList())
        .containsExactlyElementsOf(range("p0-", 40));
    assertThat(out.stream().filter(v -> v.startsWith("p1-")).toList())
        .containsExactlyElementsOf(range("p1-", 40));
    // Within a key, records also started in offset order.
    List<String> k1Started =
        echo.started.stream()
            .filter(v -> v.startsWith("p0-") && Integer.parseInt(v.substring(3)) % 4 == 1)
            .toList();
    assertThat(k1Started)
        .isSortedAccordingTo(
            (a, b) ->
                Integer.compare(
                    Integer.parseInt(a.substring(3)), Integer.parseInt(b.substring(3))));
    assertThat(loop.stats().snapshot().commits()).isPositive();
    assertThat(loop.stats().snapshot().committedInputs()).isEqualTo(80);
  }

  @Test
  void aSlowRecordDelaysOnlyItsOwnKeyAndHoldsTheCommitPrefix() {
    CountDownLatch release = new CountDownLatch(1);
    Echo echo =
        new Echo(
            r -> {
              if (r.key().equals("slow")) {
                await(release);
              }
            });
    start(echo, EngineSettings.defaults(), "tx-test");

    add(P0, 0, "slow", "v0");
    for (int i = 1; i <= 20; i++) {
      add(P0, i, "k" + (i % 4), "v" + i);
    }

    await("other keys processed", () -> loop.stats().snapshot().processed() == 20);
    sleep(300);
    assertThat(committed()).as("offset 0 is still running").doesNotContainKey(P0);
    assertThat(outputs("out")).isEmpty();

    release.countDown();
    await("all committed", () -> committed().getOrDefault(P0, 0L) == 21L);
    assertThat(outputs("out")).containsExactlyElementsOf(range("v", 21));
  }

  @Test
  void partitionOrderingIsSequential() {
    Echo echo = new Echo(r -> sleep(2));
    start(echo, EngineSettings.defaults().withOrdering(OrderingMode.PARTITION), "tx-test");

    for (int i = 0; i < 15; i++) {
      add(P0, i, "k" + i, "v" + i);
    }

    await("all committed", () -> committed().getOrDefault(P0, 0L) == 15L);
    assertThat(loop.stats().snapshot().maxInFlightSeen()).isOne();
    assertThat(echo.started).containsExactlyElementsOf(range("v", 15));
  }

  @Test
  void unorderedModeRunsRecordsOfOneKeyInParallel() {
    CountDownLatch allStarted = new CountDownLatch(6);
    Echo echo =
        new Echo(
            r -> {
              allStarted.countDown();
              await(allStarted);
            });
    start(echo, EngineSettings.defaults().withOrdering(OrderingMode.UNORDERED), "tx-test");

    for (int i = 0; i < 6; i++) {
      add(P0, i, "same-key", "v" + i);
    }

    await("all committed", () -> committed().getOrDefault(P0, 0L) == 6L);
    assertThat(loop.stats().snapshot().maxInFlightSeen()).isEqualTo(6);
    assertThat(outputs("out")).containsExactlyElementsOf(range("v", 6));
  }

  @Test
  void pausesAPartitionWhoseBacklogIsFullAndResumesItAtHalf() {
    CountDownLatch release = new CountDownLatch(1);
    Echo echo = new Echo(r -> await(release));
    start(echo, EngineSettings.defaults().withMaxPendingPerPartition(4), "tx-test");

    for (int i = 0; i < 6; i++) {
      add(P0, i, "k" + i, "v" + i);
    }
    await(
        "P0 paused",
        () -> {
          synchronized (consumer) {
            return consumer.paused().contains(P0);
          }
        });
    // Records arriving while paused stay in the consumer until the partition is resumed.
    for (int i = 6; i < 9; i++) {
      add(P0, i, "k" + i, "v" + i);
    }
    sleep(200);
    assertThat(loop.stats().snapshot().polled()).isEqualTo(6);
    assertThat(assignment()).contains(P1);

    release.countDown();
    await("all committed", () -> committed().getOrDefault(P0, 0L) == 9L);
    assertThat(loop.stats().snapshot().pauses()).isPositive();
    assertThat(loop.stats().snapshot().resumes()).isPositive();
    synchronized (consumer) {
      assertThat(consumer.paused()).isEmpty();
    }
    assertThat(outputs("out")).containsExactlyElementsOf(range("v", 9));
  }

  @Test
  void failedCommitAbortsTheTransactionRewindsAndReprocesses() {
    producer.failCommits.set(1);
    Echo echo = new Echo(r -> {});
    start(echo, EngineSettings.defaults(), "tx-test");

    for (int i = 0; i < 5; i++) {
      add(P0, i, "k" + i, "v" + i);
    }
    await("rewound", () -> loop.stats().snapshot().rewinds() >= 1);
    // The broker would redeliver from the rewound position; the mock needs the records again.
    for (int i = 0; i < 5; i++) {
      add(P0, i, "k" + i, "v" + i);
    }

    await("all committed", () -> committed().getOrDefault(P0, 0L) == 5L);
    assertThat(producer.transactionAborted()).isTrue();
    assertThat(outputs("out"))
        .as("aborted output is not visible")
        .containsExactlyElementsOf(range("v", 5));
    assertThat(loop.stats().snapshot().commitFailures()).isOne();
    assertThat(echo.calls.get()).isGreaterThan(5);
  }

  @Test
  void aRecordThatThrowsIsHandedToOnFailureAndThePartitionMovesOn() {
    Echo echo =
        new Echo(
            r -> {
              if (r.value().equals("poison")) {
                throw new IllegalStateException("cannot parse");
              }
            });
    start(echo, EngineSettings.defaults(), "tx-test");

    add(P0, 0, "a", "v0");
    add(P0, 1, "a", "poison");
    add(P0, 2, "a", "v2");

    await("all committed", () -> committed().getOrDefault(P0, 0L) == 3L);
    assertThat(outputs("out")).containsExactly("v0", "v2");
    assertThat(outputs("dlq")).containsExactly("poison");
  }

  @Test
  void revokedPartitionsAreCommittedAndTheirRunningWorkIsDiscarded() {
    CountDownLatch release = new CountDownLatch(1);
    Echo echo =
        new Echo(
            r -> {
              if (r.partition() == 1) {
                await(release);
              }
            });
    start(echo, EngineSettings.defaults(), "tx-test");

    add(P0, 0, "a", "p0-0");
    add(P1, 0, "b", "p1-0");
    await("P0 committed", () -> committed().getOrDefault(P0, 0L) == 1L);

    consumer.schedulePollTask(() -> consumer.rebalance(List.of(P0)));
    await("P1 revoked", () -> loop.assignedPartitions().equals(Set.of(P0)));
    release.countDown();

    await("stale work discarded", () -> loop.stats().snapshot().discarded() == 1);
    sleep(300);
    assertThat(outputs("out")).containsExactly("p0-0");
    assertThat(committed()).doesNotContainKey(P1);
    assertThat(loop.stats().snapshot().partitionsRevoked()).isOne();
  }

  @Test
  void atLeastOnceAcknowledgesOutputBeforeCommittingOffsets() {
    MockProducer<String, String> plain =
        new MockProducer<>(true, new StringSerializer(), new StringSerializer());
    KafkaClientFactory clients =
        new KafkaClientFactory() {
          @Override
          public org.apache.kafka.clients.consumer.Consumer<String, String> createConsumer(
              String groupId, String clientId) {
            return consumer;
          }

          @Override
          public Producer<String, String> createProducer(String clientId, String txId) {
            assertThat(txId).isNull();
            return plain;
          }
        };
    consumer.updateBeginningOffsets(Map.of(P0, 0L, P1, 0L));
    consumer.schedulePollTask(() -> consumer.rebalance(List.of(P0, P1)));
    loop =
        new ConsumerLoop(
            "alo",
            List.of("in"),
            "group",
            null,
            clients,
            new Echo(r -> {}),
            EngineSettings.defaults(),
            Clock.systemUTC());
    thread = new Thread(loop, "consumer-loop-alo");
    thread.start();
    await("assigned", () -> assignment().size() == 2);

    for (int i = 0; i < 7; i++) {
      add(P0, i, "k" + i, "v" + i);
    }

    await(
        "offsets committed",
        () -> {
          synchronized (consumer) {
            OffsetAndMetadata offset = consumer.committed(Set.of(P0)).get(P0);
            return offset != null && offset.offset() == 7L;
          }
        });
    assertThat(plain.history())
        .extracting(ProducerRecord::value)
        .containsExactlyElementsOf(range("v", 7));
    assertThat(loop.transactional()).isFalse();
  }

  @Test
  void delayedRecordsWaitWithoutHoldingUpOtherLanes() {
    long[] startedAt = new long[1];
    long dueAt = System.currentTimeMillis() + 600;
    Echo echo =
        new Echo(
            r -> {
              if (r.key().equals("retry")) {
                startedAt[0] = System.currentTimeMillis();
              }
            }) {
          @Override
          public long notBeforeMillis(ConsumerRecord<String, String> record) {
            return record.key().equals("retry") ? dueAt : 0;
          }
        };
    start(echo, EngineSettings.defaults(), "tx-test");

    add(P1, 0, "retry", "r0");
    add(P0, 0, "a", "a0");
    add(P0, 1, "a", "a1");

    await("P0 committed", () -> committed().getOrDefault(P0, 0L) == 2L);
    assertThat(echo.started).doesNotContain("r0");
    await("P1 committed", () -> committed().getOrDefault(P1, 0L) == 1L);
    assertThat(startedAt[0]).isGreaterThanOrEqualTo(dueAt);
  }

  @Test
  void stopCommitsFinishedWorkAndClosesTheClients() throws InterruptedException {
    Echo echo = new Echo(r -> {});
    start(echo, EngineSettings.defaults().withCommitInterval(Duration.ofHours(1)), "tx-test");

    for (int i = 0; i < 3; i++) {
      add(P0, i, "k", "v" + i);
    }
    await("processed", () -> loop.stats().snapshot().processed() == 3);
    assertThat(committed()).isEmpty();

    loop.stop();
    assertThat(loop.awaitTermination(WAIT)).isTrue();

    assertThat(committed()).containsEntry(P0, 3L);
    assertThat(consumer.closed()).isTrue();
    assertThat(producer.closed()).isTrue();
    assertThat(loop.failure()).isNull();
    loop = null;
  }

  private static List<String> range(String prefix, int count) {
    List<String> values = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      values.add(prefix + i);
    }
    return values;
  }
}
