package io.github.xeeshan16.enricher.kafka;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One Kafka consumer whose records are processed in parallel while the poll loop keeps running.
 *
 * <p>The poll thread is the only thread that touches the consumer, the producer and the
 * bookkeeping. Each iteration it:
 *
 * <ol>
 *   <li>polls (briefly when work is running) and buffers new records per partition;
 *   <li>hands records whose lane is free to worker virtual threads, at most {@link
 *       EngineSettings#maxInFlight()} at a time. Lanes follow {@link EngineSettings#ordering()}: by
 *       default one lane per key, so a slow model call delays only later records of the same key;
 *   <li>collects finished records; every {@link EngineSettings#commitInterval()} it writes the
 *       outputs of each partition's finished prefix, in offset order, and commits the offsets. With
 *       a transactional producer both happen in one Kafka transaction (exactly-once); otherwise
 *       outputs are acknowledged before offsets are committed (at-least-once);
 *   <li>pauses partitions whose uncommitted backlog reached {@link
 *       EngineSettings#maxPendingPerPartition()} and resumes them at half, so memory stays bounded
 *       and the consumer never stops calling {@code poll()}: a slow provider can no longer exceed
 *       {@code max.poll.interval.ms} and trigger a rebalance.
 * </ol>
 *
 * <p>A failed commit aborts the transaction and rewinds the affected partitions to their last
 * committed offset; records are processed again, cheaply when a result ledger answers them. On a
 * rebalance the loop commits what has finished, then forgets revoked partitions. Work still running
 * for them is not interrupted: it completes (storing its result in the ledger, where the new owner
 * finds it) and its output is discarded here.
 */
public final class ConsumerLoop implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(ConsumerLoop.class);
  private static final Duration BUSY_POLL = Duration.ofMillis(10);
  private static final long FAILURE_RETRY_MILLIS = 1_000;
  private static final String NULL_KEY_LANE = "\u0000null-key";
  private static final String PARTITION_LANE = "\u0000partition";

  private record Lane(PartitionState state, Object id) {}

  private record Work(PartitionState state, Lane lane, ConsumerRecord<String, String> record) {}

  private record Completion(Work work, List<ProducerRecord<String, String>> outputs) {}

  private final String name;
  private final Collection<String> topics;
  private final String groupId;
  private final String transactionalId;
  private final KafkaClientFactory clients;
  private final RecordProcessor processor;
  private final EngineSettings settings;
  private final Clock clock;
  private final EngineStats stats = new EngineStats();
  private final Map<TopicPartition, PartitionState> partitions = new HashMap<>();
  private final LaneScheduler<Work> scheduler = new LaneScheduler<>();
  private final LinkedBlockingQueue<Completion> completions = new LinkedBlockingQueue<>();
  private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
  private final CountDownLatch finished = new CountDownLatch(1);

  private Consumer<String, String> consumer;
  private Producer<String, String> producer;
  private int inFlight;
  private long lastCommitMillis;
  private volatile boolean running = true;
  private volatile Set<TopicPartition> assigned = Set.of();
  private volatile Throwable failure;

  /**
   * @param name loop name, used as client id and in logs; unique within the process
   * @param topics topics to subscribe to
   * @param groupId consumer group
   * @param transactionalId transactional id for exactly-once delivery, or {@code null} for
   *     at-least-once
   */
  public ConsumerLoop(
      String name,
      Collection<String> topics,
      String groupId,
      String transactionalId,
      KafkaClientFactory clients,
      RecordProcessor processor,
      EngineSettings settings,
      Clock clock) {
    this.name = Objects.requireNonNull(name, "name");
    this.topics = List.copyOf(topics);
    this.groupId = Objects.requireNonNull(groupId, "groupId");
    this.transactionalId = transactionalId;
    this.clients = Objects.requireNonNull(clients, "clients");
    this.processor = Objects.requireNonNull(processor, "processor");
    this.settings = Objects.requireNonNull(settings, "settings");
    this.clock = Objects.requireNonNull(clock, "clock");
    if (this.topics.isEmpty()) {
      throw new IllegalArgumentException("no topics to consume");
    }
  }

  public String name() {
    return name;
  }

  public boolean transactional() {
    return transactionalId != null;
  }

  public EngineStats stats() {
    return stats;
  }

  /** Partitions assigned to this loop's consumer right now. */
  public Set<TopicPartition> assignedPartitions() {
    return assigned;
  }

  /** The exception that ended the loop, if it ended abnormally. */
  public Throwable failure() {
    return failure;
  }

  /** Asks the loop to finish: it stops polling, waits for running records, commits and closes. */
  public void stop() {
    running = false;
  }

  /** Waits for the loop to finish after {@link #stop()}. */
  public boolean awaitTermination(Duration timeout) throws InterruptedException {
    return finished.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void run() {
    try {
      consumer = clients.createConsumer(groupId, name);
      producer = newProducer();
      consumer.subscribe(topics, new Rebalance());
      log.info(
          "loop {} consuming {} (group {}, {}, ordering {}, max in flight {})",
          name,
          topics,
          groupId,
          transactional() ? "exactly-once" : "at-least-once",
          settings.ordering(),
          settings.maxInFlight());
      lastCommitMillis = clock.millis();
      while (running) {
        iterate();
      }
      drainForShutdown();
    } catch (RuntimeException | Error e) {
      failure = e;
      log.error("loop {} stopped after an unexpected error", name, e);
    } finally {
      workers.shutdownNow();
      closeQuietly();
      assigned = Set.of();
      finished.countDown();
    }
  }

  private void iterate() {
    long now = clock.millis();
    Duration timeout = settings.pollTimeout();
    if (inFlight > 0 || !completions.isEmpty()) {
      timeout = BUSY_POLL;
    } else {
      long untilDue = scheduler.nextDueMillis() - now;
      if (untilDue < timeout.toMillis()) {
        timeout = Duration.ofMillis(Math.max(1, untilDue));
      }
    }
    ConsumerRecords<String, String> records = consumer.poll(timeout);
    now = clock.millis();
    for (ConsumerRecord<String, String> record : records) {
      accept(record, now);
    }
    drainCompletions();
    dispatch();
    if (clock.millis() - lastCommitMillis >= settings.commitInterval().toMillis()) {
      commit();
    }
    applyBackpressure();
  }

  private void accept(ConsumerRecord<String, String> record, long now) {
    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
    PartitionState state = partitions.computeIfAbsent(tp, PartitionState::new);
    if (!state.add(record)) {
      return;
    }
    stats.polled.increment();
    Lane lane = new Lane(state, laneId(record));
    scheduler.add(lane, new Work(state, lane, record), processor.notBeforeMillis(record), now);
  }

  private Object laneId(ConsumerRecord<String, String> record) {
    return switch (settings.ordering()) {
      case KEY -> record.key() == null ? NULL_KEY_LANE : record.key();
      case PARTITION -> PARTITION_LANE;
      case UNORDERED -> record.offset();
    };
  }

  private void dispatch() {
    while (inFlight < settings.maxInFlight()) {
      Work work = scheduler.next(clock.millis());
      if (work == null) {
        return;
      }
      inFlight++;
      stats.started();
      workers.execute(() -> completions.add(new Completion(work, runProcessor(work.record()))));
    }
  }

  /** Runs on a worker thread. Returns {@code null} when even the failure handler failed. */
  private List<ProducerRecord<String, String>> runProcessor(ConsumerRecord<String, String> record) {
    try {
      return Objects.requireNonNull(processor.process(record), "process returned null");
    } catch (RuntimeException | Error e) {
      try {
        return Objects.requireNonNull(processor.onFailure(record, e), "onFailure returned null");
      } catch (RuntimeException | Error e2) {
        e2.addSuppressed(e);
        log.error(
            "loop {}: cannot handle {}-{}@{}; retrying in {} ms",
            name,
            record.topic(),
            record.partition(),
            record.offset(),
            FAILURE_RETRY_MILLIS,
            e2);
        return null;
      }
    }
  }

  private void drainCompletions() {
    Completion completion;
    while ((completion = completions.poll()) != null) {
      inFlight--;
      stats.finished();
      Work work = completion.work();
      if (partitions.get(work.state().partition()) != work.state()) {
        // Revoked, lost or rewound while running.
        scheduler.done(work.lane(), clock.millis());
        stats.discarded.increment();
        continue;
      }
      if (completion.outputs() == null) {
        scheduler.addFirst(work.lane(), work, clock.millis() + FAILURE_RETRY_MILLIS);
        scheduler.done(work.lane(), clock.millis());
        continue;
      }
      work.state().complete(work.record().offset(), completion.outputs());
      scheduler.done(work.lane(), clock.millis());
      stats.processed.increment();
    }
  }

  /** Writes the outputs of every finished prefix and commits their offsets. */
  private void commit() {
    lastCommitMillis = clock.millis();
    List<ProducerRecord<String, String>> outputs = new ArrayList<>();
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    Map<TopicPartition, Long> rewindTo = new HashMap<>();
    long inputs = 0;
    for (PartitionState state : partitions.values()) {
      long first = state.firstOffset();
      int before = state.pending();
      long next = state.drainDone(outputs);
      if (next >= 0) {
        offsets.put(state.partition(), new OffsetAndMetadata(next));
        rewindTo.put(state.partition(), first);
        inputs += before - state.pending();
      }
    }
    if (offsets.isEmpty()) {
      return;
    }
    try {
      if (transactional()) {
        producer.beginTransaction();
        for (ProducerRecord<String, String> output : outputs) {
          Future<RecordMetadata> unused = producer.send(output);
        }
        producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
        producer.commitTransaction();
      } else {
        List<Future<RecordMetadata>> acks = new ArrayList<>(outputs.size());
        for (ProducerRecord<String, String> output : outputs) {
          acks.add(producer.send(output));
        }
        producer.flush();
        for (Future<RecordMetadata> ack : acks) {
          ack.get(settings.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
        }
        consumer.commitSync(offsets);
      }
      stats.commits.increment();
      stats.committedInputs.add(inputs);
      stats.writtenOutputs.add(outputs.size());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      commitFailed(new KafkaException("interrupted while waiting for acks", e), rewindTo);
    } catch (ExecutionException | TimeoutException e) {
      commitFailed(new KafkaException("output was not acknowledged", e), rewindTo);
    } catch (KafkaException | IllegalStateException e) {
      commitFailed(e, rewindTo);
    }
  }

  private void commitFailed(RuntimeException e, Map<TopicPartition, Long> rewindTo) {
    stats.commitFailures.increment();
    log.warn(
        "loop {}: commit of {} partition(s) failed, rewinding them: {}",
        name,
        rewindTo.size(),
        e.toString());
    if (transactional()) {
      if (isFatal(e)) {
        replaceProducer();
      } else {
        try {
          producer.abortTransaction();
        } catch (KafkaException | IllegalStateException abortFailure) {
          log.warn(
              "loop {}: abort failed, replacing the producer: {}", name, abortFailure.toString());
          replaceProducer();
        }
      }
    }
    Set<TopicPartition> owned = consumer.assignment();
    for (Map.Entry<TopicPartition, Long> entry : rewindTo.entrySet()) {
      TopicPartition tp = entry.getKey();
      forget(tp);
      if (owned.contains(tp)) {
        consumer.seek(tp, entry.getValue());
        stats.rewinds.increment();
      }
    }
  }

  private static boolean isFatal(Throwable e) {
    return e instanceof ProducerFencedException
        || e instanceof InvalidProducerEpochException
        || e instanceof OutOfOrderSequenceException
        || e instanceof UnsupportedVersionException
        || e instanceof AuthorizationException;
  }

  private Producer<String, String> newProducer() {
    Producer<String, String> created = clients.createProducer(name, transactionalId);
    if (transactionalId != null) {
      created.initTransactions();
    }
    return created;
  }

  private void replaceProducer() {
    try {
      producer.close(Duration.ofSeconds(5));
    } catch (RuntimeException e) {
      log.debug("loop {}: closing the old producer failed: {}", name, e.toString());
    }
    producer = newProducer();
  }

  /** Drops the buffered state and queued work of a partition. Running work becomes stale. */
  private void forget(TopicPartition tp) {
    PartitionState state = partitions.remove(tp);
    if (state != null) {
      scheduler.removeLanes(lane -> ((Lane) lane).state() == state);
    }
  }

  private void applyBackpressure() {
    Set<TopicPartition> paused = consumer.paused();
    List<TopicPartition> toPause = new ArrayList<>();
    List<TopicPartition> toResume = new ArrayList<>();
    for (TopicPartition tp : consumer.assignment()) {
      PartitionState state = partitions.get(tp);
      int pending = state == null ? 0 : state.pending();
      boolean isPaused = paused.contains(tp);
      if (!isPaused && pending >= settings.maxPendingPerPartition()) {
        toPause.add(tp);
      } else if (isPaused && pending <= settings.maxPendingPerPartition() / 2) {
        toResume.add(tp);
      }
    }
    if (!toPause.isEmpty()) {
      consumer.pause(toPause);
      stats.pauses.add(toPause.size());
      log.debug("loop {}: paused {} (backlog full)", name, toPause);
    }
    if (!toResume.isEmpty()) {
      consumer.resume(toResume);
      stats.resumes.add(toResume.size());
      log.debug("loop {}: resumed {}", name, toResume);
    }
    stats.pausedPartitions.set(paused.size() + toPause.size() - toResume.size());
  }

  private void drainForShutdown() {
    long deadline = clock.millis() + settings.shutdownTimeout().toMillis();
    while (inFlight > 0 && clock.millis() < deadline) {
      try {
        Completion next = completions.poll(50, TimeUnit.MILLISECONDS);
        if (next != null) {
          completions.add(next);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      drainCompletions();
    }
    drainCompletions();
    commit();
    if (inFlight > 0) {
      log.info(
          "loop {}: stopping with {} record(s) still running; they are redelivered to the next"
              + " owner",
          name,
          inFlight);
    }
  }

  private void closeQuietly() {
    if (consumer != null) {
      try {
        consumer.close(Duration.ofSeconds(5));
      } catch (RuntimeException e) {
        log.debug("loop {}: consumer close failed: {}", name, e.toString());
      }
    }
    if (producer != null) {
      try {
        producer.close(Duration.ofSeconds(5));
      } catch (RuntimeException e) {
        log.debug("loop {}: producer close failed: {}", name, e.toString());
      }
    }
  }

  /** Runs inside {@code poll()} on the poll thread. */
  private final class Rebalance implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> revoked) {
      if (revoked.isEmpty()) {
        return;
      }
      drainCompletions();
      commit();
      for (TopicPartition tp : revoked) {
        forget(tp);
      }
      stats.partitionsRevoked.add(revoked.size());
      updateAssigned(revoked, false);
      log.info("loop {}: revoked {}", name, revoked);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> lost) {
      for (TopicPartition tp : lost) {
        forget(tp);
      }
      stats.partitionsLost.add(lost.size());
      updateAssigned(lost, false);
      log.warn("loop {}: lost {} without committing", name, lost);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> added) {
      stats.partitionsAssigned.add(added.size());
      updateAssigned(added, true);
      if (!added.isEmpty()) {
        log.info("loop {}: assigned {}", name, added);
      }
    }

    private void updateAssigned(Collection<TopicPartition> changed, boolean add) {
      Set<TopicPartition> next = new HashSet<>(assigned);
      if (add) {
        next.addAll(changed);
      } else {
        next.removeAll(changed);
      }
      assigned = Set.copyOf(next);
    }
  }
}
