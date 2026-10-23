package io.github.xeeshan16.enricher.spring;

import io.github.xeeshan16.enricher.engine.LedgeredEnricher;
import io.github.xeeshan16.enricher.kafka.ConsumerLoop;
import io.github.xeeshan16.enricher.kafka.EngineStats;
import io.github.xeeshan16.enricher.kafka.EnrichmentProcessor;
import io.github.xeeshan16.enricher.kafka.KafkaClientFactory;
import io.github.xeeshan16.enricher.kafka.OutputRecordFactory;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Runs each pipeline's {@link ConsumerLoop}s ({@code concurrency} of them, one platform thread
 * each) and owns their lifecycle.
 *
 * <p>With a transaction id prefix every loop gets its own transactional producer, id {@code
 * <prefix><pipeline>-<n>}, and writes outputs and offsets in one Kafka transaction per commit
 * interval (exactly-once). The prefix must be unique per instance and stable across its restarts.
 * Without one, loops publish and then commit offsets (at-least-once).
 */
public final class PipelineRunner implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(PipelineRunner.class);

  private final PipelineRegistry registry;
  private final LedgeredEnricher enricher;
  private final KafkaClientFactory clients;
  private final OutputRecordFactory outputs;
  private final String transactionIdPrefix;
  private final Duration busyRetryInterval;
  private final Duration busyBudget;
  private final boolean autoStartup;
  private final String instanceTag = UUID.randomUUID().toString().substring(0, 8);
  private final Map<ConsumerLoop, String> loops = new LinkedHashMap<>();
  private final List<Thread> threads = new ArrayList<>();
  private volatile boolean running;

  /**
   * @param transactionIdPrefix prefix of the loops' transactional ids for exactly-once delivery, or
   *     {@code null} for at-least-once
   * @param busyRetryInterval pause between ledger claims while a key is leased by another worker
   * @param busyBudget how long a record waits for a busy key before it moves to a retry topic
   */
  public PipelineRunner(
      PipelineRegistry registry,
      LedgeredEnricher enricher,
      KafkaClientFactory clients,
      OutputRecordFactory outputs,
      String transactionIdPrefix,
      Duration busyRetryInterval,
      Duration busyBudget,
      boolean autoStartup) {
    this.registry = registry;
    this.enricher = enricher;
    this.clients = clients;
    this.outputs = outputs;
    this.transactionIdPrefix = transactionIdPrefix;
    this.busyRetryInterval = busyRetryInterval;
    this.busyBudget = busyBudget;
    this.autoStartup = autoStartup;
  }

  /** Whether records are processed in Kafka transactions (exactly-once). */
  public boolean transactional() {
    return transactionIdPrefix != null;
  }

  @Override
  public synchronized void start() {
    if (running) {
      return;
    }
    if (registry.pipelines().isEmpty()) {
      log.warn("no pipelines configured under enricher.pipelines; nothing to consume");
    }
    for (PipelineDefinition pipeline : registry.pipelines()) {
      PipelineRegistry.Runtime runtime = registry.runtime(pipeline.name());
      EnrichmentProcessor processor =
          new EnrichmentProcessor(
              pipeline, enricher, runtime.retryTopics(), outputs, busyRetryInterval, busyBudget);
      for (int i = 0; i < runtime.concurrency(); i++) {
        String name = "enricher-" + pipeline.name() + "-" + i;
        String transactionalId =
            transactionIdPrefix == null ? null : transactionIdPrefix + pipeline.name() + "-" + i;
        ConsumerLoop loop =
            new ConsumerLoop(
                name + "-" + instanceTag,
                runtime.retryTopics().subscriptions(),
                runtime.groupId(),
                transactionalId,
                clients,
                processor,
                runtime.engine(),
                Clock.systemUTC());
        loops.put(loop, pipeline.name());
        threads.add(Thread.ofPlatform().name(name).start(loop));
      }
      log.info(
          "pipeline={} consuming {} -> {} (dlq {}, retry {}, group {}, consumers {}, ordering {},"
              + " max in flight {}, promptVersion {}, model {}, delivery {})",
          pipeline.name(),
          pipeline.inputTopic(),
          pipeline.outputTopic(),
          pipeline.deadLetterTopic(),
          runtime.retryTopics().delays(),
          runtime.groupId(),
          runtime.concurrency(),
          runtime.engine().ordering(),
          runtime.engine().maxInFlight(),
          pipeline.promptVersion(),
          pipeline.model(),
          transactional() ? "exactly-once" : "at-least-once");
    }
    running = true;
  }

  @Override
  public synchronized void stop() {
    for (ConsumerLoop loop : loops.keySet()) {
      loop.stop();
    }
    for (ConsumerLoop loop : loops.keySet()) {
      Duration wait = Duration.ofSeconds(15);
      try {
        if (!loop.awaitTermination(wait)) {
          log.warn("loop {} did not stop within {}", loop.name(), wait);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    loops.clear();
    threads.clear();
    running = false;
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public boolean isAutoStartup() {
    return autoStartup;
  }

  /** The running loops. */
  public synchronized List<ConsumerLoop> loops() {
    return List.copyOf(loops.keySet());
  }

  /** Partitions assigned to any loop of this instance. */
  public synchronized Set<TopicPartition> assignedPartitions() {
    Set<TopicPartition> all = new HashSet<>();
    for (ConsumerLoop loop : loops.keySet()) {
      all.addAll(loop.assignedPartitions());
    }
    return all;
  }

  /** Counters of one pipeline, summed over its loops. */
  public synchronized EngineStats.Snapshot stats(String pipeline) {
    EngineStats.Snapshot total = EngineStats.Snapshot.EMPTY;
    for (Map.Entry<ConsumerLoop, String> entry : loops.entrySet()) {
      if (entry.getValue().equals(pipeline)) {
        total = total.plus(entry.getKey().stats().snapshot());
      }
    }
    return total;
  }
}
