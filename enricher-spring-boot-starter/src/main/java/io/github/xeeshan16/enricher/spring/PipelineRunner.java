package io.github.xeeshan16.enricher.spring;

import io.github.xeeshan16.enricher.engine.FailureReason;
import io.github.xeeshan16.enricher.engine.LedgeredEnricher;
import io.github.xeeshan16.enricher.ledger.LedgerBusyException;
import io.github.xeeshan16.enricher.llm.LlmProviderException;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.FailedRecordProcessor;
import org.springframework.kafka.transaction.KafkaAwareTransactionManager;
import org.springframework.util.backoff.BackOff;

/**
 * Runs one Spring Kafka listener container per pipeline and owns their lifecycle.
 *
 * <p>With a Kafka transaction manager, every record is processed read-process-write in one Kafka
 * transaction: the enriched envelope (or the dead-letter record) and the input offset commit
 * together, so {@code read_committed} consumers see exactly one result per input. Failures roll the
 * transaction back and the {@link DefaultAfterRollbackProcessor} decides: retry, or publish to the
 * dead-letter topic and commit the offset in a new transaction. Without a transaction manager, each
 * container publishes and then commits offsets per record (at-least-once) and a {@link
 * DefaultErrorHandler} applies the same classification.
 *
 * <p>Classification: unusable answers go to the dead-letter topic at once; provider, ledger and
 * publish errors are retried with the configured back-off; a key leased by another worker is
 * retried on the busy back-off, which spans about one lease lifetime.
 */
public final class PipelineRunner implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(PipelineRunner.class);

  private final PipelineRegistry registry;
  private final LedgeredEnricher enricher;
  private final ConsumerFactory<String, String> consumerFactory;
  private final KafkaOperations<String, String> dlqTemplate;
  private final EnrichedRecordSink sink;
  private final BackOff backOff;
  private final BackOff busyBackOff;
  private final KafkaAwareTransactionManager<?, ?> transactionManager;
  private final boolean autoStartup;
  private final List<ConcurrentMessageListenerContainer<String, String>> containers =
      new ArrayList<>();
  private volatile boolean running;

  /**
   * @param transactionManager Kafka transaction manager for exactly-once delivery, or {@code null}
   *     for at-least-once
   */
  public PipelineRunner(
      PipelineRegistry registry,
      LedgeredEnricher enricher,
      ConsumerFactory<String, String> consumerFactory,
      KafkaOperations<String, String> dlqTemplate,
      EnrichedRecordSink sink,
      BackOff backOff,
      BackOff busyBackOff,
      KafkaAwareTransactionManager<?, ?> transactionManager,
      boolean autoStartup) {
    this.registry = registry;
    this.enricher = enricher;
    this.consumerFactory = consumerFactory;
    this.dlqTemplate = dlqTemplate;
    this.sink = sink;
    this.backOff = backOff;
    this.busyBackOff = busyBackOff;
    this.transactionManager = transactionManager;
    this.autoStartup = autoStartup;
  }

  /** Whether records are processed in Kafka transactions (exactly-once). */
  public boolean transactional() {
    return transactionManager != null;
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
      ConcurrentMessageListenerContainer<String, String> container = container(pipeline);
      container.start();
      containers.add(container);
      log.info(
          "pipeline={} consuming {} -> {} (dlq {}, group {}, promptVersion {}, model {},"
              + " delivery {})",
          pipeline.name(),
          pipeline.inputTopic(),
          pipeline.outputTopic(),
          pipeline.deadLetterTopic(),
          registry.groupId(pipeline.name()),
          pipeline.promptVersion(),
          pipeline.model(),
          transactional() ? "exactly-once" : "at-least-once");
    }
    running = true;
  }

  @SuppressWarnings("deprecation") // setTransactionManager is the API shared by Spring Kafka 3.x
  private ConcurrentMessageListenerContainer<String, String> container(
      PipelineDefinition pipeline) {
    ContainerProperties properties = new ContainerProperties(pipeline.inputTopic());
    properties.setGroupId(registry.groupId(pipeline.name()));
    properties.setAckMode(ContainerProperties.AckMode.RECORD);
    properties.setMessageListener(new EnrichingListener(pipeline, enricher, sink));
    if (transactionManager != null) {
      properties.setTransactionManager(transactionManager);
    }
    ConcurrentMessageListenerContainer<String, String> container =
        new ConcurrentMessageListenerContainer<>(consumerFactory, properties);
    container.setConcurrency(registry.concurrency(pipeline.name()));
    container.setBeanName("enricher-" + pipeline.name());
    DeadLetterPublishingRecoverer recoverer = recoverer(pipeline);
    if (transactionManager != null) {
      DefaultAfterRollbackProcessor<String, String> afterRollback =
          new DefaultAfterRollbackProcessor<>(recoverer, backOff, dlqTemplate, true);
      classify(afterRollback);
      container.setAfterRollbackProcessor(afterRollback);
    } else {
      DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
      classify(handler);
      container.setCommonErrorHandler(handler);
    }
    return container;
  }

  private DeadLetterPublishingRecoverer recoverer(PipelineDefinition pipeline) {
    DeadLetterPublishingRecoverer recoverer =
        new DeadLetterPublishingRecoverer(
            dlqTemplate, (record, ex) -> new TopicPartition(pipeline.deadLetterTopic(), -1));
    recoverer.setHeadersFunction((record, ex) -> failureHeaders(pipeline, ex));
    return recoverer;
  }

  private void classify(FailedRecordProcessor processor) {
    processor.addNotRetryableExceptions(EnrichmentFailedException.class);
    processor.setBackOffFunction((record, ex) -> isLedgerBusy(ex) ? busyBackOff : null);
  }

  static boolean isLedgerBusy(Throwable ex) {
    for (Throwable t = ex; t != null; t = t.getCause()) {
      if (t instanceof LedgerBusyException) {
        return true;
      }
    }
    return false;
  }

  static Headers failureHeaders(PipelineDefinition pipeline, Exception ex) {
    FailureReason reason = FailureReason.PROCESSING_ERROR;
    String detail = String.valueOf(ex.getMessage());
    for (Throwable t = ex; t != null; t = t.getCause()) {
      if (t instanceof EnrichmentFailedException failed) {
        reason = failed.reason();
        detail = String.valueOf(failed.getMessage());
        break;
      }
      if (t instanceof LedgerBusyException busy) {
        reason = FailureReason.LEDGER_BUSY;
        detail = String.valueOf(busy.getMessage());
        break;
      }
      if (t instanceof LlmProviderException provider) {
        reason = FailureReason.PROVIDER_ERROR;
        detail = String.valueOf(provider.getMessage());
        break;
      }
    }
    RecordHeaders headers = new RecordHeaders();
    headers.add(EnricherHeaders.PIPELINE, pipeline.name().getBytes(StandardCharsets.UTF_8));
    headers.add(EnricherHeaders.FAILURE_REASON, reason.name().getBytes(StandardCharsets.UTF_8));
    headers.add(EnricherHeaders.FAILURE_DETAIL, truncate(detail).getBytes(StandardCharsets.UTF_8));
    return headers;
  }

  private static String truncate(String text) {
    return text.length() <= 1000 ? text : text.substring(0, 1000);
  }

  @Override
  public synchronized void stop() {
    for (ConcurrentMessageListenerContainer<String, String> container : containers) {
      container.stop();
    }
    containers.clear();
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

  /** The running containers, for health checks and tests. */
  public synchronized List<ConcurrentMessageListenerContainer<String, String>> containers() {
    return List.copyOf(containers);
  }
}
