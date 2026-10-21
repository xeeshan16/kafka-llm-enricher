package io.github.xeeshan16.enricher.spring;

import io.github.xeeshan16.enricher.engine.Enricher;
import io.github.xeeshan16.enricher.engine.FailureReason;
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
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.BackOff;

/**
 * Runs one Spring Kafka listener container per pipeline and owns their lifecycle.
 *
 * <p>Each container commits offsets per record after the listener returns (or after the record was
 * dead-lettered). Failures are classified: unusable answers go to the dead-letter topic at once;
 * provider and publish errors are retried with the configured back-off first.
 */
public final class PipelineRunner implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(PipelineRunner.class);

  private final PipelineRegistry registry;
  private final Enricher enricher;
  private final ConsumerFactory<String, String> consumerFactory;
  private final KafkaOperations<String, String> dlqTemplate;
  private final EnrichedRecordSink sink;
  private final BackOff backOff;
  private final boolean autoStartup;
  private final List<ConcurrentMessageListenerContainer<String, String>> containers =
      new ArrayList<>();
  private volatile boolean running;

  public PipelineRunner(
      PipelineRegistry registry,
      Enricher enricher,
      ConsumerFactory<String, String> consumerFactory,
      KafkaOperations<String, String> dlqTemplate,
      EnrichedRecordSink sink,
      BackOff backOff,
      boolean autoStartup) {
    this.registry = registry;
    this.enricher = enricher;
    this.consumerFactory = consumerFactory;
    this.dlqTemplate = dlqTemplate;
    this.sink = sink;
    this.backOff = backOff;
    this.autoStartup = autoStartup;
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
          "pipeline={} consuming {} -> {} (dlq {}, group {}, promptVersion {}, model {})",
          pipeline.name(),
          pipeline.inputTopic(),
          pipeline.outputTopic(),
          pipeline.deadLetterTopic(),
          registry.groupId(pipeline.name()),
          pipeline.promptVersion(),
          pipeline.model());
    }
    running = true;
  }

  private ConcurrentMessageListenerContainer<String, String> container(
      PipelineDefinition pipeline) {
    ContainerProperties properties = new ContainerProperties(pipeline.inputTopic());
    properties.setGroupId(registry.groupId(pipeline.name()));
    properties.setAckMode(ContainerProperties.AckMode.RECORD);
    properties.setMessageListener(new EnrichingListener(pipeline, enricher, sink));
    ConcurrentMessageListenerContainer<String, String> container =
        new ConcurrentMessageListenerContainer<>(consumerFactory, properties);
    container.setConcurrency(registry.concurrency(pipeline.name()));
    container.setBeanName("enricher-" + pipeline.name());
    container.setCommonErrorHandler(errorHandler(pipeline));
    return container;
  }

  private DefaultErrorHandler errorHandler(PipelineDefinition pipeline) {
    DeadLetterPublishingRecoverer recoverer =
        new DeadLetterPublishingRecoverer(
            dlqTemplate, (record, ex) -> new TopicPartition(pipeline.deadLetterTopic(), -1));
    recoverer.setHeadersFunction((record, ex) -> failureHeaders(pipeline, ex));
    DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
    handler.addNotRetryableExceptions(EnrichmentFailedException.class);
    return handler;
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
