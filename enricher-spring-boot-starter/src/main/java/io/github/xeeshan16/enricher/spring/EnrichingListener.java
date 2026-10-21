package io.github.xeeshan16.enricher.spring;

import io.github.xeeshan16.enricher.engine.Enricher;
import io.github.xeeshan16.enricher.engine.EnrichmentOutcome;
import io.github.xeeshan16.enricher.pipeline.InputRecord;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.MessageListener;

/**
 * Record listener for one pipeline: enrich, then publish or fail.
 *
 * <p>Unusable answers throw {@link EnrichmentFailedException}, which the container's error handler
 * routes to the dead-letter topic without retrying. Provider and publish failures throw their own
 * exceptions and are retried by the error handler first. Tombstones (null values) are skipped.
 */
public final class EnrichingListener implements MessageListener<String, String> {

  private static final Logger log = LoggerFactory.getLogger(EnrichingListener.class);

  private final PipelineDefinition pipeline;
  private final Enricher enricher;
  private final EnrichedRecordSink sink;

  public EnrichingListener(PipelineDefinition pipeline, Enricher enricher, EnrichedRecordSink sink) {
    this.pipeline = pipeline;
    this.enricher = enricher;
    this.sink = sink;
  }

  @Override
  public void onMessage(ConsumerRecord<String, String> record) {
    if (record.value() == null) {
      log.debug(
          "pipeline={} skipping tombstone {}-{}@{}",
          pipeline.name(),
          record.topic(),
          record.partition(),
          record.offset());
      return;
    }
    InputRecord input =
        new InputRecord(
            record.topic(), record.partition(), record.offset(), record.key(), record.value());
    EnrichmentOutcome outcome = enricher.enrich(pipeline, input);
    switch (outcome) {
      case EnrichmentOutcome.Enriched enriched -> {
        sink.publish(pipeline, record.key(), enriched);
        log.debug(
            "pipeline={} enriched {}-{}@{} inputHash={} tokens={}",
            pipeline.name(),
            record.topic(),
            record.partition(),
            record.offset(),
            enriched.inputHash(),
            enriched.response().usage().totalTokens());
      }
      case EnrichmentOutcome.Failed failed -> {
        log.warn(
            "pipeline={} cannot enrich {}-{}@{}: {} {}",
            pipeline.name(),
            record.topic(),
            record.partition(),
            record.offset(),
            failed.reason(),
            failed.detail());
        throw new EnrichmentFailedException(failed.reason(), failed.detail());
      }
    }
  }
}
