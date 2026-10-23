package io.github.xeeshan16.enricher.kafka;

import io.github.xeeshan16.enricher.engine.EnrichmentOutcome;
import io.github.xeeshan16.enricher.engine.ResultSource;
import io.github.xeeshan16.enricher.pipeline.InputRecord;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Turns an enrichment into the record written to the output topic. Replace it to change the format,
 * for example to write only the enrichment, add headers, or route by a field. Must be thread-safe
 * and deterministic: an output rebuilt from the result ledger after a retry should be identical to
 * the first one.
 */
@FunctionalInterface
public interface OutputRecordFactory {

  /**
   * @param input the original input (for a record on a retry topic, its first topic, partition and
   *     offset)
   * @param source whether this delivery paid for a model call or reused a stored result
   */
  ProducerRecord<String, String> enriched(
      PipelineDefinition pipeline,
      InputRecord input,
      EnrichmentOutcome.Enriched outcome,
      ResultSource source);
}
