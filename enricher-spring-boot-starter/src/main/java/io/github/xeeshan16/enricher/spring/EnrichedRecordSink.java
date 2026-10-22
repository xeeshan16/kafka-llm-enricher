package io.github.xeeshan16.enricher.spring;

import io.github.xeeshan16.enricher.engine.EnrichmentOutcome;
import io.github.xeeshan16.enricher.engine.ResultSource;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;

/**
 * Writes an enriched envelope to the pipeline's output. Must block until the write is durable, or,
 * inside a Kafka transaction, until the record is part of the transaction.
 */
@FunctionalInterface
public interface EnrichedRecordSink {

  /**
   * @param pipeline the pipeline
   * @param key the input record's key, reused as the output key so per-key ordering carries over
   * @param outcome the envelope to publish
   * @param source whether this delivery paid for a provider call or reused a ledger result
   */
  void publish(
      PipelineDefinition pipeline,
      String key,
      EnrichmentOutcome.Enriched outcome,
      ResultSource source);
}
