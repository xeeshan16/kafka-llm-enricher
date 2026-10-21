package io.github.xeeshan16.enricher.spring;

import io.github.xeeshan16.enricher.engine.EnrichmentOutcome;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;

/** Writes an enriched envelope to the pipeline's output. Must block until the write is durable. */
@FunctionalInterface
public interface EnrichedRecordSink {

  void publish(PipelineDefinition pipeline, String key, EnrichmentOutcome.Enriched outcome);
}
