package io.github.xeeshan16.enricher.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.xeeshan16.enricher.engine.EnrichmentOutcome;
import io.github.xeeshan16.enricher.engine.ResultSource;
import io.github.xeeshan16.enricher.pipeline.InputRecord;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import java.util.Locale;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * The default output: the full envelope as JSON, keyed by the input key so per-key ordering carries
 * over to the output topic, with pipeline, prompt version, input hash and result source headers.
 */
public final class EnvelopeRecordFactory implements OutputRecordFactory {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public ProducerRecord<String, String> enriched(
      PipelineDefinition pipeline,
      InputRecord input,
      EnrichmentOutcome.Enriched outcome,
      ResultSource source) {
    String value;
    try {
      value = MAPPER.writeValueAsString(outcome.envelope());
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("cannot serialise envelope", e);
    }
    ProducerRecord<String, String> record =
        new ProducerRecord<>(pipeline.outputTopic(), input.key(), value);
    record.headers().add(EnricherHeaders.PIPELINE, EnricherHeaders.utf8(pipeline.name()));
    record
        .headers()
        .add(EnricherHeaders.PROMPT_VERSION, EnricherHeaders.utf8(pipeline.promptVersion()));
    record.headers().add(EnricherHeaders.INPUT_HASH, EnricherHeaders.utf8(outcome.inputHash()));
    record
        .headers()
        .add(
            EnricherHeaders.RESULT_SOURCE,
            EnricherHeaders.utf8(source.name().toLowerCase(Locale.ROOT)));
    return record;
  }
}
