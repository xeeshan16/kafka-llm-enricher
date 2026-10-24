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

  /**
   * The output for a record that was deliberately not sent to the model (a skip rule matched, or
   * its tenant was over budget with action {@code skip}). The default writes the skipped envelope,
   * keyed like enriched output, so the output topic keeps one record per input; it carries {@code
   * enricher-result-source: none} and an {@code enricher-skip-reason} header.
   */
  default ProducerRecord<String, String> skipped(
      PipelineDefinition pipeline, InputRecord input, EnrichmentOutcome.Skipped outcome) {
    ProducerRecord<String, String> record =
        new ProducerRecord<>(pipeline.outputTopic(), input.key(), outcome.envelope().toString());
    record.headers().add(EnricherHeaders.PIPELINE, EnricherHeaders.utf8(pipeline.name()));
    record
        .headers()
        .add(EnricherHeaders.PROMPT_VERSION, EnricherHeaders.utf8(pipeline.promptVersion()));
    record.headers().add(EnricherHeaders.INPUT_HASH, EnricherHeaders.utf8(outcome.inputHash()));
    record.headers().add(EnricherHeaders.RESULT_SOURCE, EnricherHeaders.utf8("none"));
    record.headers().add(EnricherHeaders.SKIP_REASON, EnricherHeaders.utf8(outcome.reason()));
    return record;
  }
}
