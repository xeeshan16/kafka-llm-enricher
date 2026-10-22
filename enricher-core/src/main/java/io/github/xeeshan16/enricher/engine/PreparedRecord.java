package io.github.xeeshan16.enricher.engine;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.xeeshan16.enricher.llm.LlmRequest;
import io.github.xeeshan16.enricher.pipeline.InputRecord;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import java.util.Objects;

/**
 * A record ready to be sent to the model: parsed input, its canonical hash and the rendered
 * request.
 *
 * @param pipeline the pipeline it belongs to
 * @param record the consumed record
 * @param input the parsed value
 * @param inputHash {@code sha256:<hex>} of the canonical input
 * @param request the provider request
 */
public record PreparedRecord(
    PipelineDefinition pipeline,
    InputRecord record,
    JsonNode input,
    String inputHash,
    LlmRequest request) {

  public PreparedRecord {
    Objects.requireNonNull(pipeline, "pipeline");
    Objects.requireNonNull(record, "record");
    Objects.requireNonNull(input, "input");
    Objects.requireNonNull(inputHash, "inputHash");
    Objects.requireNonNull(request, "request");
  }
}
