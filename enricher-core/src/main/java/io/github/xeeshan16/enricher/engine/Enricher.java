package io.github.xeeshan16.enricher.engine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.xeeshan16.enricher.json.CanonicalJson;
import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.LlmRequest;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import io.github.xeeshan16.enricher.pipeline.InputRecord;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import java.util.List;
import java.util.Objects;

/**
 * Enriches one record: parse, render the prompt, call the model, extract and validate the JSON
 * answer, and build the output envelope.
 *
 * <p>This class does no I/O besides the provider call and holds no per-record state, so one
 * instance serves every pipeline and thread. Provider failures propagate as {@link
 * io.github.xeeshan16.enricher.llm.LlmProviderException} so the caller can apply its retry policy;
 * answers that are unusable come back as {@link EnrichmentOutcome.Failed}.
 *
 * <p>The envelope contains no wall-clock time, so the same input, prompt version and answer always
 * produce byte-identical output. That keeps replays and duplicates easy to compare downstream.
 */
public final class Enricher {

  /** Envelope format version, written as {@code "format"}. */
  public static final String ENVELOPE_FORMAT = "kafka-llm-enricher/v1";

  private static final int MAX_DETAIL_ERRORS = 5;

  private final LlmProvider provider;
  private final ObjectMapper mapper;

  public Enricher(LlmProvider provider) {
    this(provider, new ObjectMapper());
  }

  public Enricher(LlmProvider provider, ObjectMapper mapper) {
    this.provider = Objects.requireNonNull(provider, "provider");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
  }

  public LlmProvider provider() {
    return provider;
  }

  /** Parses a record value: JSON when it is a JSON object or array, otherwise a string. */
  public JsonNode parseValue(String value) {
    String trimmed = value.strip();
    if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
      try {
        return mapper.readTree(trimmed);
      } catch (JsonProcessingException e) {
        // Not JSON after all; fall through and treat it as text.
      }
    }
    return mapper.getNodeFactory().textNode(value);
  }

  public EnrichmentOutcome enrich(PipelineDefinition pipeline, InputRecord record) {
    JsonNode input = parseValue(record.value());
    String inputHash = CanonicalJson.sha256(input);
    String userPrompt = pipeline.userTemplate().render(input, record.key(), record.topic());
    LlmRequest request =
        new LlmRequest(
            pipeline.model(),
            pipeline.systemPrompt(),
            userPrompt,
            pipeline.outputSchema().node(),
            pipeline.maxOutputTokens());
    LlmResponse response = provider.complete(request);

    String json = extractJsonObject(response.text());
    if (json == null) {
      return new EnrichmentOutcome.Failed(
          inputHash, FailureReason.INVALID_JSON, "answer contains no JSON object", response);
    }
    JsonNode enrichment;
    try {
      enrichment = mapper.readTree(json);
    } catch (JsonProcessingException e) {
      return new EnrichmentOutcome.Failed(
          inputHash, FailureReason.INVALID_JSON, e.getOriginalMessage(), response);
    }
    List<String> errors = pipeline.outputSchema().validate(enrichment);
    if (!errors.isEmpty()) {
      String detail =
          String.join("; ", errors.subList(0, Math.min(errors.size(), MAX_DETAIL_ERRORS)));
      return new EnrichmentOutcome.Failed(
          inputHash, FailureReason.SCHEMA_VIOLATION, detail, response);
    }
    return new EnrichmentOutcome.Enriched(
        inputHash, envelope(pipeline, record, input, inputHash, enrichment, response), response);
  }

  private ObjectNode envelope(
      PipelineDefinition pipeline,
      InputRecord record,
      JsonNode input,
      String inputHash,
      JsonNode enrichment,
      LlmResponse response) {
    ObjectNode envelope = mapper.createObjectNode();
    envelope.put("format", ENVELOPE_FORMAT);
    envelope.put("pipeline", pipeline.name());
    envelope.put("promptVersion", pipeline.promptVersion());
    envelope.put("model", response.model());
    envelope.put("inputHash", inputHash);
    ObjectNode source = envelope.putObject("source");
    source.put("topic", record.topic());
    source.put("partition", record.partition());
    source.put("offset", record.offset());
    if (record.key() == null) {
      source.putNull("key");
    } else {
      source.put("key", record.key());
    }
    envelope.set("input", input);
    envelope.set("enrichment", enrichment);
    ObjectNode usage = envelope.putObject("usage");
    usage.put("inputTokens", response.usage().inputTokens());
    usage.put("outputTokens", response.usage().outputTokens());
    return envelope;
  }

  /**
   * Finds the JSON object in a model answer, tolerating Markdown code fences and text around it.
   * Returns {@code null} when there is no {@code {...}} span.
   */
  static String extractJsonObject(String text) {
    int start = text.indexOf('{');
    int end = text.lastIndexOf('}');
    if (start < 0 || end < start) {
      return null;
    }
    return text.substring(start, end + 1);
  }
}
