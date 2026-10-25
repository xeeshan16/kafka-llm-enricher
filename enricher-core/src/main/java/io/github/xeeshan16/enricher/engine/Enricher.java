package io.github.xeeshan16.enricher.engine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.xeeshan16.enricher.json.CanonicalJson;
import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.LlmRequest;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import io.github.xeeshan16.enricher.llm.TokenUsage;
import io.github.xeeshan16.enricher.pipeline.InputRecord;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import io.github.xeeshan16.enricher.pipeline.PromptTemplate;
import io.github.xeeshan16.enricher.privacy.OutputPiiAction;
import io.github.xeeshan16.enricher.privacy.PiiMasker;
import io.github.xeeshan16.enricher.privacy.PromptSafety;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Enriches one record: parse, mask personal data, render the prompt, call the model, extract and
 * validate the JSON answer (with an optional repair prompt), and build the output envelope.
 *
 * <p>This class does no I/O besides the provider call and holds no per-record state, so one
 * instance serves every pipeline and thread. Provider failures propagate as {@link
 * io.github.xeeshan16.enricher.llm.LlmProviderException} so the caller can apply its retry policy;
 * answers that are unusable come back as {@link EnrichmentOutcome.Failed}.
 *
 * <p>The envelope contains no wall-clock time and the enrichment is written in canonical form
 * (sorted keys), so the same record, prompt version and answer always produce byte-identical
 * output, whether the answer came from the provider or from the result ledger. That keeps replays
 * and duplicates easy to compare downstream.
 */
public final class Enricher {

  /** Envelope format version, written as {@code "format"}. */
  public static final String ENVELOPE_FORMAT = "kafka-llm-enricher/v1";

  private static final int MAX_DETAIL_ERRORS = 5;
  private static final int MAX_REPAIR_ECHO_CHARS = 4000;

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

  /**
   * Enriches one record: {@link #prepare}, a provider call and {@link #interpret}, then up to the
   * pipeline's {@code repairAttempts} repair calls while the answer is unusable.
   */
  public EnrichmentOutcome enrich(PipelineDefinition pipeline, InputRecord record) {
    PreparedRecord prepared = prepare(pipeline, record);
    LlmResponse response = provider.complete(prepared.request());
    EnrichmentOutcome outcome = interpret(prepared, response);
    for (int i = 0; i < pipeline.repairAttempts() && isRepairable(outcome); i++) {
      LlmResponse repaired =
          provider.complete(repairRequest(prepared, (EnrichmentOutcome.Failed) outcome));
      response = combine(response, repaired);
      outcome = interpret(prepared, response);
    }
    return outcome;
  }

  /**
   * Parses the record, hashes its canonical form and renders the request, without calling the
   * provider. The hash is what the result ledger keys on.
   */
  public PreparedRecord prepare(PipelineDefinition pipeline, InputRecord record) {
    JsonNode input = parseValue(record.value());
    String inputHash = CanonicalJson.sha256(input);
    PromptSafety safety = pipeline.safety();
    PiiMasker.Masked masked = safety.masker().mask(input);
    String key = record.key() == null ? null : safety.masker().maskText(record.key());
    String userPrompt = pipeline.userTemplate().render(masked.value(), key, record.topic());
    LlmRequest request =
        new LlmRequest(
            pipeline.model(),
            safety.systemPrompt(pipeline.systemPrompt()),
            userPrompt,
            pipeline.outputSchema().node(),
            pipeline.maxOutputTokens());
    return new PreparedRecord(pipeline, record, input, inputHash, request, masked.counts());
  }

  /**
   * Whether a repair prompt can fix {@code outcome}: the answer was not JSON or broke the schema.
   */
  public static boolean isRepairable(EnrichmentOutcome outcome) {
    return outcome instanceof EnrichmentOutcome.Failed f
        && (f.reason() == FailureReason.INVALID_JSON
            || f.reason() == FailureReason.SCHEMA_VIOLATION);
  }

  /**
   * The follow-up request for an unusable answer: the original task, the rejected answer and what
   * was wrong with it, and the instruction to return a corrected object only.
   *
   * <p>The rejected answer and the error messages are untrusted (the answer may echo event text),
   * so both are written as JSON strings with {@code <} escaped, like event content, and cannot
   * close the {@code <event>} block of the original prompt.
   */
  public LlmRequest repairRequest(PreparedRecord prepared, EnrichmentOutcome.Failed failed) {
    LlmRequest original = prepared.request();
    String previous = failed.response() == null ? "" : failed.response().text();
    String userPrompt =
        "Your previous answer to the task below was rejected because "
            + (failed.reason() == FailureReason.INVALID_JSON
                ? "it is not a single valid JSON object"
                : "it does not satisfy the response schema")
            + ".\nProblems (JSON string): "
            + quoted(failed.detail() == null ? "" : failed.detail())
            + "\nRejected answer (JSON string): "
            + quoted(truncate(previous, MAX_REPAIR_ECHO_CHARS))
            + "\nReturn only the corrected JSON object. The original task follows.\n\n"
            + original.userPrompt();
    return new LlmRequest(
        original.model(),
        original.systemPrompt(),
        userPrompt,
        original.responseSchema(),
        original.maxOutputTokens());
  }

  /**
   * One response standing for an original call and its repair: the repair's text and model, the
   * usage and latency of both, since both were paid for.
   */
  public static LlmResponse combine(LlmResponse first, LlmResponse repair) {
    return new LlmResponse(
        repair.text(),
        repair.model(),
        first.usage().plus(repair.usage()),
        first.latency().plus(repair.latency()));
  }

  private String quoted(String text) {
    return PromptTemplate.jsonText(mapper.getNodeFactory().textNode(text));
  }

  private static String truncate(String text, int max) {
    return text.length() <= max ? text : text.substring(0, max) + "...";
  }

  /** Extracts and validates the answer in {@code response} and builds the envelope. */
  public EnrichmentOutcome interpret(PreparedRecord prepared, LlmResponse response) {
    PipelineDefinition pipeline = prepared.pipeline();
    String inputHash = prepared.inputHash();
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
      return schemaViolation(inputHash, errors, response);
    }
    PromptSafety safety = pipeline.safety();
    if (safety.outputPii() != OutputPiiAction.ALLOW && !safety.masker().detectors().isEmpty()) {
      PiiMasker detectorsOnly = new PiiMasker(safety.masker().detectors(), Set.of());
      PiiMasker.Masked scanned = detectorsOnly.mask(enrichment);
      if (scanned.total() > 0) {
        if (safety.outputPii() == OutputPiiAction.REJECT) {
          return new EnrichmentOutcome.Failed(
              inputHash,
              FailureReason.PII_IN_OUTPUT,
              "answer contains personal data: " + scanned.counts(),
              response);
        }
        enrichment = scanned.value();
        errors = pipeline.outputSchema().validate(enrichment);
        if (!errors.isEmpty()) {
          return schemaViolation(inputHash, errors, response);
        }
      }
    }
    return fromStored(
        prepared, CanonicalJson.write(enrichment), response.model(), response.usage(), response);
  }

  private static EnrichmentOutcome.Failed schemaViolation(
      String inputHash, List<String> errors, LlmResponse response) {
    String detail =
        String.join("; ", errors.subList(0, Math.min(errors.size(), MAX_DETAIL_ERRORS)));
    return new EnrichmentOutcome.Failed(
        inputHash, FailureReason.SCHEMA_VIOLATION, detail, response);
  }

  /**
   * Builds the envelope for an enrichment that was already paid for, typically read back from the
   * result ledger. The envelope is byte-identical to the one built when the answer first arrived,
   * provided the record, the canonical enrichment, the model and the usage are the same.
   *
   * @param enrichmentJson the canonical JSON of the validated answer
   * @param model the model that produced it
   * @param usage the tokens spent producing it
   */
  public EnrichmentOutcome.Enriched fromStored(
      PreparedRecord prepared, String enrichmentJson, String model, TokenUsage usage) {
    return fromStored(
        prepared,
        enrichmentJson,
        model,
        usage,
        new LlmResponse(enrichmentJson, model, usage, Duration.ZERO));
  }

  /**
   * The envelope written for a record that was deliberately not sent to the model: the usual header
   * fields and input, {@code "enrichment": null} and {@code "skipped": {"reason": ...}}. It carries
   * no model or usage, since nothing was called, and is deterministic like every envelope.
   */
  public ObjectNode skippedEnvelope(PreparedRecord prepared, String reason) {
    PipelineDefinition pipeline = prepared.pipeline();
    InputRecord record = prepared.record();
    ObjectNode envelope = mapper.createObjectNode();
    envelope.put("format", ENVELOPE_FORMAT);
    envelope.put("pipeline", pipeline.name());
    envelope.put("promptVersion", pipeline.promptVersion());
    envelope.put("inputHash", prepared.inputHash());
    putSource(envelope, record);
    envelope.set("input", prepared.input());
    envelope.putNull("enrichment");
    envelope.putObject("skipped").put("reason", reason);
    return envelope;
  }

  private EnrichmentOutcome.Enriched fromStored(
      PreparedRecord prepared,
      String enrichmentJson,
      String model,
      TokenUsage usage,
      LlmResponse response) {
    JsonNode enrichment;
    try {
      enrichment = mapper.readTree(enrichmentJson);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("stored enrichment is not JSON", e);
    }
    ObjectNode envelope =
        envelope(
            prepared.pipeline(),
            prepared.record(),
            prepared.input(),
            prepared.inputHash(),
            enrichment,
            model,
            usage);
    return new EnrichmentOutcome.Enriched(prepared.inputHash(), envelope, response);
  }

  private ObjectNode envelope(
      PipelineDefinition pipeline,
      InputRecord record,
      JsonNode input,
      String inputHash,
      JsonNode enrichment,
      String model,
      TokenUsage usage) {
    ObjectNode envelope = mapper.createObjectNode();
    envelope.put("format", ENVELOPE_FORMAT);
    envelope.put("pipeline", pipeline.name());
    envelope.put("promptVersion", pipeline.promptVersion());
    envelope.put("model", model);
    envelope.put("inputHash", inputHash);
    putSource(envelope, record);
    envelope.set("input", input);
    envelope.set("enrichment", enrichment);
    ObjectNode usageNode = envelope.putObject("usage");
    usageNode.put("inputTokens", usage.inputTokens());
    usageNode.put("outputTokens", usage.outputTokens());
    return envelope;
  }

  private static void putSource(ObjectNode envelope, InputRecord record) {
    ObjectNode source = envelope.putObject("source");
    source.put("topic", record.topic());
    source.put("partition", record.partition());
    source.put("offset", record.offset());
    if (record.key() == null) {
      source.putNull("key");
    } else {
      source.put("key", record.key());
    }
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
