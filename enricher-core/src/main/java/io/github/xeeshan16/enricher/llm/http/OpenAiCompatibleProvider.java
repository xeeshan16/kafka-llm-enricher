package io.github.xeeshan16.enricher.llm.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.xeeshan16.enricher.llm.LlmProviderException;
import io.github.xeeshan16.enricher.llm.LlmRequest;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

/**
 * The OpenAI Chat Completions API ({@code POST <base-url>/chat/completions}), which OpenAI, Ollama
 * ({@code http://localhost:11434/v1}), vLLM ({@code http://host:8000/v1}), LiteLLM and most
 * gateways serve.
 *
 * <p>The response schema is sent according to {@link StructuredOutput}: as a {@code json_schema}
 * response format (OpenAI, recent Ollama and vLLM), as plain JSON mode with the schema in the
 * system prompt, or only in the system prompt. Whatever the server does with it, the engine still
 * validates every answer.
 */
public class OpenAiCompatibleProvider extends HttpLlmProvider {

  /** How the response schema reaches the server. */
  public enum StructuredOutput {
    /** {@code response_format: {type: json_schema}}: the server constrains decoding. */
    JSON_SCHEMA,
    /** {@code response_format: {type: json_object}} plus the schema in the system prompt. */
    JSON_OBJECT,
    /** No response format; the schema goes into the system prompt only. */
    NONE
  }

  /**
   * Generation options.
   *
   * @param structuredOutput how the schema is passed
   * @param temperature sampling temperature; {@code null} leaves the server default (some reasoning
   *     models accept no other value)
   * @param maxCompletionTokensField send the output cap as {@code max_completion_tokens} (required
   *     by OpenAI's reasoning models) instead of {@code max_tokens}
   */
  public record Options(
      StructuredOutput structuredOutput, Double temperature, boolean maxCompletionTokensField) {
    public Options {
      Objects.requireNonNull(structuredOutput, "structuredOutput");
    }

    public static Options defaults() {
      return new Options(StructuredOutput.JSON_SCHEMA, 0.0, false);
    }
  }

  private final Options options;

  public OpenAiCompatibleProvider(HttpProviderSettings settings, Options options) {
    super(settings);
    this.options = Objects.requireNonNull(options, "options");
  }

  public OpenAiCompatibleProvider(
      HttpProviderSettings settings, Options options, HttpClient client, Clock clock) {
    super(settings, client, clock);
    this.options = Objects.requireNonNull(options, "options");
  }

  public Options options() {
    return options;
  }

  @Override
  public String name() {
    return "openai";
  }

  @Override
  protected URI endpoint(LlmRequest request) {
    return settings.resolve("/chat/completions");
  }

  @Override
  protected void authenticate(HttpRequest.Builder builder) {
    if (settings.hasApiKey()) {
      builder.header("Authorization", "Bearer " + settings.apiKey());
    }
  }

  @Override
  protected ObjectNode body(LlmRequest request) {
    ObjectNode body = mapper.createObjectNode();
    body.put("model", request.model());
    boolean schema = request.responseSchema() != null && !request.responseSchema().isNull();
    String system =
        schema && options.structuredOutput() != StructuredOutput.JSON_SCHEMA
            ? systemWithSchema(request)
            : request.systemPrompt();
    ArrayNode messages = body.putArray("messages");
    if (!system.isBlank()) {
      messages.addObject().put("role", "system").put("content", system);
    }
    messages.addObject().put("role", "user").put("content", request.userPrompt());
    body.put(
        options.maxCompletionTokensField() ? "max_completion_tokens" : "max_tokens",
        request.maxOutputTokens());
    if (options.temperature() != null) {
      body.put("temperature", options.temperature());
    }
    if (schema) {
      switch (options.structuredOutput()) {
        case JSON_SCHEMA -> {
          ObjectNode format = body.putObject("response_format");
          format.put("type", "json_schema");
          ObjectNode jsonSchema = format.putObject("json_schema");
          jsonSchema.put("name", "enrichment");
          // Not strict: strict mode rejects common schema features (optional fields, formats).
          jsonSchema.put("strict", false);
          jsonSchema.set("schema", request.responseSchema());
        }
        case JSON_OBJECT -> body.putObject("response_format").put("type", "json_object");
        case NONE -> {
          // schema is in the system prompt
        }
      }
    }
    return body;
  }

  @Override
  protected LlmResponse parse(LlmRequest request, JsonNode body, Duration latency) {
    JsonNode choice = body.path("choices").path(0);
    if (choice.isMissingNode()) {
      throw new LlmProviderException(name() + ": response has no choices", true);
    }
    JsonNode message = choice.path("message");
    String text;
    if (message.path("content").isTextual()) {
      text = message.path("content").asText();
    } else if (message.path("refusal").isTextual()) {
      // A refusal is an answer, just not a usable one: the engine reports it as invalid JSON.
      text = message.path("refusal").asText();
    } else {
      text = "";
    }
    String model = body.path("model").isTextual() ? body.path("model").asText() : request.model();
    return new LlmResponse(
        text,
        model,
        usage(body.path("usage"), "prompt_tokens", "completion_tokens", request, text),
        latency);
  }
}
