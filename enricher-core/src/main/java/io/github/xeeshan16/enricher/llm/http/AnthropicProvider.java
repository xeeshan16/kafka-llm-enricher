package io.github.xeeshan16.enricher.llm.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.xeeshan16.enricher.llm.LlmProviderException;
import io.github.xeeshan16.enricher.llm.LlmRequest;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Clock;
import java.time.Duration;

/**
 * The Anthropic Messages API ({@code POST <base-url>/v1/messages}).
 *
 * <p>The response schema is added to the system prompt; the engine validates the answer and, if
 * configured, sends a repair prompt. HTTP 529 ("overloaded") is treated like 429.
 */
public final class AnthropicProvider extends HttpLlmProvider {

  /** Value of the {@code anthropic-version} header. */
  public static final String API_VERSION = "2023-06-01";

  private final Double temperature;

  /**
   * @param temperature sampling temperature, or {@code null} for the API default
   */
  public AnthropicProvider(HttpProviderSettings settings, Double temperature) {
    super(settings);
    this.temperature = temperature;
  }

  public AnthropicProvider(
      HttpProviderSettings settings, Double temperature, HttpClient client, Clock clock) {
    super(settings, client, clock);
    this.temperature = temperature;
  }

  @Override
  public String name() {
    return "anthropic";
  }

  @Override
  protected URI endpoint(LlmRequest request) {
    return settings.resolve("/v1/messages");
  }

  @Override
  protected void authenticate(HttpRequest.Builder builder) {
    builder.header("anthropic-version", API_VERSION);
    if (settings.hasApiKey()) {
      builder.header("x-api-key", settings.apiKey());
    }
  }

  @Override
  protected boolean isThrottled(int status) {
    return status == 529;
  }

  @Override
  protected ObjectNode body(LlmRequest request) {
    ObjectNode body = mapper.createObjectNode();
    body.put("model", request.model());
    body.put("max_tokens", request.maxOutputTokens());
    String system = systemWithSchema(request);
    if (!system.isBlank()) {
      body.put("system", system);
    }
    if (temperature != null) {
      body.put("temperature", temperature);
    }
    body.putArray("messages").addObject().put("role", "user").put("content", request.userPrompt());
    return body;
  }

  @Override
  protected LlmResponse parse(LlmRequest request, JsonNode body, Duration latency) {
    JsonNode content = body.path("content");
    if (!content.isArray()) {
      throw new LlmProviderException(name() + ": response has no content", true);
    }
    StringBuilder text = new StringBuilder();
    for (JsonNode block : content) {
      if ("text".equals(block.path("type").asText())) {
        text.append(block.path("text").asText());
      }
    }
    String answer = text.toString();
    String model = body.path("model").isTextual() ? body.path("model").asText() : request.model();
    return new LlmResponse(
        answer,
        model,
        usage(body.path("usage"), "input_tokens", "output_tokens", request, answer),
        latency);
  }
}
