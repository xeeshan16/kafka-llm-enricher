package io.github.xeeshan16.enricher.llm;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Objects;

/**
 * One completion request.
 *
 * @param model provider-specific model name
 * @param systemPrompt instructions the model must follow; may be empty but not {@code null}
 * @param userPrompt the rendered prompt, including the framed event
 * @param responseSchema JSON Schema the answer must satisfy, or {@code null} for free text.
 *     Providers that support structured output pass it on; others ignore it and the engine still
 *     validates the answer.
 * @param maxOutputTokens upper bound on generated tokens
 */
public record LlmRequest(
    String model,
    String systemPrompt,
    String userPrompt,
    JsonNode responseSchema,
    int maxOutputTokens) {

  public LlmRequest {
    Objects.requireNonNull(model, "model");
    Objects.requireNonNull(systemPrompt, "systemPrompt");
    Objects.requireNonNull(userPrompt, "userPrompt");
    if (maxOutputTokens <= 0) {
      throw new IllegalArgumentException("maxOutputTokens must be positive: " + maxOutputTokens);
    }
  }
}
