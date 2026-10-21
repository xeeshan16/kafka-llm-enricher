package io.github.xeeshan16.enricher.llm;

import java.time.Duration;
import java.util.Objects;

/**
 * A completion result.
 *
 * @param text raw model output
 * @param model the model that actually answered (may differ from the requested alias)
 * @param usage token usage as reported by the provider, or estimated when it reports none
 * @param latency wall-clock time spent in the provider call
 */
public record LlmResponse(String text, String model, TokenUsage usage, Duration latency) {

  public LlmResponse {
    Objects.requireNonNull(text, "text");
    Objects.requireNonNull(model, "model");
    Objects.requireNonNull(usage, "usage");
    Objects.requireNonNull(latency, "latency");
  }
}
