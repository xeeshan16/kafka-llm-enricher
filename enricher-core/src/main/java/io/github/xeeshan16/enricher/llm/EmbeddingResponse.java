package io.github.xeeshan16.enricher.llm;

import java.util.List;
import java.util.Objects;

/**
 * Embedding vectors in input order.
 *
 * @param vectors one vector per input; callers must not mutate the arrays
 * @param model the model that produced them
 * @param usage input tokens consumed
 */
public record EmbeddingResponse(List<float[]> vectors, String model, TokenUsage usage) {

  public EmbeddingResponse {
    vectors = List.copyOf(vectors);
    Objects.requireNonNull(model, "model");
    Objects.requireNonNull(usage, "usage");
  }
}
