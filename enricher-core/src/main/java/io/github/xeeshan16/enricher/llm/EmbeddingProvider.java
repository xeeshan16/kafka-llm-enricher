package io.github.xeeshan16.enricher.llm;

import java.util.List;

/** Turns texts into fixed-size vectors. Implementations must be thread-safe. */
public interface EmbeddingProvider {

  String name();

  /** Length of every vector this provider returns. */
  int dimensions();

  /** Embeds every input, preserving order. */
  EmbeddingResponse embed(List<String> inputs);
}
