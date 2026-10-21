package io.github.xeeshan16.enricher.llm.fake;

import io.github.xeeshan16.enricher.llm.EmbeddingProvider;
import io.github.xeeshan16.enricher.llm.EmbeddingResponse;
import io.github.xeeshan16.enricher.llm.TokenEstimator;
import io.github.xeeshan16.enricher.llm.TokenUsage;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Deterministic hashed bag-of-words embeddings.
 *
 * <p>Each lowercase word is hashed into one signed bucket and the vector is L2-normalised, so texts
 * sharing words have positive cosine similarity. Good enough for tests of similarity-based
 * features; not a semantic model.
 */
public final class FakeEmbeddingProvider implements EmbeddingProvider {

  private static final Pattern NON_WORD = Pattern.compile("[^\\p{L}\\p{N}]+");

  private final int dimensions;

  public FakeEmbeddingProvider(int dimensions) {
    if (dimensions <= 0) {
      throw new IllegalArgumentException("dimensions must be positive: " + dimensions);
    }
    this.dimensions = dimensions;
  }

  @Override
  public String name() {
    return "fake";
  }

  @Override
  public int dimensions() {
    return dimensions;
  }

  @Override
  public EmbeddingResponse embed(List<String> inputs) {
    List<float[]> vectors = new ArrayList<>(inputs.size());
    long tokens = 0;
    for (String input : inputs) {
      vectors.add(vector(input));
      tokens += TokenEstimator.estimate(input);
    }
    return new EmbeddingResponse(vectors, "fake-embed-" + dimensions, new TokenUsage(tokens, 0));
  }

  private float[] vector(String text) {
    float[] v = new float[dimensions];
    for (String word : NON_WORD.splitAsStream(text.toLowerCase(Locale.ROOT)).toList()) {
      if (word.isEmpty()) {
        continue;
      }
      int h = mix(word.hashCode());
      int bucket = Math.floorMod(h, dimensions);
      v[bucket] += ((h >>> 16) & 1) == 0 ? 1f : -1f;
    }
    double norm = 0;
    for (float x : v) {
      norm += (double) x * x;
    }
    if (norm > 0) {
      float inv = (float) (1.0 / Math.sqrt(norm));
      for (int i = 0; i < v.length; i++) {
        v[i] *= inv;
      }
    }
    return v;
  }

  /** Murmur3 finaliser, so similar strings land in unrelated buckets. */
  private static int mix(int h) {
    h ^= h >>> 16;
    h *= 0x85ebca6b;
    h ^= h >>> 13;
    h *= 0xc2b2ae35;
    h ^= h >>> 16;
    return h;
  }
}
