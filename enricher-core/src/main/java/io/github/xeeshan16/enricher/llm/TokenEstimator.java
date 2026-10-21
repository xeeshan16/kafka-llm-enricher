package io.github.xeeshan16.enricher.llm;

/**
 * Rough token estimate for providers that report no usage, and for budgeting before a call.
 *
 * <p>Uses the common four-characters-per-token rule of thumb for English text. It is an
 * estimate, not a tokenizer: real counts differ by model and language.
 */
public final class TokenEstimator {

  private TokenEstimator() {}

  public static long estimate(String text) {
    if (text == null || text.isEmpty()) {
      return 0;
    }
    return (text.length() + 3) / 4;
  }
}
