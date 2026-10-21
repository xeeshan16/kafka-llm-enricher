package io.github.xeeshan16.enricher.llm;

/** Input and output token counts of one call. */
public record TokenUsage(long inputTokens, long outputTokens) {

  public static final TokenUsage ZERO = new TokenUsage(0, 0);

  public TokenUsage {
    if (inputTokens < 0 || outputTokens < 0) {
      throw new IllegalArgumentException("token counts must not be negative");
    }
  }

  public long totalTokens() {
    return inputTokens + outputTokens;
  }

  public TokenUsage plus(TokenUsage other) {
    return new TokenUsage(inputTokens + other.inputTokens, outputTokens + other.outputTokens);
  }
}
