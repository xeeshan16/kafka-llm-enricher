package io.github.xeeshan16.enricher.llm;

/**
 * Rough token estimate for providers that report no usage, and for budgeting before a call.
 *
 * <p>Uses the common four-characters-per-token rule of thumb for English text. It is an estimate,
 * not a tokenizer: real counts differ by model and language.
 */
public final class TokenEstimator {

  private TokenEstimator() {}

  public static long estimate(String text) {
    if (text == null || text.isEmpty()) {
      return 0;
    }
    return (text.length() + 3L) / 4;
  }

  /**
   * Worst-case usage of a request before it is sent: the estimated prompt tokens plus the full
   * output cap. Used to reserve rate-limit and budget capacity, then settled against real usage.
   */
  public static TokenUsage estimate(LlmRequest request) {
    return new TokenUsage(
        estimate(request.systemPrompt()) + estimate(request.userPrompt()),
        request.maxOutputTokens());
  }
}
