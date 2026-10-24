package io.github.xeeshan16.enricher.llm;

import java.time.Duration;

/**
 * The provider rejected the call because a quota or rate limit was hit (HTTP 429, or a provider's
 * "overloaded" answer). Always retryable. Provider adapters throw it so the rate limiter can shrink
 * its concurrency and honour the provider's {@code Retry-After}.
 */
public class LlmThrottledException extends LlmProviderException {

  private static final long serialVersionUID = 1L;

  /**
   * @param retryAfter the wait the provider asked for, or {@code null} when it sent none
   */
  public LlmThrottledException(String message, Duration retryAfter) {
    super(message, true, retryAfter, null);
  }
}
