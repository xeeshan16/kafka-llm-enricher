package io.github.xeeshan16.enricher.llm;

import java.time.Duration;
import java.util.Optional;

/** A provider call failed before producing a usable response. */
public class LlmProviderException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final boolean retryable;
  private final Duration retryAfter;

  public LlmProviderException(String message, boolean retryable) {
    this(message, retryable, null, null);
  }

  public LlmProviderException(String message, boolean retryable, Throwable cause) {
    this(message, retryable, null, cause);
  }

  /**
   * @param retryAfter how long the provider asked callers to wait (an HTTP {@code Retry-After}), or
   *     {@code null} when it gave no hint
   */
  public LlmProviderException(
      String message, boolean retryable, Duration retryAfter, Throwable cause) {
    super(message, cause);
    this.retryable = retryable;
    this.retryAfter = retryAfter;
  }

  /** Whether repeating the same request later can succeed (timeouts, throttling, 5xx). */
  public boolean retryable() {
    return retryable;
  }

  /** The wait the provider asked for before the next attempt, if it sent one. */
  public Optional<Duration> retryAfter() {
    return Optional.ofNullable(retryAfter);
  }
}
