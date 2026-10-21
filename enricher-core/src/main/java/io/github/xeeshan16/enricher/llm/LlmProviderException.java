package io.github.xeeshan16.enricher.llm;

/** A provider call failed before producing a usable response. */
public class LlmProviderException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final boolean retryable;

  public LlmProviderException(String message, boolean retryable) {
    super(message);
    this.retryable = retryable;
  }

  public LlmProviderException(String message, boolean retryable, Throwable cause) {
    super(message, cause);
    this.retryable = retryable;
  }

  /** Whether repeating the same request later can succeed (timeouts, throttling, 5xx). */
  public boolean retryable() {
    return retryable;
  }
}
