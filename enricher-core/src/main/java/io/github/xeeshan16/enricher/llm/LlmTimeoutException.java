package io.github.xeeshan16.enricher.llm;

import java.time.Duration;

/** A provider call did not answer within its timeout. Always retryable. */
public class LlmTimeoutException extends LlmProviderException {

  private static final long serialVersionUID = 1L;

  public LlmTimeoutException(String provider, Duration timeout) {
    super(provider + " call timed out after " + timeout.toMillis() + " ms", true);
  }
}
