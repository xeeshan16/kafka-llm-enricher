package io.github.xeeshan16.enricher.spring;

import io.github.xeeshan16.enricher.engine.FailureReason;

/**
 * Thrown by the listener when the model answered but the answer is unusable. Not retried: the
 * record goes straight to the dead-letter topic with the reason in a header.
 */
public class EnrichmentFailedException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final FailureReason reason;

  public EnrichmentFailedException(FailureReason reason, String detail) {
    super(reason + ": " + detail);
    this.reason = reason;
  }

  public FailureReason reason() {
    return reason;
  }
}
