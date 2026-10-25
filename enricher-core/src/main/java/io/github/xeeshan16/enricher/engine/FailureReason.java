package io.github.xeeshan16.enricher.engine;

/** Why a record could not be enriched. Written to the dead-letter header. */
public enum FailureReason {
  /** The model answer contained no parseable JSON object. */
  INVALID_JSON,
  /** The answer was JSON but did not satisfy the pipeline's output schema. */
  SCHEMA_VIOLATION,
  /**
   * The answer contained personal data and the pipeline's {@code output-pii} action is {@code
   * reject}.
   */
  PII_IN_OUTPUT,
  /** The provider failed with a non-retryable error (bad request, authentication). */
  PROVIDER_ERROR,
  /**
   * A retryable provider failure (timeout, throttling, 5xx) persisted through every in-place
   * attempt and every retry topic.
   */
  RETRIES_EXHAUSTED,
  /** Another worker held the record's ledger lease for longer than every retry allowed. */
  LEDGER_BUSY,
  /**
   * The tenant's budget was exhausted; written on records parked on the deferred topic until the
   * budget window resets.
   */
  BUDGET_EXCEEDED,
  /** Anything else, for example a bug or an exception thrown while handling the record. */
  PROCESSING_ERROR
}
