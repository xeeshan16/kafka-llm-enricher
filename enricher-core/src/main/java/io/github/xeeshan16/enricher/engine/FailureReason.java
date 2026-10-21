package io.github.xeeshan16.enricher.engine;

/** Why a record could not be enriched. Written to the dead-letter header. */
public enum FailureReason {
  /** The model answer contained no parseable JSON object. */
  INVALID_JSON,
  /** The answer was JSON but did not satisfy the pipeline's output schema. */
  SCHEMA_VIOLATION,
  /** The provider failed with a non-retryable error, or retries were exhausted. */
  PROVIDER_ERROR,
  /** Anything else, for example the output topic could not be written. */
  PROCESSING_ERROR
}
