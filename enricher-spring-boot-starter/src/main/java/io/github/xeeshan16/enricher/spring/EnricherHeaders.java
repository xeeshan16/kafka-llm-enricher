package io.github.xeeshan16.enricher.spring;

/** Kafka header names written by the enricher. Values are UTF-8 strings. */
public final class EnricherHeaders {

  /** Pipeline name, on enriched and dead-lettered records. */
  public static final String PIPELINE = "enricher-pipeline";

  /** Prompt version, on enriched records. */
  public static final String PROMPT_VERSION = "enricher-prompt-version";

  /** {@code sha256:<hex>} of the canonical input, on enriched records. */
  public static final String INPUT_HASH = "enricher-input-hash";

  /** {@link io.github.xeeshan16.enricher.engine.FailureReason} name, on dead-lettered records. */
  public static final String FAILURE_REASON = "enricher-failure-reason";

  /** Human-readable failure detail, on dead-lettered records. */
  public static final String FAILURE_DETAIL = "enricher-failure-detail";

  private EnricherHeaders() {}
}
