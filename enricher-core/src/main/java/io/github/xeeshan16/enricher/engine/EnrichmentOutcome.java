package io.github.xeeshan16.enricher.engine;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import java.time.Instant;

/**
 * Result of handling one record: an output envelope, a classified failure, a deliberate skip, or a
 * deferral until the tenant's budget resets.
 */
public sealed interface EnrichmentOutcome {

  /** {@code sha256:<hex>} of the canonical input value. */
  String inputHash();

  /**
   * The provider response once the model was called (for {@link Enriched} and {@link Failed}), or
   * {@code null} when no call was made ({@link Skipped}, {@link Deferred}).
   */
  LlmResponse response();

  /** The record was enriched; {@code envelope} is ready to publish. */
  record Enriched(String inputHash, ObjectNode envelope, LlmResponse response)
      implements EnrichmentOutcome {}

  /** The model answered, but the answer is unusable. */
  record Failed(String inputHash, FailureReason reason, String detail, LlmResponse response)
      implements EnrichmentOutcome {}

  /**
   * The record was deliberately not sent to the model.
   *
   * @param reason {@code rule:<name>} for a skip rule, {@code over-budget} for a tenant out of
   *     budget
   * @param envelope an envelope with {@code "enrichment": null} and a {@code "skipped"} object, for
   *     pipelines that keep one output record per input
   */
  record Skipped(String inputHash, String reason, ObjectNode envelope)
      implements EnrichmentOutcome {
    @Override
    public LlmResponse response() {
      return null;
    }
  }

  /**
   * The tenant's budget is exhausted; enrich the record once it resets.
   *
   * @param tenant the tenant charged
   * @param until start of the next budget window
   * @param detail why, for headers and logs
   */
  record Deferred(String inputHash, String tenant, Instant until, String detail)
      implements EnrichmentOutcome {
    @Override
    public LlmResponse response() {
      return null;
    }
  }
}
