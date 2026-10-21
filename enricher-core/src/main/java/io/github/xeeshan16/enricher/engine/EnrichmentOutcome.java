package io.github.xeeshan16.enricher.engine;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.xeeshan16.enricher.llm.LlmResponse;

/** Result of enriching one record: either an output envelope or a classified failure. */
public sealed interface EnrichmentOutcome {

  /** {@code sha256:<hex>} of the canonical input value. */
  String inputHash();

  /** The provider response, available on both paths once the model was called. */
  LlmResponse response();

  /** The record was enriched; {@code envelope} is ready to publish. */
  record Enriched(String inputHash, ObjectNode envelope, LlmResponse response)
      implements EnrichmentOutcome {}

  /** The model answered, but the answer is unusable. */
  record Failed(String inputHash, FailureReason reason, String detail, LlmResponse response)
      implements EnrichmentOutcome {}
}
