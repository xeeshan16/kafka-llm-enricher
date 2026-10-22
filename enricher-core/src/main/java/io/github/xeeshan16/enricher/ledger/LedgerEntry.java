package io.github.xeeshan16.enricher.ledger;

import io.github.xeeshan16.enricher.llm.TokenUsage;
import java.util.Objects;

/**
 * A completed, validated enrichment as stored in the ledger.
 *
 * @param enrichmentJson canonical JSON of the validated answer
 * @param responseModel the model that actually answered
 * @param usage tokens spent producing it
 */
public record LedgerEntry(String enrichmentJson, String responseModel, TokenUsage usage) {

  public LedgerEntry {
    Objects.requireNonNull(enrichmentJson, "enrichmentJson");
    Objects.requireNonNull(responseModel, "responseModel");
    Objects.requireNonNull(usage, "usage");
  }
}
