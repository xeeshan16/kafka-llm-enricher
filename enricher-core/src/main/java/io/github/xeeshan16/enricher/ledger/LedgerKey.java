package io.github.xeeshan16.enricher.ledger;

import java.util.Objects;

/**
 * Identity of one paid enrichment. Two records with the same key get the same answer, so the second
 * one never needs a provider call.
 *
 * <p>The key includes the requested model and the prompt version: changing either one deliberately
 * starts a new set of results, while re-deliveries, rebalances, retries and replays of the same
 * event keep the same key.
 *
 * @param pipeline pipeline name
 * @param promptVersion prompt and schema version of the pipeline
 * @param model the model requested from the provider
 * @param inputHash {@code sha256:<hex>} of the canonical input
 */
public record LedgerKey(String pipeline, String promptVersion, String model, String inputHash) {

  public LedgerKey {
    Objects.requireNonNull(pipeline, "pipeline");
    Objects.requireNonNull(promptVersion, "promptVersion");
    Objects.requireNonNull(model, "model");
    Objects.requireNonNull(inputHash, "inputHash");
  }

  @Override
  public String toString() {
    return pipeline + "/" + promptVersion + "/" + model + "/" + inputHash;
  }
}
