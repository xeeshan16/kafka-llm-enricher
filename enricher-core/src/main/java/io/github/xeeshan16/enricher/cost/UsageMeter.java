package io.github.xeeshan16.enricher.cost;

import io.github.xeeshan16.enricher.llm.TokenUsage;

/**
 * Receives the engine's spend events. {@link UsageAccounting} keeps totals in memory; metrics
 * exporters (Micrometer) subscribe the same way. Implementations must be thread-safe and fast: they
 * run on the record's worker thread.
 *
 * <p>{@code model} is the model the pipeline asked for (after any downgrade), which is also what
 * prices are looked up by.
 */
public interface UsageMeter {

  /** A provider call was paid for. */
  void providerCall(String pipeline, String tenant, String model, TokenUsage usage);

  /** A record was answered from the result ledger; {@code usage} is what the stored answer cost. */
  void ledgerHit(String pipeline, String tenant, String model, TokenUsage usage);

  /** A record was not sent to the model; {@code reason} is {@code rule:<name>} or over-budget. */
  void skipped(String pipeline, String tenant, String reason);

  /** A record was parked until its tenant's budget resets. */
  void deferred(String pipeline, String tenant);

  /** A record was routed to the fallback model because its tenant was over budget. */
  void downgraded(String pipeline, String tenant, String fromModel, String toModel);

  UsageMeter NOOP =
      new UsageMeter() {
        @Override
        public void providerCall(String pipeline, String tenant, String model, TokenUsage usage) {}

        @Override
        public void ledgerHit(String pipeline, String tenant, String model, TokenUsage usage) {}

        @Override
        public void skipped(String pipeline, String tenant, String reason) {}

        @Override
        public void deferred(String pipeline, String tenant) {}

        @Override
        public void downgraded(String pipeline, String tenant, String fromModel, String toModel) {}
      };
}
