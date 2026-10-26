package io.github.xeeshan16.enricher.cost;

import io.github.xeeshan16.enricher.llm.TokenUsage;
import java.util.List;

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

  /**
   * A repair prompt was sent for an unusable answer; {@code fixed} tells whether the repaired
   * answer was valid. The repair call itself is also reported through {@link #providerCall}.
   */
  default void repairAttempt(String pipeline, String tenant, String model, boolean fixed) {}

  /** Personal data was masked in a prompt: {@code count} distinct values of type {@code label}. */
  default void piiMasked(String pipeline, String label, int count) {}

  /**
   * One meter that forwards every event to each of {@code meters} in order, for example in-memory
   * accounting and a Micrometer exporter. Returns the only meter as is, or {@link #NOOP} for none.
   */
  static UsageMeter composite(List<? extends UsageMeter> meters) {
    List<UsageMeter> all = List.copyOf(meters);
    if (all.isEmpty()) {
      return NOOP;
    }
    if (all.size() == 1) {
      return all.get(0);
    }
    return new UsageMeter() {
      @Override
      public void providerCall(String pipeline, String tenant, String model, TokenUsage usage) {
        all.forEach(m -> m.providerCall(pipeline, tenant, model, usage));
      }

      @Override
      public void ledgerHit(String pipeline, String tenant, String model, TokenUsage usage) {
        all.forEach(m -> m.ledgerHit(pipeline, tenant, model, usage));
      }

      @Override
      public void skipped(String pipeline, String tenant, String reason) {
        all.forEach(m -> m.skipped(pipeline, tenant, reason));
      }

      @Override
      public void deferred(String pipeline, String tenant) {
        all.forEach(m -> m.deferred(pipeline, tenant));
      }

      @Override
      public void downgraded(String pipeline, String tenant, String fromModel, String toModel) {
        all.forEach(m -> m.downgraded(pipeline, tenant, fromModel, toModel));
      }

      @Override
      public void repairAttempt(String pipeline, String tenant, String model, boolean fixed) {
        all.forEach(m -> m.repairAttempt(pipeline, tenant, model, fixed));
      }

      @Override
      public void piiMasked(String pipeline, String label, int count) {
        all.forEach(m -> m.piiMasked(pipeline, label, count));
      }
    };
  }

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
