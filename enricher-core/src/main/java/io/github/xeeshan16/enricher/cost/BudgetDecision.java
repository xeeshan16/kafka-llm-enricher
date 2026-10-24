package io.github.xeeshan16.enricher.cost;

import java.time.Instant;

/** The answer to a budget reservation. */
public sealed interface BudgetDecision {

  /**
   * The estimate was charged; settle it with the real usage once the call returns, or refund it if
   * the call never happened.
   *
   * @param model the model the reservation was priced for
   * @param tokens tokens reserved
   * @param costMicros micro-USD reserved
   */
  record Granted(BudgetKey key, String model, long tokens, long costMicros)
      implements BudgetDecision {}

  /**
   * The estimate does not fit.
   *
   * @param usage spend so far in the window
   * @param limit the tenant's caps
   * @param resetsAt when the window ends and the budget starts again
   */
  record Denied(BudgetKey key, BudgetUsage usage, BudgetLimit limit, Instant resetsAt)
      implements BudgetDecision {

    /** A short explanation for logs and failure headers. */
    public String detail() {
      return "tenant '"
          + key.tenant()
          + "' is over its budget for the window starting "
          + key.periodStart()
          + " (used "
          + usage.tokens()
          + " tokens / "
          + PriceTable.toUsd(usage.costMicros()).toPlainString()
          + " USD; caps "
          + (limit.maxTokens() < 0 ? "none" : Long.toString(limit.maxTokens()))
          + " tokens / "
          + (limit.maxCostMicros() < 0
              ? "none"
              : PriceTable.toUsd(limit.maxCostMicros()).toPlainString() + " USD")
          + "); resets at "
          + resetsAt;
    }
  }
}
