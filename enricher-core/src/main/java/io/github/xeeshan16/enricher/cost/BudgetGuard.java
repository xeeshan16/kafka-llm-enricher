package io.github.xeeshan16.enricher.cost;

import io.github.xeeshan16.enricher.llm.TokenUsage;

/**
 * Admits provider calls against per-tenant budgets. The engine reserves before a call that is
 * actually going to be paid for (never for a result-ledger hit), then settles or refunds.
 */
public interface BudgetGuard {

  /** Reserves the worst-case {@code estimate} of a call to {@code model} for {@code tenant}. */
  BudgetDecision reserve(String pipeline, String tenant, String model, TokenUsage estimate);

  /** Replaces the reserved estimate with the usage the provider reported. */
  void settle(BudgetDecision.Granted grant, TokenUsage actual);

  /** Returns a reservation whose call was never answered (it failed or was not sent). */
  void refund(BudgetDecision.Granted grant);
}
