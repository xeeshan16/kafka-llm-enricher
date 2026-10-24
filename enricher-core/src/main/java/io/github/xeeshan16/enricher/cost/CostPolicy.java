package io.github.xeeshan16.enricher.cost;

import java.util.List;
import java.util.Objects;

/**
 * Per-pipeline cost controls applied by the engine before any money is spent.
 *
 * @param skipRules conditions under which a record is not sent to the model, checked in order
 * @param tenants who a record's spend is charged to
 * @param budgets the budget guard, or {@code null} for no budgets
 * @param onExceeded what to do when the tenant's budget is exhausted
 * @param fallbackModel the cheaper model for {@link OverBudgetAction#DOWNGRADE}, else {@code null}
 * @param afterDowngrade {@link OverBudgetAction#DEFER} or {@link OverBudgetAction#SKIP}, applied
 *     when even the fallback model does not fit
 */
public record CostPolicy(
    List<SkipRule> skipRules,
    TenantResolver tenants,
    BudgetGuard budgets,
    OverBudgetAction onExceeded,
    String fallbackModel,
    OverBudgetAction afterDowngrade) {

  private static final CostPolicy NONE =
      new CostPolicy(
          List.of(),
          TenantResolver.fixed(TenantResolver.DEFAULT_TENANT),
          null,
          OverBudgetAction.DEFER,
          null,
          OverBudgetAction.DEFER);

  public CostPolicy {
    skipRules = List.copyOf(skipRules);
    Objects.requireNonNull(tenants, "tenants");
    Objects.requireNonNull(onExceeded, "onExceeded");
    Objects.requireNonNull(afterDowngrade, "afterDowngrade");
    if (onExceeded == OverBudgetAction.DOWNGRADE
        && (fallbackModel == null || fallbackModel.isBlank())) {
      throw new IllegalArgumentException("on-exceeded=downgrade needs a fallback-model");
    }
    if (afterDowngrade == OverBudgetAction.DOWNGRADE) {
      throw new IllegalArgumentException("after-downgrade must be defer or skip");
    }
  }

  /** No skip rules, no budgets, every record charged to the default tenant. */
  public static CostPolicy none() {
    return NONE;
  }

  /** Whether over-budget records can end up on the deferred topic. */
  public boolean defers() {
    if (budgets == null) {
      return false;
    }
    return onExceeded == OverBudgetAction.DEFER
        || (onExceeded == OverBudgetAction.DOWNGRADE && afterDowngrade == OverBudgetAction.DEFER);
  }
}
