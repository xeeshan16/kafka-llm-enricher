package io.github.xeeshan16.enricher.cost;

import io.github.xeeshan16.enricher.llm.TokenUsage;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Token and cost budgets per tenant and window, on a {@link BudgetStore}.
 *
 * <p>A call reserves its worst case (prompt estimate plus the output cap) priced with the {@link
 * PriceTable}; the reservation succeeds only if it fits under the tenant's caps, atomically in the
 * store. After the call the difference to the real usage is settled. Reserving the worst case means
 * a tenant close to its cap is stopped slightly early rather than overshooting, and concurrent
 * workers can never jointly overspend.
 *
 * <p>A call whose answer later fails validation still costs money and stays charged; only a call
 * that produced no answer at all (a provider error, a timeout) is refunded.
 */
public final class TenantBudgets implements BudgetGuard {

  private final BudgetStore store;
  private final PriceTable prices;
  private final BudgetPeriod period;
  private final BudgetLimit defaultLimit;
  private final Map<String, BudgetLimit> tenantLimits;
  private final Clock clock;

  /**
   * @param defaultLimit caps for tenants without an entry in {@code tenantLimits}
   * @param tenantLimits caps per tenant id
   */
  public TenantBudgets(
      BudgetStore store,
      PriceTable prices,
      BudgetPeriod period,
      BudgetLimit defaultLimit,
      Map<String, BudgetLimit> tenantLimits,
      Clock clock) {
    this.store = Objects.requireNonNull(store, "store");
    this.prices = Objects.requireNonNull(prices, "prices");
    this.period = Objects.requireNonNull(period, "period");
    this.defaultLimit = Objects.requireNonNull(defaultLimit, "defaultLimit");
    this.tenantLimits = Map.copyOf(tenantLimits);
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  public BudgetPeriod period() {
    return period;
  }

  public BudgetLimit limitFor(String tenant) {
    return tenantLimits.getOrDefault(tenant, defaultLimit);
  }

  /** Spend of {@code tenant} on {@code pipeline} in the current window. */
  public BudgetUsage usage(String pipeline, String tenant) {
    return store.usage(new BudgetKey(pipeline, tenant, period.start(clock.instant())));
  }

  @Override
  public BudgetDecision reserve(String pipeline, String tenant, String model, TokenUsage estimate) {
    Instant now = clock.instant();
    BudgetKey key = new BudgetKey(pipeline, tenant, period.start(now));
    BudgetLimit limit = limitFor(tenant);
    long tokens = estimate.totalTokens();
    long cost = prices.costMicros(model, estimate);
    if (store.tryCharge(key, tokens, cost, limit)) {
      return new BudgetDecision.Granted(key, model, tokens, cost);
    }
    return new BudgetDecision.Denied(key, store.usage(key), limit, period.next(now));
  }

  @Override
  public void settle(BudgetDecision.Granted grant, TokenUsage actual) {
    long tokens = actual.totalTokens() - grant.tokens();
    long cost = prices.costMicros(grant.model(), actual) - grant.costMicros();
    if (tokens != 0 || cost != 0) {
      store.adjust(grant.key(), tokens, cost, 0);
    }
  }

  @Override
  public void refund(BudgetDecision.Granted grant) {
    store.adjust(grant.key(), -grant.tokens(), -grant.costMicros(), -1);
  }
}
