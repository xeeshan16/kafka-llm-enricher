package io.github.xeeshan16.enricher.cost;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link BudgetStore} in process memory. Counters are lost on restart and not shared between
 * instances, so N replicas together may spend up to N times a tenant's budget. Fine for tests, for
 * a single instance, or as a soft guard; use the PostgreSQL store for a hard cap.
 */
public final class InMemoryBudgetStore implements BudgetStore {

  private final Map<BudgetKey, BudgetUsage> usage = new ConcurrentHashMap<>();

  @Override
  public boolean tryCharge(BudgetKey key, long tokens, long costMicros, BudgetLimit limit) {
    boolean[] charged = {false};
    usage.compute(
        key,
        (k, current) -> {
          BudgetUsage now = current == null ? BudgetUsage.ZERO : current;
          if (!limit.allows(now.tokens() + tokens, now.costMicros() + costMicros)) {
            return current;
          }
          charged[0] = true;
          return now.plus(tokens, costMicros, 1);
        });
    return charged[0];
  }

  @Override
  public void adjust(BudgetKey key, long tokens, long costMicros, long calls) {
    usage.merge(
        key,
        BudgetUsage.ZERO.plus(tokens, costMicros, calls),
        (current, ignored) -> current.plus(tokens, costMicros, calls));
  }

  @Override
  public BudgetUsage usage(BudgetKey key) {
    return usage.getOrDefault(key, BudgetUsage.ZERO);
  }
}
