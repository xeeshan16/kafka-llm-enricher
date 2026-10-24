package io.github.xeeshan16.enricher.cost;

/**
 * Where budget counters live. {@link InMemoryBudgetStore} counts per process; the PostgreSQL store
 * in {@code enricher-ledger-postgres} shares counters between every instance.
 *
 * <p>Implementations must make {@link #tryCharge} atomic: two workers charging the same key at the
 * same time must never both succeed when only one fits.
 */
public interface BudgetStore {

  /**
   * Adds {@code tokens}, {@code costMicros} and one call to {@code key} if the result stays within
   * {@code limit}.
   *
   * @return whether the charge was applied
   */
  boolean tryCharge(BudgetKey key, long tokens, long costMicros, BudgetLimit limit);

  /**
   * Adds the given deltas unconditionally (they may be negative, to settle an over-estimate or
   * refund a call that never happened). Counters never go below zero.
   */
  void adjust(BudgetKey key, long tokens, long costMicros, long calls);

  /** Current counters of {@code key}; {@link BudgetUsage#ZERO} when nothing was charged. */
  BudgetUsage usage(BudgetKey key);
}
