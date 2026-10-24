package io.github.xeeshan16.enricher.cost;

/**
 * Spend recorded against a {@link BudgetKey}: reserved estimates plus settled corrections.
 *
 * @param tokens input plus output tokens
 * @param costMicros cost in micro-USD
 * @param calls provider calls charged
 */
public record BudgetUsage(long tokens, long costMicros, long calls) {
  public static final BudgetUsage ZERO = new BudgetUsage(0, 0, 0);

  public BudgetUsage plus(long tokens, long costMicros, long calls) {
    return new BudgetUsage(
        Math.max(0, this.tokens + tokens),
        Math.max(0, this.costMicros + costMicros),
        Math.max(0, this.calls + calls));
  }
}
