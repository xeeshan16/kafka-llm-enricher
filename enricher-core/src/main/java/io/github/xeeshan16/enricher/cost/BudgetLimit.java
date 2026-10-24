package io.github.xeeshan16.enricher.cost;

/**
 * Caps for one tenant and one budget window. A negative value means no cap on that dimension; zero
 * means nothing may be spent (useful to cut a tenant off).
 *
 * @param maxTokens input plus output tokens
 * @param maxCostMicros cost in micro-USD
 */
public record BudgetLimit(long maxTokens, long maxCostMicros) {

  public static final BudgetLimit UNLIMITED = new BudgetLimit(-1, -1);

  public boolean unlimited() {
    return maxTokens < 0 && maxCostMicros < 0;
  }

  /** Whether usage of {@code tokens} and {@code costMicros} fits under the caps. */
  public boolean allows(long tokens, long costMicros) {
    return (maxTokens < 0 || tokens <= maxTokens)
        && (maxCostMicros < 0 || costMicros <= maxCostMicros);
  }
}
