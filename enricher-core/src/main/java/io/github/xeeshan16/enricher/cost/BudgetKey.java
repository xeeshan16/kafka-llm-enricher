package io.github.xeeshan16.enricher.cost;

import java.time.Instant;
import java.util.Objects;

/**
 * One budget counter: a pipeline's spend for one tenant in one window.
 *
 * @param periodStart start of the window, from {@link BudgetPeriod#start}
 */
public record BudgetKey(String pipeline, String tenant, Instant periodStart) {
  public BudgetKey {
    Objects.requireNonNull(pipeline, "pipeline");
    Objects.requireNonNull(tenant, "tenant");
    Objects.requireNonNull(periodStart, "periodStart");
  }
}
