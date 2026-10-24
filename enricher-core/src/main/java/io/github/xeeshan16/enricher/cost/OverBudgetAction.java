package io.github.xeeshan16.enricher.cost;

/** What happens to a record whose tenant has no budget left. */
public enum OverBudgetAction {
  /**
   * Park the record on {@code <input-topic>.enricher-deferred} with a not-before time at the start
   * of the next budget window. The pipeline consumes that topic and enriches the record then.
   */
  DEFER,
  /** Write a skipped envelope (no enrichment, reason {@code over-budget}) and move on. */
  SKIP,
  /**
   * Retry with the pipeline's cheaper fallback model; if that does not fit either, apply the
   * pipeline's after-downgrade action ({@link #DEFER} or {@link #SKIP}).
   */
  DOWNGRADE
}
