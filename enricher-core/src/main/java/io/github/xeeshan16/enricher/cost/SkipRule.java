package io.github.xeeshan16.enricher.cost;

import io.github.xeeshan16.enricher.engine.PreparedRecord;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * A named condition under which a record is not sent to the model at all: routine events that do
 * not need an LLM (a refund under a few dollars, a status ping, an event type the pipeline does not
 * handle). Skipping is the cheapest call there is.
 *
 * <p>The condition must be side-effect free and thread-safe. If it throws, the record is enriched
 * as usual: a broken rule should cost money, not silently drop work.
 *
 * @param name label written as {@code rule:<name>} into the skipped envelope and metrics
 */
public record SkipRule(String name, Predicate<PreparedRecord> condition) {
  public SkipRule {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(condition, "condition");
    if (name.isBlank()) {
      throw new IllegalArgumentException("skip rule name must not be blank");
    }
  }
}
