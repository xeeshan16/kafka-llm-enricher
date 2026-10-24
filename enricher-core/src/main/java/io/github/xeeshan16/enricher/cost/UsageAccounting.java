package io.github.xeeshan16.enricher.cost;

import io.github.xeeshan16.enricher.llm.TokenUsage;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Token and cost totals per pipeline, tenant and model since the process started, including what
 * the result ledger and skip rules saved. Cheap enough to call on every record (lock-free adders).
 *
 * <p>Cost saved by a ledger hit is the price of the stored answer's usage: what a naive consumer
 * would have paid to ask the model again.
 */
public final class UsageAccounting implements UsageMeter {

  /**
   * Totals for one pipeline, tenant and model ({@code model} is empty for skips and deferrals,
   * which happen before a model is chosen).
   */
  public record Line(
      String pipeline,
      String tenant,
      String model,
      long calls,
      long inputTokens,
      long outputTokens,
      long costMicros,
      long ledgerHits,
      long savedTokens,
      long savedCostMicros,
      long skipped,
      long deferred,
      long downgraded) {}

  private record Key(String pipeline, String tenant, String model) {}

  private static final class Counters {
    final LongAdder calls = new LongAdder();
    final LongAdder inputTokens = new LongAdder();
    final LongAdder outputTokens = new LongAdder();
    final LongAdder costMicros = new LongAdder();
    final LongAdder ledgerHits = new LongAdder();
    final LongAdder savedTokens = new LongAdder();
    final LongAdder savedCostMicros = new LongAdder();
    final LongAdder skipped = new LongAdder();
    final LongAdder deferred = new LongAdder();
    final LongAdder downgraded = new LongAdder();
  }

  private final PriceTable prices;
  private final Map<Key, Counters> counters = new ConcurrentHashMap<>();
  private final Map<String, LongAdder> skipReasons = new ConcurrentHashMap<>();

  public UsageAccounting(PriceTable prices) {
    this.prices = Objects.requireNonNull(prices, "prices");
  }

  public PriceTable prices() {
    return prices;
  }

  @Override
  public void providerCall(String pipeline, String tenant, String model, TokenUsage usage) {
    Counters c = counters(pipeline, tenant, model);
    c.calls.increment();
    c.inputTokens.add(usage.inputTokens());
    c.outputTokens.add(usage.outputTokens());
    c.costMicros.add(prices.costMicros(model, usage));
  }

  @Override
  public void ledgerHit(String pipeline, String tenant, String model, TokenUsage usage) {
    Counters c = counters(pipeline, tenant, model);
    c.ledgerHits.increment();
    c.savedTokens.add(usage.totalTokens());
    c.savedCostMicros.add(prices.costMicros(model, usage));
  }

  @Override
  public void skipped(String pipeline, String tenant, String reason) {
    counters(pipeline, tenant, "").skipped.increment();
    skipReasons.computeIfAbsent(pipeline + "\u0000" + reason, k -> new LongAdder()).increment();
  }

  @Override
  public void deferred(String pipeline, String tenant) {
    counters(pipeline, tenant, "").deferred.increment();
  }

  @Override
  public void downgraded(String pipeline, String tenant, String fromModel, String toModel) {
    counters(pipeline, tenant, toModel).downgraded.increment();
  }

  /** Every line, sorted by pipeline, tenant and model. */
  public List<Line> snapshot() {
    List<Line> lines = new ArrayList<>();
    counters.forEach(
        (k, c) ->
            lines.add(
                new Line(
                    k.pipeline(),
                    k.tenant(),
                    k.model(),
                    c.calls.sum(),
                    c.inputTokens.sum(),
                    c.outputTokens.sum(),
                    c.costMicros.sum(),
                    c.ledgerHits.sum(),
                    c.savedTokens.sum(),
                    c.savedCostMicros.sum(),
                    c.skipped.sum(),
                    c.deferred.sum(),
                    c.downgraded.sum())));
    lines.sort(
        Comparator.comparing(Line::pipeline)
            .thenComparing(Line::tenant)
            .thenComparing(Line::model));
    return lines;
  }

  /** All lines of {@code pipeline} added together, with tenant and model set to {@code *}. */
  public Line total(String pipeline) {
    long[] t = new long[10];
    for (Line l : snapshot()) {
      if (!l.pipeline().equals(pipeline)) {
        continue;
      }
      t[0] += l.calls();
      t[1] += l.inputTokens();
      t[2] += l.outputTokens();
      t[3] += l.costMicros();
      t[4] += l.ledgerHits();
      t[5] += l.savedTokens();
      t[6] += l.savedCostMicros();
      t[7] += l.skipped();
      t[8] += l.deferred();
      t[9] += l.downgraded();
    }
    return new Line(
        pipeline, "*", "*", t[0], t[1], t[2], t[3], t[4], t[5], t[6], t[7], t[8], t[9]);
  }

  /** Skips of {@code pipeline} by reason. */
  public Map<String, Long> skipReasons(String pipeline) {
    Map<String, Long> out = new java.util.TreeMap<>();
    String prefix = pipeline + "\u0000";
    skipReasons.forEach(
        (k, v) -> {
          if (k.startsWith(prefix)) {
            out.put(k.substring(prefix.length()), v.sum());
          }
        });
    return out;
  }

  private Counters counters(String pipeline, String tenant, String model) {
    return counters.computeIfAbsent(new Key(pipeline, tenant, model), k -> new Counters());
  }
}
