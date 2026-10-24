package io.github.xeeshan16.enricher.cost;

import io.github.xeeshan16.enricher.llm.TokenUsage;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.Objects;

/**
 * Model prices, used to turn token usage into cost.
 *
 * <p>Prices are in USD per million tokens, the unit providers publish. Cost is kept in micro-USD
 * ({@code 1_000_000} = one dollar) as a {@code long}, which is exact for addition and comparison;
 * with prices per million tokens, the cost of one token in micro-USD equals its price, so {@code
 * costMicros = inputTokens * inputPrice + outputTokens * outputPrice}, rounded up.
 *
 * <p>Models are looked up by the name configured on the pipeline (the requested model), not the
 * versioned name a provider may report back, so the configuration decides what a call costs. A
 * model without a price costs zero; {@link #knows} lets callers warn about that.
 */
public final class PriceTable {

  /**
   * @param inputPerMillion USD per million input tokens
   * @param outputPerMillion USD per million output tokens
   */
  public record Price(BigDecimal inputPerMillion, BigDecimal outputPerMillion) {
    public static final Price FREE = new Price(BigDecimal.ZERO, BigDecimal.ZERO);

    public Price {
      Objects.requireNonNull(inputPerMillion, "inputPerMillion");
      Objects.requireNonNull(outputPerMillion, "outputPerMillion");
      if (inputPerMillion.signum() < 0 || outputPerMillion.signum() < 0) {
        throw new IllegalArgumentException("prices must not be negative");
      }
    }

    public static Price of(String inputPerMillion, String outputPerMillion) {
      return new Price(new BigDecimal(inputPerMillion), new BigDecimal(outputPerMillion));
    }
  }

  private static final PriceTable EMPTY = new PriceTable(Map.of());

  private final Map<String, Price> prices;

  public PriceTable(Map<String, Price> prices) {
    this.prices = Map.copyOf(prices);
  }

  public static PriceTable empty() {
    return EMPTY;
  }

  public boolean knows(String model) {
    return prices.containsKey(model);
  }

  public Price price(String model) {
    return prices.getOrDefault(model, Price.FREE);
  }

  public Map<String, Price> prices() {
    return prices;
  }

  /** Cost of {@code usage} on {@code model}, in micro-USD, rounded up. */
  public long costMicros(String model, TokenUsage usage) {
    Price price = price(model);
    BigDecimal micros =
        price
            .inputPerMillion()
            .multiply(BigDecimal.valueOf(usage.inputTokens()))
            .add(price.outputPerMillion().multiply(BigDecimal.valueOf(usage.outputTokens())));
    return micros.setScale(0, RoundingMode.CEILING).longValueExact();
  }

  /** Micro-USD to USD, for display: {@code 1234} becomes {@code 0.001234}. */
  public static BigDecimal toUsd(long micros) {
    return BigDecimal.valueOf(micros, 6);
  }

  /** USD to micro-USD, rounded up. */
  public static long toMicros(BigDecimal usd) {
    return usd.movePointRight(6).setScale(0, RoundingMode.CEILING).longValueExact();
  }
}
