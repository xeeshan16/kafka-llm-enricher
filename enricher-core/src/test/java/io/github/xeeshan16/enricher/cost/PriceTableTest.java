package io.github.xeeshan16.enricher.cost;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.xeeshan16.enricher.llm.TokenUsage;
import java.math.BigDecimal;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PriceTableTest {

  private final PriceTable prices =
      new PriceTable(Map.of("big", PriceTable.Price.of("2.50", "10.00"), "small", PriceTable.Price.of("0.15", "0.60")));

  @Test
  void costIsTokensTimesPricePerMillionInMicroUsd() {
    // 1000 * 2.50 + 500 * 10.00 = 7500 micro-USD = 0.0075 USD
    assertThat(prices.costMicros("big", new TokenUsage(1000, 500))).isEqualTo(7500);
    assertThat(PriceTable.toUsd(7500)).isEqualByComparingTo(new BigDecimal("0.0075"));
  }

  @Test
  void fractionalMicrosRoundUp() {
    // 3 * 0.15 + 1 * 0.60 = 1.05 micro-USD
    assertThat(prices.costMicros("small", new TokenUsage(3, 1))).isEqualTo(2);
  }

  @Test
  void unknownModelsAreFree() {
    assertThat(prices.knows("other")).isFalse();
    assertThat(prices.costMicros("other", new TokenUsage(1_000_000, 1_000_000))).isZero();
  }

  @Test
  void usdConvertsToMicros() {
    assertThat(PriceTable.toMicros(new BigDecimal("12.5"))).isEqualTo(12_500_000);
    assertThat(PriceTable.toMicros(new BigDecimal("0.0000001"))).isEqualTo(1);
  }
}
