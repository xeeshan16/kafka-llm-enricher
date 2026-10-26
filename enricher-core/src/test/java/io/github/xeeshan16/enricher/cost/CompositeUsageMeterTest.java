package io.github.xeeshan16.enricher.cost;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.xeeshan16.enricher.llm.TokenUsage;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CompositeUsageMeterTest {

  @Test
  void forwardsEveryEventToEveryMeter() {
    PriceTable prices =
        new PriceTable(Map.of("m", new PriceTable.Price(BigDecimal.ONE, BigDecimal.ONE)));
    UsageAccounting a = new UsageAccounting(prices);
    UsageAccounting b = new UsageAccounting(prices);
    UsageMeter both = UsageMeter.composite(List.of(a, b));

    both.providerCall("p", "t", "m", new TokenUsage(10, 5));
    both.ledgerHit("p", "t", "m", new TokenUsage(10, 5));
    both.skipped("p", "t", "rule:tiny");
    both.deferred("p", "t");
    both.downgraded("p", "t", "m", "m-mini");
    both.repairAttempt("p", "t", "m", true);
    both.piiMasked("p", "PAN", 2);

    for (UsageAccounting meter : List.of(a, b)) {
      UsageAccounting.Line total = meter.total("p");
      assertThat(total.calls()).isEqualTo(1);
      assertThat(total.ledgerHits()).isEqualTo(1);
      assertThat(total.skipped()).isEqualTo(1);
      assertThat(total.deferred()).isEqualTo(1);
      assertThat(total.downgraded()).isEqualTo(1);
      assertThat(meter.quality("p").repaired()).isEqualTo(1);
      assertThat(meter.quality("p").masked()).containsEntry("PAN", 2L);
    }
  }

  @Test
  void collapsesTrivialCases() {
    UsageAccounting only = new UsageAccounting(new PriceTable(Map.of()));
    assertThat(UsageMeter.composite(List.of(only))).isSameAs(only);
    assertThat(UsageMeter.composite(List.of())).isSameAs(UsageMeter.NOOP);
  }
}
