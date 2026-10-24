package io.github.xeeshan16.enricher.cost;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.xeeshan16.enricher.llm.TokenUsage;
import java.util.Map;
import org.junit.jupiter.api.Test;

class UsageAccountingTest {

  @Test
  void totalsCallsCostAndSavingsPerPipeline() {
    UsageAccounting usage =
        new UsageAccounting(new PriceTable(Map.of("m", PriceTable.Price.of("1", "4"))));

    usage.providerCall("p", "a", "m", new TokenUsage(100, 10)); // 140 micro-USD
    usage.providerCall("p", "b", "m", new TokenUsage(200, 20)); // 280
    usage.ledgerHit("p", "a", "m", new TokenUsage(100, 10)); // saved 140
    usage.skipped("p", "a", "rule:small-refund");
    usage.skipped("p", "b", "rule:small-refund");
    usage.skipped("p", "b", "over-budget");
    usage.deferred("p", "b");
    usage.downgraded("p", "b", "m", "cheap");
    usage.providerCall("other", "a", "m", new TokenUsage(1, 1));

    UsageAccounting.Line total = usage.total("p");
    assertThat(total.calls()).isEqualTo(2);
    assertThat(total.inputTokens()).isEqualTo(300);
    assertThat(total.outputTokens()).isEqualTo(30);
    assertThat(total.costMicros()).isEqualTo(420);
    assertThat(total.ledgerHits()).isEqualTo(1);
    assertThat(total.savedCostMicros()).isEqualTo(140);
    assertThat(total.skipped()).isEqualTo(3);
    assertThat(total.deferred()).isEqualTo(1);
    assertThat(total.downgraded()).isEqualTo(1);
    assertThat(usage.skipReasons("p"))
        .containsExactly(Map.entry("over-budget", 1L), Map.entry("rule:small-refund", 2L));
    assertThat(usage.snapshot()).extracting(UsageAccounting.Line::tenant).contains("a", "b");
  }
}
