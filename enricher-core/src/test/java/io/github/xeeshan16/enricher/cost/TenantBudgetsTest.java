package io.github.xeeshan16.enricher.cost;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.xeeshan16.enricher.MutableClock;
import io.github.xeeshan16.enricher.llm.TokenUsage;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TenantBudgetsTest {

  private final MutableClock clock = new MutableClock(Instant.parse("2026-10-24T10:00:00Z"));
  private final InMemoryBudgetStore store = new InMemoryBudgetStore();
  private final PriceTable prices = new PriceTable(Map.of("big", PriceTable.Price.of("2", "8")));

  private TenantBudgets budgets(BudgetLimit defaults, Map<String, BudgetLimit> tenants) {
    return new TenantBudgets(store, prices, BudgetPeriod.DAILY, defaults, tenants, clock);
  }

  @Test
  void reservesTheEstimateAndSettlesToTheRealUsage() {
    TenantBudgets budgets = budgets(new BudgetLimit(10_000, -1), Map.of());

    BudgetDecision.Granted grant =
        (BudgetDecision.Granted) budgets.reserve("p", "acme", "big", new TokenUsage(300, 500));
    assertThat(budgets.usage("p", "acme")).isEqualTo(new BudgetUsage(800, 600 + 4000, 1));

    budgets.settle(grant, new TokenUsage(300, 40));
    assertThat(budgets.usage("p", "acme")).isEqualTo(new BudgetUsage(340, 600 + 320, 1));
  }

  @Test
  void deniesWhenTheEstimateDoesNotFitAndSaysWhenItResets() {
    TenantBudgets budgets = budgets(new BudgetLimit(1000, -1), Map.of());
    budgets.reserve("p", "acme", "big", new TokenUsage(400, 400));

    BudgetDecision decision = budgets.reserve("p", "acme", "big", new TokenUsage(100, 200));

    assertThat(decision).isInstanceOf(BudgetDecision.Denied.class);
    BudgetDecision.Denied denied = (BudgetDecision.Denied) decision;
    assertThat(denied.resetsAt()).isEqualTo(Instant.parse("2026-10-25T00:00:00Z"));
    assertThat(denied.usage().tokens()).isEqualTo(800);
    assertThat(denied.detail()).contains("acme").contains("1000 tokens");
  }

  @Test
  void costCapsApplyInMicroUsd() {
    // 1 USD cap; each call reserves 100 * 2 + 100_000 * 8 = 800_200 micro-USD.
    TenantBudgets budgets = budgets(new BudgetLimit(-1, 1_000_000), Map.of());
    assertThat(budgets.reserve("p", "t", "big", new TokenUsage(100, 100_000)))
        .isInstanceOf(BudgetDecision.Granted.class);
    assertThat(budgets.reserve("p", "t", "big", new TokenUsage(100, 100_000)))
        .isInstanceOf(BudgetDecision.Denied.class);
  }

  @Test
  void perTenantOverridesAndZeroCutsATenantOff() {
    TenantBudgets budgets =
        budgets(
            new BudgetLimit(100, -1),
            Map.of("vip", BudgetLimit.UNLIMITED, "blocked", new BudgetLimit(0, -1)));

    assertThat(budgets.reserve("p", "vip", "big", new TokenUsage(1_000_000, 0)))
        .isInstanceOf(BudgetDecision.Granted.class);
    assertThat(budgets.reserve("p", "blocked", "big", new TokenUsage(1, 0)))
        .isInstanceOf(BudgetDecision.Denied.class);
    assertThat(budgets.reserve("p", "other", "big", new TokenUsage(101, 0)))
        .isInstanceOf(BudgetDecision.Denied.class);
  }

  @Test
  void refundReturnsTheReservationAndTheCall() {
    TenantBudgets budgets = budgets(new BudgetLimit(1000, -1), Map.of());
    BudgetDecision.Granted grant =
        (BudgetDecision.Granted) budgets.reserve("p", "acme", "big", new TokenUsage(500, 500));

    budgets.refund(grant);

    assertThat(budgets.usage("p", "acme")).isEqualTo(BudgetUsage.ZERO);
    assertThat(budgets.reserve("p", "acme", "big", new TokenUsage(500, 500)))
        .isInstanceOf(BudgetDecision.Granted.class);
  }

  @Test
  void windowsResetAndPipelinesAreSeparate() {
    TenantBudgets budgets = budgets(new BudgetLimit(100, -1), Map.of());
    budgets.reserve("p", "acme", "big", new TokenUsage(100, 0));
    assertThat(budgets.reserve("p", "acme", "big", new TokenUsage(1, 0)))
        .isInstanceOf(BudgetDecision.Denied.class);
    assertThat(budgets.reserve("q", "acme", "big", new TokenUsage(100, 0)))
        .isInstanceOf(BudgetDecision.Granted.class);

    clock.advance(Duration.ofDays(1));
    assertThat(budgets.reserve("p", "acme", "big", new TokenUsage(100, 0)))
        .isInstanceOf(BudgetDecision.Granted.class);
  }

  @Test
  void concurrentReservationsNeverOverspend() throws Exception {
    TenantBudgets budgets = budgets(new BudgetLimit(10_000, -1), Map.of());
    AtomicInteger granted = new AtomicInteger();
    try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < 500; i++) {
        futures.add(
            pool.submit(
                () -> {
                  if (budgets.reserve("p", "acme", "big", new TokenUsage(60, 40))
                      instanceof BudgetDecision.Granted) {
                    granted.incrementAndGet();
                  }
                }));
      }
      for (Future<?> f : futures) {
        f.get(30, TimeUnit.SECONDS);
      }
    }
    assertThat(granted).hasValue(100);
    assertThat(budgets.usage("p", "acme").tokens()).isEqualTo(10_000);
  }
}
