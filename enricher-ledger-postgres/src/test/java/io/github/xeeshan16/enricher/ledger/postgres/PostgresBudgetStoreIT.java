package io.github.xeeshan16.enricher.ledger.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.xeeshan16.enricher.cost.BudgetDecision;
import io.github.xeeshan16.enricher.cost.BudgetKey;
import io.github.xeeshan16.enricher.cost.BudgetLimit;
import io.github.xeeshan16.enricher.cost.BudgetPeriod;
import io.github.xeeshan16.enricher.cost.BudgetUsage;
import io.github.xeeshan16.enricher.cost.PriceTable;
import io.github.xeeshan16.enricher.cost.TenantBudgets;
import io.github.xeeshan16.enricher.llm.TokenUsage;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class PostgresBudgetStoreIT {

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

  private static final Instant WINDOW = Instant.parse("2026-10-24T00:00:00Z");

  private static PGSimpleDataSource dataSource;

  private final PostgresBudgetStore store = new PostgresBudgetStore(dataSource);

  @BeforeAll
  static void migrate() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(POSTGRES.getJdbcUrl());
    dataSource.setUser(POSTGRES.getUsername());
    dataSource.setPassword(POSTGRES.getPassword());
    LedgerSchema.migrate(dataSource);
  }

  private static BudgetKey newKey() {
    return new BudgetKey("triage", "tenant-" + UUID.randomUUID(), WINDOW);
  }

  @Test
  void chargesUpToTheCapAndNoFurther() {
    BudgetKey key = newKey();
    BudgetLimit limit = new BudgetLimit(1000, 5000);

    assertThat(store.tryCharge(key, 600, 1000, limit)).isTrue();
    assertThat(store.tryCharge(key, 500, 1000, limit)).isFalse();
    assertThat(store.tryCharge(key, 400, 1000, limit)).isTrue();
    assertThat(store.tryCharge(key, 0, 3001, limit)).isFalse();

    assertThat(store.usage(key)).isEqualTo(new BudgetUsage(1000, 2000, 2));
  }

  @Test
  void firstChargeLargerThanTheCapIsRefused() {
    BudgetKey key = newKey();
    assertThat(store.tryCharge(key, 2000, 0, new BudgetLimit(1000, -1))).isFalse();
    assertThat(store.usage(key)).isEqualTo(BudgetUsage.ZERO);
  }

  @Test
  void adjustSettlesAndNeverGoesNegative() {
    BudgetKey key = newKey();
    store.tryCharge(key, 800, 900, BudgetLimit.UNLIMITED);

    store.adjust(key, -500, -100, 0);
    assertThat(store.usage(key)).isEqualTo(new BudgetUsage(300, 800, 1));

    store.adjust(key, -10_000, -10_000, -5);
    assertThat(store.usage(key)).isEqualTo(BudgetUsage.ZERO);

    BudgetKey fresh = newKey();
    store.adjust(fresh, 50, 60, 0);
    assertThat(store.usage(fresh)).isEqualTo(new BudgetUsage(50, 60, 0));
  }

  @Test
  void concurrentInstancesNeverJointlyOverspend() throws Exception {
    BudgetKey key = newKey();
    BudgetLimit limit = new BudgetLimit(10_000, -1);
    int workers = 16;
    int perWorker = 40;
    CountDownLatch start = new CountDownLatch(1);
    List<Future<Integer>> results = new ArrayList<>();
    try (ExecutorService pool = Executors.newFixedThreadPool(workers)) {
      for (int w = 0; w < workers; w++) {
        PostgresBudgetStore instance = new PostgresBudgetStore(dataSource);
        results.add(
            pool.submit(
                () -> {
                  start.await();
                  int granted = 0;
                  for (int i = 0; i < perWorker; i++) {
                    if (instance.tryCharge(key, 100, 0, limit)) {
                      granted++;
                    }
                  }
                  return granted;
                }));
      }
      start.countDown();
      int granted = 0;
      for (Future<Integer> f : results) {
        granted += f.get();
      }
      System.out.println(
          "budget: workers="
              + workers
              + " attempts="
              + workers * perWorker
              + " granted="
              + granted
              + " tokens="
              + store.usage(key).tokens()
              + " cap="
              + limit.maxTokens());
      assertThat(granted).isEqualTo(100);
    }
    assertThat(store.usage(key)).isEqualTo(new BudgetUsage(10_000, 0, 100));
  }

  @Test
  void tenantBudgetsOnPostgresDenyAcrossInstances() {
    Clock clock = Clock.fixed(Instant.parse("2026-10-24T12:00:00Z"), ZoneOffset.UTC);
    PriceTable prices = new PriceTable(Map.of("m", PriceTable.Price.of("1", "2")));
    String tenant = "acme-" + UUID.randomUUID();
    TenantBudgets a =
        new TenantBudgets(
            new PostgresBudgetStore(dataSource),
            prices,
            BudgetPeriod.DAILY,
            new BudgetLimit(-1, 1000),
            Map.of(),
            clock);
    TenantBudgets b =
        new TenantBudgets(
            new PostgresBudgetStore(dataSource),
            prices,
            BudgetPeriod.DAILY,
            new BudgetLimit(-1, 1000),
            Map.of(),
            clock);

    // 100 * 1 + 300 * 2 = 700 micro-USD reserved by instance a.
    BudgetDecision first = a.reserve("triage", tenant, "m", new TokenUsage(100, 300));
    BudgetDecision second = b.reserve("triage", tenant, "m", new TokenUsage(100, 300));
    assertThat(first).isInstanceOf(BudgetDecision.Granted.class);
    assertThat(second).isInstanceOf(BudgetDecision.Denied.class);

    // Settling a's call to what it really used frees room for b.
    a.settle((BudgetDecision.Granted) first, new TokenUsage(100, 50));
    assertThat(b.reserve("triage", tenant, "m", new TokenUsage(100, 300)))
        .isInstanceOf(BudgetDecision.Granted.class);
    assertThat(b.usage("triage", tenant).costMicros()).isEqualTo(200 + 700);
  }

  @Test
  void purgeDropsOldWindows() {
    BudgetKey old = new BudgetKey("triage", "t-" + UUID.randomUUID(), Instant.parse("2020-01-01T00:00:00Z"));
    store.adjust(old, 1, 1, 1);
    assertThat(store.purgeWindowsBefore(Instant.parse("2021-01-01T00:00:00Z"))).isPositive();
    assertThat(store.usage(old)).isEqualTo(BudgetUsage.ZERO);
  }
}
