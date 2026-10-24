package io.github.xeeshan16.enricher.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.xeeshan16.enricher.MutableClock;
import io.github.xeeshan16.enricher.TestPipelines;
import io.github.xeeshan16.enricher.cost.BudgetLimit;
import io.github.xeeshan16.enricher.cost.BudgetPeriod;
import io.github.xeeshan16.enricher.cost.BudgetUsage;
import io.github.xeeshan16.enricher.cost.CostPolicy;
import io.github.xeeshan16.enricher.cost.InMemoryBudgetStore;
import io.github.xeeshan16.enricher.cost.OverBudgetAction;
import io.github.xeeshan16.enricher.cost.PriceTable;
import io.github.xeeshan16.enricher.cost.SkipRule;
import io.github.xeeshan16.enricher.cost.TenantBudgets;
import io.github.xeeshan16.enricher.cost.TenantResolver;
import io.github.xeeshan16.enricher.cost.UsageAccounting;
import io.github.xeeshan16.enricher.ledger.Claim;
import io.github.xeeshan16.enricher.ledger.InMemoryResultLedger;
import io.github.xeeshan16.enricher.llm.LlmProviderException;
import io.github.xeeshan16.enricher.llm.fake.FakeLlmProvider;
import io.github.xeeshan16.enricher.llm.fake.SchemaAwareResponder;
import io.github.xeeshan16.enricher.pipeline.InputRecord;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class LedgeredEnricherCostTest {

  private static final LedgeredEnricher.Settings SETTINGS =
      new LedgeredEnricher.Settings(
          Duration.ofSeconds(60), Duration.ofSeconds(2), Duration.ofMillis(100));

  private final PipelineDefinition pipeline = TestPipelines.disputeTriage(); // model fake-1
  private final MutableClock clock = new MutableClock(Instant.parse("2026-10-24T10:00:00Z"));
  private final InMemoryResultLedger ledger = new InMemoryResultLedger(clock, "test");
  private final PriceTable prices =
      new PriceTable(
          Map.of(
              "fake-1", PriceTable.Price.of("10", "40"),
              "cheap-1", PriceTable.Price.of("0.1", "0.4")));
  private final UsageAccounting usage = new UsageAccounting(prices);
  private final InMemoryBudgetStore store = new InMemoryBudgetStore();
  private final FakeLlmProvider provider = new FakeLlmProvider();

  private LedgeredEnricher enricher(FakeLlmProvider provider) {
    return new LedgeredEnricher(
        new Enricher(provider), ledger, SETTINGS, clock, clock::advance, usage);
  }

  private TenantBudgets budgets(BudgetLimit limit) {
    return new TenantBudgets(store, prices, BudgetPeriod.DAILY, limit, Map.of(), clock);
  }

  private CostPolicy policy(
      List<SkipRule> rules, TenantBudgets budgets, OverBudgetAction action, String fallback) {
    return new CostPolicy(
        rules,
        TenantResolver.jsonPointer("/merchant"),
        budgets,
        action,
        fallback,
        OverBudgetAction.SKIP);
  }

  private static InputRecord dispute(long offset, String merchant, double amount) {
    return new InputRecord(
        "payments.disputes",
        0,
        offset,
        "card-" + offset,
        "{\"disputeId\":\"D-"
            + offset
            + "\",\"merchant\":\""
            + merchant
            + "\",\"amount\":"
            + amount
            + ",\"note\":\"I was charged twice, please refund the duplicate charge.\"}");
  }

  private static final SkipRule SMALL_AMOUNT =
      new SkipRule("small-amount", r -> r.input().path("amount").asDouble() < 5);

  @Test
  void matchingSkipRuleNeverCallsTheModel() {
    LedgeredEnricher.Result result =
        enricher(provider)
            .process(
                pipeline,
                dispute(1, "Acme", 2.50),
                policy(List.of(SMALL_AMOUNT), null, OverBudgetAction.DEFER, null));

    assertThat(result.source()).isEqualTo(ResultSource.NONE);
    assertThat(result.tenant()).isEqualTo("Acme");
    EnrichmentOutcome.Skipped skipped = (EnrichmentOutcome.Skipped) result.outcome();
    assertThat(skipped.reason()).isEqualTo("rule:small-amount");
    assertThat(skipped.envelope().get("enrichment").isNull()).isTrue();
    assertThat(skipped.envelope().path("skipped").path("reason").asText())
        .isEqualTo("rule:small-amount");
    assertThat(skipped.envelope().path("input").path("amount").asDouble()).isEqualTo(2.5);
    assertThat(skipped.envelope().has("usage")).isFalse();
    assertThat(provider.calls()).isZero();
    assertThat(usage.skipReasons(pipeline.name())).containsEntry("rule:small-amount", 1L);
  }

  @Test
  void brokenSkipRuleEnrichesInsteadOfDropping() {
    SkipRule broken =
        new SkipRule(
            "broken",
            r -> {
              throw new IllegalStateException("bad rule");
            });
    LedgeredEnricher.Result result =
        enricher(provider)
            .process(
                pipeline,
                dispute(1, "Acme", 2.50),
                policy(List.of(broken), null, OverBudgetAction.DEFER, null));

    assertThat(result.outcome()).isInstanceOf(EnrichmentOutcome.Enriched.class);
    assertThat(provider.calls()).isOne();
  }

  @Test
  void overBudgetIsDeferredUntilTheWindowResetsAndReleasesTheLease() {
    LedgeredEnricher enricher = enricher(provider);
    CostPolicy policy =
        policy(List.of(), budgets(new BudgetLimit(0, -1)), OverBudgetAction.DEFER, null);

    LedgeredEnricher.Result result = enricher.process(pipeline, dispute(1, "Acme", 50), policy);

    EnrichmentOutcome.Deferred deferred = (EnrichmentOutcome.Deferred) result.outcome();
    assertThat(deferred.tenant()).isEqualTo("Acme");
    assertThat(deferred.until()).isEqualTo(Instant.parse("2026-10-25T00:00:00Z"));
    assertThat(deferred.detail()).contains("over its budget");
    assertThat(provider.calls()).isZero();
    assertThat(usage.total(pipeline.name()).deferred()).isOne();
    // Nobody is left holding the key.
    PreparedRecord prepared = new Enricher(provider).prepare(pipeline, dispute(1, "Acme", 50));
    assertThat(ledger.claim(LedgeredEnricher.keyOf(prepared), Duration.ofSeconds(5)))
        .isInstanceOf(Claim.Acquired.class);
  }

  @Test
  void ledgerHitsAreFreeEvenWhenTheTenantIsOutOfBudget() {
    LedgeredEnricher enricher = enricher(provider);
    // Each call reserves about 60 prompt tokens plus the 256-token output cap and settles to
    // about 100: 400 tokens fit the first reservation but not a second one on top of it.
    CostPolicy policy =
        policy(List.of(), budgets(new BudgetLimit(400, -1)), OverBudgetAction.DEFER, null);

    LedgeredEnricher.Result first = enricher.process(pipeline, dispute(1, "Acme", 50), policy);
    LedgeredEnricher.Result newEvent = enricher.process(pipeline, dispute(2, "Acme", 51), policy);
    LedgeredEnricher.Result replay = enricher.process(pipeline, dispute(1, "Acme", 50), policy);

    assertThat(first.source()).isEqualTo(ResultSource.PROVIDER);
    assertThat(newEvent.outcome()).isInstanceOf(EnrichmentOutcome.Deferred.class);
    assertThat(replay.source()).isEqualTo(ResultSource.LEDGER);
    assertThat(provider.calls()).isOne();
    UsageAccounting.Line total = usage.total(pipeline.name());
    assertThat(total.ledgerHits()).isOne();
    assertThat(total.savedCostMicros()).isEqualTo(total.costMicros()).isPositive();
  }

  @Test
  void downgradesToTheFallbackModelWhenOnlyItFits() {
    LedgeredEnricher enricher = enricher(provider);
    // fake-1 reserves about 100 * 10 + 256 * 40 = 11_000 micro-USD; cheap-1 about 110.
    CostPolicy policy =
        policy(
            List.of(), budgets(new BudgetLimit(-1, 5_000)), OverBudgetAction.DOWNGRADE, "cheap-1");

    LedgeredEnricher.Result result = enricher.process(pipeline, dispute(1, "Acme", 50), policy);

    EnrichmentOutcome.Enriched enriched = (EnrichmentOutcome.Enriched) result.outcome();
    assertThat(enriched.envelope().path("model").asText()).isEqualTo("cheap-1");
    assertThat(result.source()).isEqualTo(ResultSource.PROVIDER);
    assertThat(provider.calls()).isOne();
    assertThat(usage.snapshot())
        .anySatisfy(
            l -> {
              assertThat(l.model()).isEqualTo("cheap-1");
              assertThat(l.calls()).isOne();
              assertThat(l.downgraded()).isOne();
            });
  }

  @Test
  void whenEvenTheFallbackDoesNotFitTheAfterDowngradeActionApplies() {
    LedgeredEnricher enricher = enricher(provider);
    CostPolicy policy =
        policy(List.of(), budgets(new BudgetLimit(-1, 10)), OverBudgetAction.DOWNGRADE, "cheap-1");

    LedgeredEnricher.Result result = enricher.process(pipeline, dispute(1, "Acme", 50), policy);

    EnrichmentOutcome.Skipped skipped = (EnrichmentOutcome.Skipped) result.outcome();
    assertThat(skipped.reason()).isEqualTo(LedgeredEnricher.OVER_BUDGET);
    assertThat(provider.calls()).isZero();
  }

  @Test
  void providerFailureRefundsTheReservation() {
    AtomicInteger attempts = new AtomicInteger();
    FakeLlmProvider failing =
        new FakeLlmProvider(
            request -> {
              attempts.incrementAndGet();
              throw new LlmProviderException("503", true);
            });
    TenantBudgets budgets = budgets(new BudgetLimit(100_000, -1));
    CostPolicy policy = policy(List.of(), budgets, OverBudgetAction.DEFER, null);

    assertThatThrownBy(() -> enricher(failing).process(pipeline, dispute(1, "Acme", 50), policy))
        .isInstanceOf(LlmProviderException.class);

    assertThat(attempts).hasValue(1);
    assertThat(budgets.usage(pipeline.name(), "Acme")).isEqualTo(BudgetUsage.ZERO);
    assertThat(usage.total(pipeline.name()).calls()).isZero();
  }

  @Test
  void unusableAnswerStaysChargedAtItsRealUsage() {
    FakeLlmProvider garbage = new FakeLlmProvider(request -> "not json at all");
    TenantBudgets budgets = budgets(new BudgetLimit(100_000, -1));
    CostPolicy policy = policy(List.of(), budgets, OverBudgetAction.DEFER, null);

    LedgeredEnricher.Result result =
        enricher(garbage).process(pipeline, dispute(1, "Acme", 50), policy);

    assertThat(result.outcome()).isInstanceOf(EnrichmentOutcome.Failed.class);
    BudgetUsage spent = budgets.usage(pipeline.name(), "Acme");
    assertThat(spent.calls()).isOne();
    // Settled to the fake's estimate of the real usage, well under the 256-token output cap.
    long input = result.outcome().response().usage().inputTokens();
    assertThat(spent.tokens()).isEqualTo(input + 4);
  }

  @Test
  void defaultsKeepTheOldBehaviour() {
    LedgeredEnricher enricher = enricher(new FakeLlmProvider(new SchemaAwareResponder()));
    LedgeredEnricher.Result result = enricher.process(pipeline, dispute(1, "Acme", 2));
    assertThat(result.tenant()).isEqualTo(TenantResolver.DEFAULT_TENANT);
    assertThat(result.outcome()).isInstanceOf(EnrichmentOutcome.Enriched.class);
  }
}
