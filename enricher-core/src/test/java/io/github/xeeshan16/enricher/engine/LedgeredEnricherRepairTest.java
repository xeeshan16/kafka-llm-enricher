package io.github.xeeshan16.enricher.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.xeeshan16.enricher.MutableClock;
import io.github.xeeshan16.enricher.TestPipelines;
import io.github.xeeshan16.enricher.cost.BudgetLimit;
import io.github.xeeshan16.enricher.cost.BudgetPeriod;
import io.github.xeeshan16.enricher.cost.CostPolicy;
import io.github.xeeshan16.enricher.cost.InMemoryBudgetStore;
import io.github.xeeshan16.enricher.cost.OverBudgetAction;
import io.github.xeeshan16.enricher.cost.PriceTable;
import io.github.xeeshan16.enricher.cost.TenantBudgets;
import io.github.xeeshan16.enricher.cost.TenantResolver;
import io.github.xeeshan16.enricher.cost.UsageAccounting;
import io.github.xeeshan16.enricher.ledger.Claim;
import io.github.xeeshan16.enricher.ledger.InMemoryResultLedger;
import io.github.xeeshan16.enricher.llm.LlmProviderException;
import io.github.xeeshan16.enricher.llm.LlmRequest;
import io.github.xeeshan16.enricher.llm.fake.FakeLlmProvider;
import io.github.xeeshan16.enricher.pipeline.InputRecord;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import io.github.xeeshan16.enricher.privacy.PromptSafety;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class LedgeredEnricherRepairTest {

  private static final LedgeredEnricher.Settings SETTINGS =
      new LedgeredEnricher.Settings(
          Duration.ofSeconds(60), Duration.ofSeconds(2), Duration.ofMillis(100));
  private static final String BAD = "Here you go: {\"category\": \"fraud\", urgency: high}";
  private static final String GOOD =
      "{\"category\":\"fraud\",\"urgency\":\"high\",\"summary\":\"Card used without consent.\","
          + "\"customer_requests_refund\":true}";

  private final PipelineDefinition pipeline =
      TestPipelines.disputeTriage().withSafety(PromptSafety.defaults()).withRepairAttempts(1);
  private final MutableClock clock = new MutableClock(Instant.parse("2026-10-25T10:00:00Z"));
  private final InMemoryResultLedger ledger = new InMemoryResultLedger(clock, "test");
  private final PriceTable prices =
      new PriceTable(Map.of("fake-1", PriceTable.Price.of("10", "40")));
  private final UsageAccounting usage = new UsageAccounting(prices);

  private static final InputRecord RECORD =
      new InputRecord(
          "payments.disputes",
          0,
          3,
          "card-3",
          "{\"merchant\":\"Acme\",\"amount\":80,\"card\":\"4111111111111111\","
              + "\"note\":\"Not my purchase.\"}");

  /** Answers BAD to first prompts and GOOD to repair prompts. */
  private static Function<LlmRequest, String> badThenRepaired(AtomicInteger calls) {
    return request -> {
      calls.incrementAndGet();
      return request.userPrompt().startsWith("Your previous answer") ? GOOD : BAD;
    };
  }

  private LedgeredEnricher enricher(FakeLlmProvider provider) {
    return new LedgeredEnricher(
        new Enricher(provider), ledger, SETTINGS, clock, clock::advance, usage);
  }

  @Test
  void repairedAnswerIsStoredOnceAndReplaysAreFree() {
    AtomicInteger calls = new AtomicInteger();
    LedgeredEnricher enricher = enricher(new FakeLlmProvider(badThenRepaired(calls)));

    LedgeredEnricher.Result first = enricher.process(pipeline, RECORD);
    LedgeredEnricher.Result replay = enricher.process(pipeline, RECORD);

    assertThat(first.source()).isEqualTo(ResultSource.PROVIDER);
    EnrichmentOutcome.Enriched enriched = (EnrichmentOutcome.Enriched) first.outcome();
    assertThat(replay.source()).isEqualTo(ResultSource.LEDGER);
    assertThat(((EnrichmentOutcome.Enriched) replay.outcome()).envelope())
        .isEqualTo(enriched.envelope());
    assertThat(calls).hasValue(2);

    UsageAccounting.Line total = usage.total(pipeline.name());
    assertThat(total.calls()).isEqualTo(2);
    assertThat(total.ledgerHits()).isEqualTo(1);
    // The ledger stored the usage of both calls, so a replay saves both.
    assertThat(total.savedTokens())
        .isEqualTo(
            enriched.envelope().path("usage").path("inputTokens").asLong()
                + enriched.envelope().path("usage").path("outputTokens").asLong());
    UsageAccounting.Quality quality = usage.quality(pipeline.name());
    assertThat(quality.repairs()).isEqualTo(1);
    assertThat(quality.repaired()).isEqualTo(1);
    // One card number masked per delivery (the replay prepared the record too).
    assertThat(quality.masked()).containsEntry("PAN", 2L);
  }

  @Test
  void failedRepairKeepsTheOriginalFailureAndReleasesTheLease() {
    AtomicInteger calls = new AtomicInteger();
    FakeLlmProvider provider =
        new FakeLlmProvider(
            request -> {
              if (calls.incrementAndGet() > 1) {
                throw new LlmProviderException("503 from provider", true);
              }
              return BAD;
            });

    LedgeredEnricher.Result result = enricher(provider).process(pipeline, RECORD);

    assertThat(((EnrichmentOutcome.Failed) result.outcome()).reason())
        .isEqualTo(FailureReason.INVALID_JSON);
    assertThat(calls).hasValue(2);
    assertThat(usage.quality(pipeline.name()).repairs()).isEqualTo(1);
    assertThat(usage.quality(pipeline.name()).repaired()).isZero();
    assertThat(
            ledger.claim(
                LedgeredEnricher.keyOf(new Enricher(provider).prepare(pipeline, RECORD)),
                Duration.ofSeconds(5)))
        .isInstanceOf(Claim.Acquired.class);
  }

  @Test
  void repairIsSkippedWhenTheBudgetHasNoRoomForIt() {
    AtomicInteger calls = new AtomicInteger();
    FakeLlmProvider provider = new FakeLlmProvider(badThenRepaired(calls));
    LlmRequest firstRequest = new Enricher(provider).prepare(pipeline, RECORD).request();
    // Room for exactly one worst-case call.
    long oneCall =
        io.github.xeeshan16.enricher.llm.TokenEstimator.estimate(firstRequest).totalTokens();
    TenantBudgets budgets =
        new TenantBudgets(
            new InMemoryBudgetStore(),
            prices,
            BudgetPeriod.DAILY,
            new BudgetLimit(oneCall, -1),
            Map.of(),
            clock);
    CostPolicy policy =
        new CostPolicy(
            List.of(),
            TenantResolver.jsonPointer("/merchant"),
            budgets,
            OverBudgetAction.DEFER,
            null,
            OverBudgetAction.DEFER);

    LedgeredEnricher.Result result = enricher(provider).process(pipeline, RECORD, policy);

    assertThat(((EnrichmentOutcome.Failed) result.outcome()).reason())
        .isEqualTo(FailureReason.INVALID_JSON);
    assertThat(calls).hasValue(1);
  }
}
