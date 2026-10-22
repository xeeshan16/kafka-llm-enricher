package io.github.xeeshan16.enricher.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.xeeshan16.enricher.MutableClock;
import io.github.xeeshan16.enricher.TestPipelines;
import io.github.xeeshan16.enricher.ledger.Claim;
import io.github.xeeshan16.enricher.ledger.InMemoryResultLedger;
import io.github.xeeshan16.enricher.ledger.LedgerBusyException;
import io.github.xeeshan16.enricher.ledger.LedgerEntry;
import io.github.xeeshan16.enricher.ledger.LedgerKey;
import io.github.xeeshan16.enricher.llm.LlmProviderException;
import io.github.xeeshan16.enricher.llm.TokenUsage;
import io.github.xeeshan16.enricher.llm.fake.FakeLlmProvider;
import io.github.xeeshan16.enricher.llm.fake.SchemaAwareResponder;
import io.github.xeeshan16.enricher.pipeline.InputRecord;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class LedgeredEnricherTest {

  private static final String DISPUTE =
      "{\"disputeId\":\"D-1\",\"merchant\":\"Acme Streaming\",\"amount\":12.99,"
          + "\"note\":\"I was charged twice, please refund the duplicate charge.\"}";
  private static final LedgeredEnricher.Settings SETTINGS =
      new LedgeredEnricher.Settings(
          Duration.ofSeconds(60), Duration.ofSeconds(2), Duration.ofMillis(100));

  private final PipelineDefinition pipeline = TestPipelines.disputeTriage();
  private final MutableClock clock = new MutableClock(Instant.parse("2026-10-22T10:00:00Z"));
  private final InMemoryResultLedger ledger = new InMemoryResultLedger(clock, "test");
  private final FakeLlmProvider provider = new FakeLlmProvider();

  private LedgeredEnricher enricher(FakeLlmProvider provider) {
    return new LedgeredEnricher(
        new Enricher(provider), ledger, SETTINGS, clock, clock::advance /* sleeping moves time */);
  }

  private static InputRecord record(long offset, String value) {
    return new InputRecord("payments.disputes", 0, offset, "card-1", value);
  }

  @Test
  void redeliveryIsAnsweredFromTheLedgerWithIdenticalEnvelope() {
    LedgeredEnricher enricher = enricher(provider);

    LedgeredEnricher.Result first = enricher.process(pipeline, record(5, DISPUTE));
    LedgeredEnricher.Result again = enricher.process(pipeline, record(5, DISPUTE));

    assertThat(first.source()).isEqualTo(ResultSource.PROVIDER);
    assertThat(again.source()).isEqualTo(ResultSource.LEDGER);
    assertThat(provider.calls()).isOne();
    assertThat(((EnrichmentOutcome.Enriched) again.outcome()).envelope())
        .isEqualTo(((EnrichmentOutcome.Enriched) first.outcome()).envelope());
  }

  @Test
  void sameEventWithDifferentKeyOrderIsAHit() {
    LedgeredEnricher enricher = enricher(provider);
    enricher.process(pipeline, record(5, DISPUTE));

    String reordered =
        "{\"note\":\"I was charged twice, please refund the duplicate charge.\","
            + "\"amount\":12.99,\"merchant\":\"Acme Streaming\",\"disputeId\":\"D-1\"}";
    LedgeredEnricher.Result replay = enricher.process(pipeline, record(900, reordered));

    assertThat(replay.source()).isEqualTo(ResultSource.LEDGER);
    assertThat(provider.calls()).isOne();
    assertThat(
            ((EnrichmentOutcome.Enriched) replay.outcome())
                .envelope()
                .path("source")
                .path("offset")
                .asLong())
        .isEqualTo(900L);
  }

  @Test
  void newPromptVersionIsANewKey() {
    LedgeredEnricher enricher = enricher(provider);
    enricher.process(pipeline, record(5, DISPUTE));
    PipelineDefinition v2 =
        new PipelineDefinition(
            pipeline.name(),
            pipeline.inputTopic(),
            pipeline.outputTopic(),
            pipeline.deadLetterTopic(),
            "v2",
            pipeline.model(),
            pipeline.systemPrompt(),
            pipeline.userTemplate(),
            pipeline.outputSchema(),
            pipeline.maxOutputTokens());

    assertThat(enricher.process(v2, record(5, DISPUTE)).source()).isEqualTo(ResultSource.PROVIDER);
    assertThat(provider.calls()).isEqualTo(2);
  }

  @Test
  void unusableAnswerReleasesTheLeaseSoTheNextAttemptCallsAgain() {
    FakeLlmProvider broken = new FakeLlmProvider(req -> "not json");
    LedgeredEnricher enricher = enricher(broken);

    LedgeredEnricher.Result result = enricher.process(pipeline, record(5, DISPUTE));
    assertThat(result.outcome()).isInstanceOf(EnrichmentOutcome.Failed.class);

    enricher.process(pipeline, record(5, DISPUTE));
    assertThat(broken.calls()).isEqualTo(2);
  }

  @Test
  void providerFailureReleasesTheLeaseAndPropagates() {
    AtomicInteger attempts = new AtomicInteger();
    SchemaAwareResponder responder = new SchemaAwareResponder();
    FakeLlmProvider flaky =
        new FakeLlmProvider(
            req -> {
              if (attempts.incrementAndGet() == 1) {
                throw new LlmProviderException("503", true);
              }
              return responder.apply(req);
            });
    LedgeredEnricher enricher = enricher(flaky);

    assertThatThrownBy(() -> enricher.process(pipeline, record(5, DISPUTE)))
        .isInstanceOf(LlmProviderException.class);
    assertThat(enricher.process(pipeline, record(5, DISPUTE)).source())
        .isEqualTo(ResultSource.PROVIDER);
  }

  @Test
  void busyKeyWaitsThenThrowsRetryableBusy() {
    LedgeredEnricher enricher = enricher(provider);
    LedgerKey key =
        LedgeredEnricher.keyOf(new Enricher(provider).prepare(pipeline, record(5, DISPUTE)));
    assertThat(ledger.claim(key, SETTINGS.leaseTtl())).isInstanceOf(Claim.Acquired.class);

    assertThatThrownBy(() -> enricher.process(pipeline, record(5, DISPUTE)))
        .isInstanceOfSatisfying(LedgerBusyException.class, e -> assertThat(e.key()).isEqualTo(key));
    assertThat(provider.calls()).isZero();
  }

  @Test
  void busyKeyResolvesToAHitWhenTheHolderCompletesDuringTheWait() {
    LedgerKey key =
        LedgeredEnricher.keyOf(new Enricher(provider).prepare(pipeline, record(5, DISPUTE)));
    ledger.claim(key, SETTINGS.leaseTtl());
    LedgerEntry stored =
        new LedgerEntry(
            "{\"amount\":12.99,\"category\":\"duplicate_charge\",\"customer_requests_refund\":true,"
                + "\"summary\":\"stored\",\"urgency\":\"low\"}",
            "fake-1",
            new TokenUsage(100, 20));
    AtomicInteger pauses = new AtomicInteger();
    LedgeredEnricher enricher =
        new LedgeredEnricher(
            new Enricher(provider),
            ledger,
            SETTINGS,
            clock,
            d -> {
              clock.advance(d);
              if (pauses.incrementAndGet() == 3) {
                ledger.complete(key, stored);
              }
            });

    LedgeredEnricher.Result result = enricher.process(pipeline, record(5, DISPUTE));

    assertThat(result.source()).isEqualTo(ResultSource.LEDGER);
    assertThat(
            ((EnrichmentOutcome.Enriched) result.outcome())
                .envelope()
                .path("enrichment")
                .path("summary")
                .asText())
        .isEqualTo("stored");
    assertThat(provider.calls()).isZero();
  }

  @Test
  void lateCompletionPublishesTheWinningResult() {
    LedgerEntry winner =
        new LedgerEntry(
            "{\"amount\":12.99,\"category\":\"fraud\",\"customer_requests_refund\":false,"
                + "\"summary\":\"first\",\"urgency\":\"high\"}",
            "fake-1",
            new TokenUsage(1, 1));
    SchemaAwareResponder responder = new SchemaAwareResponder();
    LedgerKey[] key = new LedgerKey[1];
    FakeLlmProvider slow =
        new FakeLlmProvider(
            req -> {
              // While this worker is calling, its lease lapses and another worker completes.
              ledger.complete(key[0], winner);
              return responder.apply(req);
            });
    key[0] = LedgeredEnricher.keyOf(new Enricher(slow).prepare(pipeline, record(5, DISPUTE)));

    LedgeredEnricher.Result result = enricher(slow).process(pipeline, record(5, DISPUTE));

    assertThat(result.source()).isEqualTo(ResultSource.LEDGER);
    assertThat(
            ((EnrichmentOutcome.Enriched) result.outcome())
                .envelope()
                .path("enrichment")
                .path("category")
                .asText())
        .isEqualTo("fraud");
  }

  @Test
  void settingsRejectNonPositiveDurations() {
    assertThatThrownBy(
            () -> new LedgeredEnricher.Settings(Duration.ZERO, Duration.ZERO, Duration.ofMillis(1)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(LedgeredEnricher.Settings.defaults().leaseTtl()).isGreaterThan(Duration.ZERO);
  }
}
