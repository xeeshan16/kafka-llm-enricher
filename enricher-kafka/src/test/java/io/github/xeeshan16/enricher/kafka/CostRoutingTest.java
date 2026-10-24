package io.github.xeeshan16.enricher.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.xeeshan16.enricher.cost.BudgetLimit;
import io.github.xeeshan16.enricher.cost.BudgetPeriod;
import io.github.xeeshan16.enricher.cost.CostPolicy;
import io.github.xeeshan16.enricher.cost.InMemoryBudgetStore;
import io.github.xeeshan16.enricher.cost.OverBudgetAction;
import io.github.xeeshan16.enricher.cost.PriceTable;
import io.github.xeeshan16.enricher.cost.SkipRule;
import io.github.xeeshan16.enricher.cost.TenantBudgets;
import io.github.xeeshan16.enricher.cost.TenantResolver;
import io.github.xeeshan16.enricher.engine.Enricher;
import io.github.xeeshan16.enricher.engine.LedgeredEnricher;
import io.github.xeeshan16.enricher.ledger.InMemoryResultLedger;
import io.github.xeeshan16.enricher.llm.fake.FakeLlmProvider;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import io.github.xeeshan16.enricher.pipeline.PromptTemplate;
import io.github.xeeshan16.enricher.schema.OutputSchema;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;

/** Where skipped and over-budget records go. */
class CostRoutingTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Instant NOW = Instant.parse("2026-10-24T10:00:00Z");
  private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
  private static final PipelineDefinition PIPELINE =
      new PipelineDefinition(
          "triage",
          "in",
          "out",
          "in.enricher-dlq",
          "v1",
          "fake-1",
          "",
          PromptTemplate.compile("{{event}}"),
          OutputSchema.parse(
              "{\"type\":\"object\",\"required\":[\"label\"],"
                  + "\"properties\":{\"label\":{\"enum\":[\"refund\",\"other\"]}}}"),
          64);
  private static final SkipRule PINGS =
      new SkipRule("ping", r -> "ping".equals(r.input().path("type").asText()));

  private final FakeLlmProvider provider = new FakeLlmProvider();

  private EnrichmentProcessor processor(CostPolicy policy, boolean emitSkipped) {
    LedgeredEnricher enricher =
        new LedgeredEnricher(
            new Enricher(provider),
            new InMemoryResultLedger(),
            new LedgeredEnricher.Settings(
                Duration.ofMinutes(1), Duration.ZERO, Duration.ofMillis(10)));
    return new EnrichmentProcessor(
        PIPELINE,
        enricher,
        RetryTopics.none("in"),
        new EnvelopeRecordFactory(),
        Duration.ofMillis(10),
        Duration.ZERO,
        CLOCK,
        d -> {},
        policy,
        emitSkipped);
  }

  private static CostPolicy policy(BudgetLimit limit, OverBudgetAction action) {
    TenantBudgets budgets =
        limit == null
            ? null
            : new TenantBudgets(
                new InMemoryBudgetStore(),
                PriceTable.empty(),
                BudgetPeriod.DAILY,
                limit,
                Map.of(),
                CLOCK);
    return new CostPolicy(
        List.of(PINGS),
        TenantResolver.jsonPointer("/merchant"),
        budgets,
        action,
        null,
        OverBudgetAction.DEFER);
  }

  private static ConsumerRecord<String, String> input(String value) {
    return new ConsumerRecord<>("in", 1, 7L, "card-1", value);
  }

  private static String header(ProducerRecord<String, String> record, String name) {
    return EnricherHeaders.read(record.headers(), name);
  }

  @Test
  void skippedRecordWritesASkippedEnvelopeToTheOutputTopic() throws Exception {
    List<ProducerRecord<String, String>> out =
        processor(policy(null, OverBudgetAction.DEFER), true)
            .process(input("{\"type\":\"ping\",\"merchant\":\"acme\"}"));

    assertThat(out).hasSize(1);
    ProducerRecord<String, String> record = out.getFirst();
    assertThat(record.topic()).isEqualTo("out");
    assertThat(record.key()).isEqualTo("card-1");
    assertThat(header(record, EnricherHeaders.RESULT_SOURCE)).isEqualTo("none");
    assertThat(header(record, EnricherHeaders.SKIP_REASON)).isEqualTo("rule:ping");
    assertThat(header(record, EnricherHeaders.TENANT)).isEqualTo("acme");
    JsonNode envelope = MAPPER.readTree(record.value());
    assertThat(envelope.get("enrichment").isNull()).isTrue();
    assertThat(envelope.path("skipped").path("reason").asText()).isEqualTo("rule:ping");
    assertThat(envelope.path("source").path("offset").asLong()).isEqualTo(7);
    assertThat(provider.calls()).isZero();
  }

  @Test
  void skippedRecordsCanWriteNothing() {
    List<ProducerRecord<String, String>> out =
        processor(policy(null, OverBudgetAction.DEFER), false)
            .process(input("{\"type\":\"ping\"}"));

    assertThat(out).isEmpty();
    assertThat(provider.calls()).isZero();
  }

  @Test
  void overBudgetRecordIsParkedOnTheDeferredTopicUntilTheBudgetResets() {
    EnrichmentProcessor processor =
        processor(policy(new BudgetLimit(0, -1), OverBudgetAction.DEFER), true);

    List<ProducerRecord<String, String>> out =
        processor.process(
            input("{\"type\":\"dispute\",\"merchant\":\"acme\",\"note\":\"refund\"}"));

    assertThat(out).hasSize(1);
    ProducerRecord<String, String> deferred = out.getFirst();
    assertThat(deferred.topic()).isEqualTo("in.enricher-deferred");
    assertThat(deferred.key()).isEqualTo("card-1");
    assertThat(header(deferred, EnricherHeaders.FAILURE_REASON)).isEqualTo("BUDGET_EXCEEDED");
    assertThat(header(deferred, EnricherHeaders.TENANT)).isEqualTo("acme");
    long resets = Instant.parse("2026-10-25T00:00:00Z").toEpochMilli();
    assertThat(header(deferred, EnricherHeaders.RETRY_NOT_BEFORE)).isEqualTo(Long.toString(resets));
    assertThat(header(deferred, EnricherHeaders.ORIGINAL_TOPIC)).isEqualTo("in");
    assertThat(header(deferred, EnricherHeaders.ORIGINAL_OFFSET)).isEqualTo("7");
    assertThat(provider.calls()).isZero();

    // Consumed back from the deferred topic, it is held until the reset.
    ConsumerRecord<String, String> parked =
        new ConsumerRecord<>(deferred.topic(), 0, 0L, deferred.key(), deferred.value());
    for (Header h : deferred.headers()) {
      parked.headers().add(h);
    }
    assertThat(processor.notBeforeMillis(parked)).isEqualTo(resets);
  }

  @Test
  void overBudgetWithSkipWritesAnOverBudgetEnvelope() throws Exception {
    List<ProducerRecord<String, String>> out =
        processor(policy(new BudgetLimit(0, -1), OverBudgetAction.SKIP), true)
            .process(input("{\"type\":\"dispute\",\"merchant\":\"acme\",\"note\":\"refund\"}"));

    assertThat(out).hasSize(1);
    assertThat(out.getFirst().topic()).isEqualTo("out");
    assertThat(header(out.getFirst(), EnricherHeaders.SKIP_REASON)).isEqualTo("over-budget");
    assertThat(provider.calls()).isZero();
  }

  @Test
  void enrichedRecordCarriesItsTenant() {
    List<ProducerRecord<String, String>> out =
        processor(policy(new BudgetLimit(100_000, -1), OverBudgetAction.DEFER), true)
            .process(input("{\"type\":\"dispute\",\"merchant\":\"acme\",\"note\":\"refund\"}"));

    assertThat(out).hasSize(1);
    assertThat(out.getFirst().topic()).isEqualTo("out");
    assertThat(header(out.getFirst(), EnricherHeaders.TENANT)).isEqualTo("acme");
    assertThat(header(out.getFirst(), EnricherHeaders.RESULT_SOURCE)).isEqualTo("provider");
    assertThat(provider.calls()).isOne();
  }
}
