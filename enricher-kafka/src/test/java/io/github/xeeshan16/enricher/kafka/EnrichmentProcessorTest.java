package io.github.xeeshan16.enricher.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.xeeshan16.enricher.engine.Enricher;
import io.github.xeeshan16.enricher.engine.LedgeredEnricher;
import io.github.xeeshan16.enricher.ledger.Claim;
import io.github.xeeshan16.enricher.ledger.InMemoryResultLedger;
import io.github.xeeshan16.enricher.ledger.LedgerEntry;
import io.github.xeeshan16.enricher.ledger.LedgerKey;
import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.LlmProviderException;
import io.github.xeeshan16.enricher.llm.LlmRequest;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import io.github.xeeshan16.enricher.llm.TokenUsage;
import io.github.xeeshan16.enricher.llm.fake.FakeLlmProvider;
import io.github.xeeshan16.enricher.pipeline.InputRecord;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import io.github.xeeshan16.enricher.pipeline.PromptTemplate;
import io.github.xeeshan16.enricher.schema.OutputSchema;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;

class EnrichmentProcessorTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Instant NOW = Instant.parse("2026-10-23T10:00:00Z");
  private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
  private static final PipelineDefinition PIPELINE =
      new PipelineDefinition(
          "triage",
          "in",
          "out",
          "in.enricher-dlq",
          "v2",
          "fake-1",
          "",
          PromptTemplate.compile("{{event}}"),
          OutputSchema.parse(
              "{\"type\":\"object\",\"required\":[\"label\"],"
                  + "\"properties\":{\"label\":{\"enum\":[\"refund\",\"other\"]}}}"),
          64);
  private static final RetryTopics RETRIES =
      new RetryTopics("in", List.of(Duration.ofSeconds(5), Duration.ofSeconds(30)));

  private final InMemoryResultLedger ledger = new InMemoryResultLedger();
  private final AtomicInteger sleeps = new AtomicInteger();

  private EnrichmentProcessor processor(LlmProvider provider, Duration busyBudget) {
    LedgeredEnricher enricher =
        new LedgeredEnricher(
            new Enricher(provider),
            ledger,
            new LedgeredEnricher.Settings(
                Duration.ofMinutes(1), Duration.ZERO, Duration.ofMillis(10)));
    return new EnrichmentProcessor(
        PIPELINE,
        enricher,
        RETRIES,
        new EnvelopeRecordFactory(),
        Duration.ofMillis(10),
        busyBudget,
        CLOCK,
        d -> sleeps.incrementAndGet());
  }

  private EnrichmentProcessor processor(LlmProvider provider) {
    return processor(provider, Duration.ZERO);
  }

  /** A provider that throws {@code failure} for its first {@code failures} calls. */
  private static LlmProvider failing(int failures, Function<Integer, RuntimeException> failure) {
    FakeLlmProvider fake = new FakeLlmProvider();
    AtomicInteger calls = new AtomicInteger();
    return new LlmProvider() {
      @Override
      public String name() {
        return "failing";
      }

      @Override
      public LlmResponse complete(LlmRequest request) {
        int n = calls.incrementAndGet();
        if (n <= failures) {
          throw failure.apply(n);
        }
        return fake.complete(request);
      }
    };
  }

  private static ConsumerRecord<String, String> input(String value) {
    return new ConsumerRecord<>("in", 2, 41L, "card-1", value);
  }

  private static String header(ProducerRecord<String, String> record, String name) {
    return EnricherHeaders.read(record.headers(), name);
  }

  /** Consumes {@code produced} as the next stage would see it. */
  private static ConsumerRecord<String, String> consumedFrom(
      ProducerRecord<String, String> produced, long offset) {
    ConsumerRecord<String, String> record =
        new ConsumerRecord<>(produced.topic(), 0, offset, produced.key(), produced.value());
    for (Header h : produced.headers()) {
      record.headers().add(h);
    }
    return record;
  }

  @Test
  void enrichedRecordGoesToTheOutputTopicWithHeaders() throws Exception {
    List<ProducerRecord<String, String>> out =
        processor(new FakeLlmProvider()).process(input("{\"note\":\"please refund me\"}"));

    assertThat(out).hasSize(1);
    ProducerRecord<String, String> record = out.getFirst();
    assertThat(record.topic()).isEqualTo("out");
    assertThat(record.key()).isEqualTo("card-1");
    JsonNode envelope = MAPPER.readTree(record.value());
    assertThat(envelope.path("enrichment").path("label").asText()).isEqualTo("refund");
    assertThat(envelope.path("source").path("offset").asLong()).isEqualTo(41);
    assertThat(header(record, EnricherHeaders.RESULT_SOURCE)).isEqualTo("provider");
    assertThat(header(record, EnricherHeaders.PROMPT_VERSION)).isEqualTo("v2");
    assertThat(header(record, EnricherHeaders.INPUT_HASH)).startsWith("sha256:");
  }

  @Test
  void retryableFailureMovesThroughEveryRetryTopicThenToTheDeadLetterTopic() {
    EnrichmentProcessor processor =
        processor(failing(100, n -> new LlmProviderException("503 #" + n, true)));
    ConsumerRecord<String, String> original = input("{\"note\":\"please refund me\"}");
    original.headers().add("traceparent", EnricherHeaders.utf8("00-abc-def-01"));

    ProducerRecord<String, String> first = processor.process(original).getFirst();
    assertThat(first.topic()).isEqualTo("in.enricher-retry-1");
    assertThat(first.key()).isEqualTo("card-1");
    assertThat(first.value()).isEqualTo(original.value());
    assertThat(header(first, EnricherHeaders.FAILURE_REASON)).isEqualTo("PROVIDER_ERROR");
    assertThat(header(first, EnricherHeaders.FAILURE_DETAIL)).isEqualTo("503 #1");
    assertThat(header(first, EnricherHeaders.ATTEMPTS)).isEqualTo("1");
    assertThat(header(first, EnricherHeaders.RETRY_NOT_BEFORE))
        .isEqualTo(Long.toString(NOW.plusSeconds(5).toEpochMilli()));
    assertThat(header(first, EnricherHeaders.ORIGINAL_TOPIC)).isEqualTo("in");
    assertThat(header(first, EnricherHeaders.ORIGINAL_PARTITION)).isEqualTo("2");
    assertThat(header(first, EnricherHeaders.ORIGINAL_OFFSET)).isEqualTo("41");
    assertThat(header(first, "traceparent"))
        .as("other headers are kept")
        .isEqualTo("00-abc-def-01");

    ConsumerRecord<String, String> atStage1 = consumedFrom(first, 7);
    assertThat(processor.notBeforeMillis(atStage1)).isEqualTo(NOW.plusSeconds(5).toEpochMilli());
    ProducerRecord<String, String> second = processor.process(atStage1).getFirst();
    assertThat(second.topic()).isEqualTo("in.enricher-retry-2");
    assertThat(header(second, EnricherHeaders.ATTEMPTS)).isEqualTo("2");
    assertThat(header(second, EnricherHeaders.RETRY_NOT_BEFORE))
        .isEqualTo(Long.toString(NOW.plusSeconds(30).toEpochMilli()));
    assertThat(header(second, EnricherHeaders.ORIGINAL_OFFSET)).isEqualTo("41");
    assertThat(second.headers().headers(EnricherHeaders.FAILURE_REASON)).hasSize(1);

    ProducerRecord<String, String> dead = processor.process(consumedFrom(second, 3)).getFirst();
    assertThat(dead.topic()).isEqualTo("in.enricher-dlq");
    assertThat(dead.value()).isEqualTo(original.value());
    assertThat(header(dead, EnricherHeaders.FAILURE_REASON)).isEqualTo("RETRIES_EXHAUSTED");
    assertThat(header(dead, EnricherHeaders.FAILURE_DETAIL))
        .isEqualTo("503 #3 (gave up after 3 delivery attempt(s))");
    assertThat(header(dead, EnricherHeaders.ATTEMPTS)).isEqualTo("3");
    assertThat(header(dead, EnricherHeaders.RETRY_NOT_BEFORE)).isNull();
    assertThat(header(dead, EnricherHeaders.ORIGINAL_TOPIC)).isEqualTo("in");
    assertThat(header(dead, EnricherHeaders.PIPELINE)).isEqualTo("triage");
  }

  @Test
  void aRetriedRecordThatSucceedsKeepsTheOriginalCoordinatesInItsEnvelope() throws Exception {
    EnrichmentProcessor processor =
        processor(failing(1, n -> new LlmProviderException("429", true)));
    ProducerRecord<String, String> retry =
        processor.process(input("{\"note\":\"please refund me\"}")).getFirst();

    ProducerRecord<String, String> enriched = processor.process(consumedFrom(retry, 0)).getFirst();

    assertThat(enriched.topic()).isEqualTo("out");
    JsonNode source = MAPPER.readTree(enriched.value()).path("source");
    assertThat(source.path("topic").asText()).isEqualTo("in");
    assertThat(source.path("partition").asInt()).isEqualTo(2);
    assertThat(source.path("offset").asLong()).isEqualTo(41);
  }

  @Test
  void withoutRetryTopicsARetryableFailureIsDeadLetteredAsExhausted() {
    LedgeredEnricher enricher =
        new LedgeredEnricher(
            new Enricher(failing(1, n -> new LlmProviderException("timeout", true))),
            ledger,
            LedgeredEnricher.Settings.defaults());
    EnrichmentProcessor processor =
        new EnrichmentProcessor(
            PIPELINE,
            enricher,
            RetryTopics.none("in"),
            new EnvelopeRecordFactory(),
            Duration.ofMillis(10),
            Duration.ZERO);

    ProducerRecord<String, String> dead = processor.process(input("{}")).getFirst();

    assertThat(dead.topic()).isEqualTo("in.enricher-dlq");
    assertThat(header(dead, EnricherHeaders.FAILURE_REASON)).isEqualTo("RETRIES_EXHAUSTED");
    assertThat(header(dead, EnricherHeaders.ATTEMPTS)).isEqualTo("1");
  }

  @Test
  void nonRetryableProviderFailureAndUnusableAnswersAreDeadLetteredAtOnce() {
    ProducerRecord<String, String> badRequest =
        processor(failing(1, n -> new LlmProviderException("400 bad request", false)))
            .process(input("{}"))
            .getFirst();
    ProducerRecord<String, String> schema =
        processor(new FakeLlmProvider(req -> "{\"label\":\"maybe\"}"))
            .process(input("{}"))
            .getFirst();
    ProducerRecord<String, String> notJson =
        processor(new FakeLlmProvider(req -> "Sorry, I cannot help."))
            .process(input("{}"))
            .getFirst();

    assertThat(List.of(badRequest, schema, notJson))
        .extracting(ProducerRecord::topic)
        .containsOnly("in.enricher-dlq");
    assertThat(header(badRequest, EnricherHeaders.FAILURE_REASON)).isEqualTo("PROVIDER_ERROR");
    assertThat(header(schema, EnricherHeaders.FAILURE_REASON)).isEqualTo("SCHEMA_VIOLATION");
    assertThat(header(notJson, EnricherHeaders.FAILURE_REASON)).isEqualTo("INVALID_JSON");
  }

  @Test
  void busyKeyIsRetriedInPlaceUntilTheOtherWorkerStoresItsResult() {
    FakeLlmProvider provider = new FakeLlmProvider();
    String value = "{\"note\":\"please refund me\"}";
    ConsumerRecord<String, String> record = input(value);
    LedgerKey key =
        LedgeredEnricher.keyOf(
            new Enricher(provider)
                .prepare(
                    PIPELINE,
                    new InputRecord(record.topic(), record.partition(), 41, "card-1", value)));
    assertThat(ledger.claim(key, Duration.ofMinutes(5))).isInstanceOf(Claim.Acquired.class);
    LedgeredEnricher enricher =
        new LedgeredEnricher(
            new Enricher(provider),
            ledger,
            new LedgeredEnricher.Settings(
                Duration.ofMinutes(1), Duration.ZERO, Duration.ofMillis(10)));
    EnrichmentProcessor processor =
        new EnrichmentProcessor(
            PIPELINE,
            enricher,
            RETRIES,
            new EnvelopeRecordFactory(),
            Duration.ofMillis(10),
            Duration.ofMinutes(10),
            Clock.systemUTC(),
            d -> {
              // The other worker finishes while this one waits.
              if (sleeps.incrementAndGet() == 3) {
                ledger.complete(
                    key, new LedgerEntry("{\"label\":\"refund\"}", "fake-1", new TokenUsage(5, 3)));
              }
            });

    ProducerRecord<String, String> out = processor.process(record).getFirst();

    assertThat(out.topic()).isEqualTo("out");
    assertThat(header(out, EnricherHeaders.RESULT_SOURCE)).isEqualTo("ledger");
    assertThat(provider.calls()).isZero();
    assertThat(sleeps).hasValue(3);
  }

  @Test
  void busyKeyBeyondTheBudgetMovesToTheRetryTopic() {
    FakeLlmProvider provider = new FakeLlmProvider();
    ConsumerRecord<String, String> record = input("{}");
    LedgerKey key =
        LedgeredEnricher.keyOf(
            new Enricher(provider).prepare(PIPELINE, new InputRecord("in", 2, 41, "card-1", "{}")));
    ledger.claim(key, Duration.ofMinutes(5));

    ProducerRecord<String, String> retry = processor(provider).process(record).getFirst();

    assertThat(retry.topic()).isEqualTo("in.enricher-retry-1");
    assertThat(header(retry, EnricherHeaders.FAILURE_REASON)).isEqualTo("LEDGER_BUSY");
    assertThat(provider.calls()).isZero();
  }

  @Test
  void tombstonesProduceNothingAndFailuresAreDeadLettered() {
    EnrichmentProcessor processor = processor(new FakeLlmProvider());

    assertThat(processor.process(new ConsumerRecord<>("in", 0, 1L, "k", null))).isEmpty();
    ProducerRecord<String, String> dead =
        processor.onFailure(input("{}"), new IllegalStateException("boom")).getFirst();
    assertThat(dead.topic()).isEqualTo("in.enricher-dlq");
    assertThat(header(dead, EnricherHeaders.FAILURE_REASON)).isEqualTo("PROCESSING_ERROR");
    assertThat(header(dead, EnricherHeaders.FAILURE_DETAIL)).contains("boom");
    assertThat(processor.notBeforeMillis(input("{}"))).isZero();
  }

  @Test
  void retryTopicNamesAndStages() {
    assertThat(RETRIES.topics()).containsExactly("in.enricher-retry-1", "in.enricher-retry-2");
    assertThat(RETRIES.subscriptions())
        .containsExactly("in", "in.enricher-retry-1", "in.enricher-retry-2");
    assertThat(RETRIES.stageOf("in")).isZero();
    assertThat(RETRIES.stageOf("in.enricher-retry-2")).isEqualTo(2);
    assertThat(RETRIES.stageOf("elsewhere")).isEqualTo(-1);
    assertThat(RetryTopics.none("in").subscriptions()).containsExactly("in");
  }
}
