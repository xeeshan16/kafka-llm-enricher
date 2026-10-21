package io.github.xeeshan16.enricher.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.xeeshan16.enricher.TestPipelines;
import io.github.xeeshan16.enricher.llm.LlmProviderException;
import io.github.xeeshan16.enricher.llm.fake.FakeLlmProvider;
import io.github.xeeshan16.enricher.pipeline.InputRecord;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import org.junit.jupiter.api.Test;

class EnricherTest {

  private static final String DUPLICATE =
      "{\"disputeId\":\"D-1\",\"merchant\":\"Acme Streaming\",\"amount\":12.99,"
          + "\"note\":\"I was charged twice, please refund the duplicate charge.\"}";

  private final PipelineDefinition pipeline = TestPipelines.disputeTriage();

  private static InputRecord record(String value) {
    return new InputRecord("payments.disputes", 2, 41L, "card-7", value);
  }

  @Test
  void buildsEnvelopeWithValidatedEnrichment() {
    FakeLlmProvider provider = new FakeLlmProvider();
    EnrichmentOutcome outcome = new Enricher(provider).enrich(pipeline, record(DUPLICATE));

    assertThat(outcome).isInstanceOf(EnrichmentOutcome.Enriched.class);
    JsonNode envelope = ((EnrichmentOutcome.Enriched) outcome).envelope();
    assertThat(envelope.path("format").asText()).isEqualTo(Enricher.ENVELOPE_FORMAT);
    assertThat(envelope.path("pipeline").asText()).isEqualTo("dispute-triage");
    assertThat(envelope.path("promptVersion").asText()).isEqualTo("v1");
    assertThat(envelope.path("model").asText()).isEqualTo("fake-1");
    assertThat(envelope.path("inputHash").asText()).startsWith("sha256:").hasSize(71);
    assertThat(envelope.path("source").path("partition").asInt()).isEqualTo(2);
    assertThat(envelope.path("source").path("offset").asLong()).isEqualTo(41L);
    assertThat(envelope.path("source").path("key").asText()).isEqualTo("card-7");
    assertThat(envelope.path("input").path("disputeId").asText()).isEqualTo("D-1");
    assertThat(envelope.path("enrichment").path("category").asText()).isEqualTo("duplicate_charge");
    assertThat(envelope.path("enrichment").path("customer_requests_refund").asBoolean()).isTrue();
    assertThat(envelope.path("enrichment").path("amount").asDouble()).isEqualTo(12.99);
    assertThat(envelope.path("usage").path("inputTokens").asLong()).isPositive();
    assertThat(provider.calls()).isEqualTo(1);
  }

  @Test
  void sameInputGivesIdenticalEnvelopeAndHashRegardlessOfKeyOrder() {
    Enricher enricher = new Enricher(new FakeLlmProvider());
    String reordered =
        "{\"note\":\"I was charged twice, please refund the duplicate charge.\","
            + "\"amount\":12.99,\"merchant\":\"Acme Streaming\",\"disputeId\":\"D-1\"}";

    EnrichmentOutcome first = enricher.enrich(pipeline, record(DUPLICATE));
    EnrichmentOutcome second = enricher.enrich(pipeline, record(DUPLICATE));
    EnrichmentOutcome third = enricher.enrich(pipeline, record(reordered));

    assertThat(((EnrichmentOutcome.Enriched) first).envelope())
        .isEqualTo(((EnrichmentOutcome.Enriched) second).envelope());
    assertThat(third.inputHash()).isEqualTo(first.inputHash());
  }

  @Test
  void acceptsAnswerWrappedInCodeFence() {
    String answer =
        "Sure! ```json\n{\"category\":\"fraud\",\"urgency\":\"high\",\"summary\":\"Card stolen.\","
            + "\"customer_requests_refund\":true}\n```";
    Enricher enricher = new Enricher(new FakeLlmProvider(req -> answer));

    EnrichmentOutcome outcome = enricher.enrich(pipeline, record(DUPLICATE));

    assertThat(outcome).isInstanceOf(EnrichmentOutcome.Enriched.class);
  }

  @Test
  void answerWithoutJsonIsInvalidJson() {
    Enricher enricher = new Enricher(new FakeLlmProvider(req -> "I cannot help with that."));

    EnrichmentOutcome outcome = enricher.enrich(pipeline, record(DUPLICATE));

    assertThat(outcome)
        .isInstanceOfSatisfying(
            EnrichmentOutcome.Failed.class,
            f -> assertThat(f.reason()).isEqualTo(FailureReason.INVALID_JSON));
  }

  @Test
  void truncatedJsonIsInvalidJson() {
    Enricher enricher = new Enricher(new FakeLlmProvider(req -> "{\"category\": \"fraud\", }"));

    EnrichmentOutcome outcome = enricher.enrich(pipeline, record(DUPLICATE));

    assertThat(outcome)
        .isInstanceOfSatisfying(
            EnrichmentOutcome.Failed.class,
            f -> assertThat(f.reason()).isEqualTo(FailureReason.INVALID_JSON));
  }

  @Test
  void answerOutsideSchemaIsSchemaViolationWithDetail() {
    Enricher enricher =
        new Enricher(
            new FakeLlmProvider(
                req -> "{\"category\":\"chargeback\",\"urgency\":\"high\",\"summary\":\"x\"}"));

    EnrichmentOutcome outcome = enricher.enrich(pipeline, record(DUPLICATE));

    assertThat(outcome)
        .isInstanceOfSatisfying(
            EnrichmentOutcome.Failed.class,
            f -> {
              assertThat(f.reason()).isEqualTo(FailureReason.SCHEMA_VIOLATION);
              assertThat(f.detail()).contains("category").contains("customer_requests_refund");
            });
  }

  @Test
  void providerFailurePropagatesForTheCallerToRetry() {
    Enricher enricher =
        new Enricher(
            new FakeLlmProvider(
                req -> {
                  throw new LlmProviderException("429 Too Many Requests", true);
                }));

    assertThatThrownBy(() -> enricher.enrich(pipeline, record(DUPLICATE)))
        .isInstanceOf(LlmProviderException.class)
        .satisfies(e -> assertThat(((LlmProviderException) e).retryable()).isTrue());
  }

  @Test
  void nonJsonValueIsTreatedAsText() {
    Enricher enricher = new Enricher(new FakeLlmProvider());

    EnrichmentOutcome outcome =
        enricher.enrich(pipeline, record("Customer says the parcel never arrived, product missing."));

    JsonNode envelope = ((EnrichmentOutcome.Enriched) outcome).envelope();
    assertThat(envelope.path("input").isTextual()).isTrue();
    assertThat(envelope.path("enrichment").path("category").asText())
        .isEqualTo("product_not_received");
  }

  @Test
  void extractsOutermostObject() {
    assertThat(Enricher.extractJsonObject("x {\"a\":{\"b\":1}} y")).isEqualTo("{\"a\":{\"b\":1}}");
    assertThat(Enricher.extractJsonObject("no json")).isNull();
    assertThat(Enricher.extractJsonObject("} {")).isNull();
  }
}
