package io.github.xeeshan16.enricher.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.xeeshan16.enricher.engine.Enricher;
import io.github.xeeshan16.enricher.engine.EnrichmentOutcome;
import io.github.xeeshan16.enricher.engine.FailureReason;
import io.github.xeeshan16.enricher.engine.LedgeredEnricher;
import io.github.xeeshan16.enricher.engine.ResultSource;
import io.github.xeeshan16.enricher.ledger.InMemoryResultLedger;
import io.github.xeeshan16.enricher.ledger.LedgerBusyException;
import io.github.xeeshan16.enricher.ledger.LedgerKey;
import io.github.xeeshan16.enricher.llm.LlmProviderException;
import io.github.xeeshan16.enricher.llm.fake.FakeLlmProvider;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import io.github.xeeshan16.enricher.pipeline.PromptTemplate;
import io.github.xeeshan16.enricher.schema.OutputSchema;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ListenerExecutionFailedException;

class EnrichingListenerTest {

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

  private final List<EnrichmentOutcome.Enriched> published = new ArrayList<>();
  private final List<ResultSource> sources = new ArrayList<>();
  private final EnrichedRecordSink sink =
      (pipeline, key, outcome, source) -> {
        published.add(outcome);
        sources.add(source);
      };
  private final InMemoryResultLedger ledger = new InMemoryResultLedger();

  private LedgeredEnricher ledgered(FakeLlmProvider provider) {
    return new LedgeredEnricher(
        new Enricher(provider), ledger, LedgeredEnricher.Settings.defaults());
  }

  private static ConsumerRecord<String, String> record(String value) {
    return new ConsumerRecord<>("in", 0, 7L, "k", value);
  }

  @Test
  void publishesEnrichedEnvelope() {
    new EnrichingListener(PIPELINE, ledgered(new FakeLlmProvider()), sink)
        .onMessage(record("{\"note\":\"please refund me\"}"));

    assertThat(published).hasSize(1);
    assertThat(published.getFirst().envelope().path("enrichment").path("label").asText())
        .isEqualTo("refund");
    assertThat(published.getFirst().envelope().path("promptVersion").asText()).isEqualTo("v2");
  }

  @Test
  void redeliveryIsPublishedFromTheLedgerWithoutASecondCall() {
    FakeLlmProvider provider = new FakeLlmProvider();
    EnrichingListener listener = new EnrichingListener(PIPELINE, ledgered(provider), sink);

    listener.onMessage(record("{\"note\":\"please refund me\"}"));
    listener.onMessage(record("{\"note\":\"please refund me\"}"));

    assertThat(provider.calls()).isOne();
    assertThat(sources).containsExactly(ResultSource.PROVIDER, ResultSource.LEDGER);
    assertThat(published.get(1).envelope()).isEqualTo(published.get(0).envelope());
  }

  @Test
  void skipsTombstones() {
    FakeLlmProvider provider = new FakeLlmProvider();
    new EnrichingListener(PIPELINE, ledgered(provider), sink).onMessage(record(null));

    assertThat(published).isEmpty();
    assertThat(provider.calls()).isZero();
  }

  @Test
  void unusableAnswerThrowsNonRetryableFailure() {
    EnrichingListener listener =
        new EnrichingListener(PIPELINE, ledgered(new FakeLlmProvider(req -> "{\"x\":1}")), sink);

    assertThatThrownBy(() -> listener.onMessage(record("{}")))
        .isInstanceOfSatisfying(
            EnrichmentFailedException.class,
            e -> assertThat(e.reason()).isEqualTo(FailureReason.SCHEMA_VIOLATION));
    assertThat(published).isEmpty();
  }

  @Test
  void failureHeadersClassifyWrappedExceptions() {
    Headers schema =
        PipelineRunner.failureHeaders(
            PIPELINE,
            new ListenerExecutionFailedException(
                "listener failed",
                new EnrichmentFailedException(FailureReason.INVALID_JSON, "no object")));
    Headers provider =
        PipelineRunner.failureHeaders(
            PIPELINE,
            new ListenerExecutionFailedException(
                "listener failed", new LlmProviderException("503 from upstream", true)));
    Headers other = PipelineRunner.failureHeaders(PIPELINE, new IllegalStateException("boom"));
    LedgerBusyException busyException =
        new LedgerBusyException(
            new LedgerKey("triage", "v2", "fake-1", "sha256:00"), "pod-a", Instant.EPOCH);
    Headers busy =
        PipelineRunner.failureHeaders(
            PIPELINE, new ListenerExecutionFailedException("listener failed", busyException));

    assertThat(header(schema, EnricherHeaders.FAILURE_REASON)).isEqualTo("INVALID_JSON");
    assertThat(header(schema, EnricherHeaders.FAILURE_DETAIL)).contains("no object");
    assertThat(header(schema, EnricherHeaders.PIPELINE)).isEqualTo("triage");
    assertThat(header(provider, EnricherHeaders.FAILURE_REASON)).isEqualTo("PROVIDER_ERROR");
    assertThat(header(other, EnricherHeaders.FAILURE_REASON)).isEqualTo("PROCESSING_ERROR");
    assertThat(header(busy, EnricherHeaders.FAILURE_REASON)).isEqualTo("LEDGER_BUSY");
    assertThat(PipelineRunner.isLedgerBusy(new RuntimeException(busyException))).isTrue();
    assertThat(PipelineRunner.isLedgerBusy(new IllegalStateException("boom"))).isFalse();
  }

  private static String header(Headers headers, String name) {
    return new String(headers.lastHeader(name).value(), StandardCharsets.UTF_8);
  }
}
