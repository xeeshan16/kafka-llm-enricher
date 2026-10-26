package io.github.xeeshan16.enricher.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.xeeshan16.enricher.spring.PipelinesHealthIndicator;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The application as shipped, with tracing and Prometheus export on: one dispute published inside
 * an upstream trace comes out enriched with the trace continued, the spans form process, chat and
 * send under the upstream span, no span carries the card number, and the Prometheus scrape holds
 * the model-call, record, token, cost, engine and rate-limiter series the Grafana dashboard reads.
 */
@SpringBootTest(
    properties = {
      "management.tracing.sampling.probability=1.0",
      "enricher.providers.fake.latency=15ms",
      "enricher.pricing.fake-1.input-per-million=0.15",
      "enricher.pricing.fake-1.output-per-million=0.60"
    })
@AutoConfigureObservability
@Testcontainers
class ObservabilityIT {

  private static final String INPUT = "payments.disputes";
  private static final String OUTPUT = "payments.disputes.enriched";
  private static final String TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
  private static final String UPSTREAM_SPAN = "00f067aa0ba902b7";

  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

  @DynamicPropertySource
  static void infrastructure(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
  }

  @TestConfiguration
  static class Spans {
    @Bean
    InMemorySpanExporter inMemorySpanExporter() {
      return InMemorySpanExporter.create();
    }
  }

  @Autowired InMemorySpanExporter spans;
  @Autowired PrometheusMeterRegistry prometheus;
  @Autowired PipelinesHealthIndicator health;

  @Test
  void tracesEachRecordThroughTheModelCallAndExportsTheDashboardSeries() throws Exception {
    ProducerRecord<String, String> dispute =
        new ProducerRecord<>(
            INPUT,
            "card-2001",
            "{\"disputeId\":\"D-2001\",\"merchant\":\"Northwind Electronics\",\"amount\":349.00,"
                + "\"note\":\"I did not make this purchase, my card 4111 1111 1111 1111 was used"
                + " fraudulently.\"}");
    dispute
        .headers()
        .add(
            "traceparent",
            ("00-" + TRACE_ID + "-" + UPSTREAM_SPAN + "-01").getBytes(StandardCharsets.UTF_8));
    try (KafkaProducer<String, String> producer = producer()) {
      producer.send(dispute).get();
    }

    ConsumerRecord<String, String> enriched = null;
    try (KafkaConsumer<String, String> consumer = consumer()) {
      consumer.subscribe(List.of(OUTPUT));
      Instant deadline = Instant.now().plus(Duration.ofSeconds(90));
      while (enriched == null && Instant.now().isBefore(deadline)) {
        for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
          enriched = r;
        }
      }
    }
    assertThat(enriched).isNotNull();
    String outputTraceparent = header(enriched, "traceparent");
    assertThat(outputTraceparent).startsWith("00-" + TRACE_ID + "-");

    List<SpanData> trace = awaitSpans(3);
    SpanData process = span(trace, "process " + INPUT);
    SpanData chat = span(trace, "chat fake-1");
    SpanData send = span(trace, "send " + OUTPUT);
    assertThat(process.getKind()).isEqualTo(SpanKind.CONSUMER);
    assertThat(process.getParentSpanId()).isEqualTo(UPSTREAM_SPAN);
    assertThat(chat.getKind()).isEqualTo(SpanKind.CLIENT);
    assertThat(chat.getParentSpanId()).isEqualTo(process.getSpanId());
    assertThat(send.getKind()).isEqualTo(SpanKind.PRODUCER);
    assertThat(send.getParentSpanId()).isEqualTo(process.getSpanId());
    assertThat(outputTraceparent).contains("-" + send.getSpanId() + "-");
    assertThat(attribute(chat, "gen_ai.provider.name")).isEqualTo("fake");
    assertThat(attribute(chat, "gen_ai.request.model")).isEqualTo("fake-1");
    assertThat(Long.parseLong(attribute(chat, "gen_ai.usage.input_tokens"))).isPositive();
    assertThat(attribute(process, "outcome")).isEqualTo("enriched");
    assertThat(attribute(process, "pipeline")).isEqualTo("dispute-triage");
    assertThat(attribute(process, "enricher.tenant")).isEqualTo("Northwind Electronics");
    for (SpanData span : trace) {
      assertThat(span.getAttributes().asMap().values())
          .as("attributes of %s", span.getName())
          .noneMatch(v -> String.valueOf(v).contains("4111"));
      // Printed so the README can quote a real trace.
      System.out.printf(
          "TRACE_SPAN %-32s kind=%-8s parent=%s duration=%.1fms%n",
          span.getName(),
          span.getKind(),
          span.getParentSpanId().equals(UPSTREAM_SPAN)
              ? "upstream"
              : span.getParentSpanId().equals(process.getSpanId()) ? "process" : "other",
          (span.getEndEpochNanos() - span.getStartEpochNanos()) / 1e6);
    }

    String scrape = prometheus.scrape();
    List<String> series =
        Arrays.stream(scrape.split("\n"))
            .filter(l -> l.startsWith("enricher_") || l.startsWith("gen_ai_"))
            .toList();
    assertThat(series)
        .anyMatch(
            l ->
                l.startsWith("gen_ai_client_operation_seconds_count{")
                    && l.contains("gen_ai_provider_name=\"fake\"")
                    && l.contains("error_type=\"none\""));
    assertThat(series)
        .anyMatch(
            l ->
                l.startsWith("enricher_record_process_seconds_count{")
                    && l.contains("outcome=\"enriched\""));
    assertThat(series).anyMatch(l -> l.startsWith("gen_ai_client_token_usage_bucket{"));
    assertThat(series)
        .anyMatch(
            l ->
                l.startsWith("enricher_cost_usd_total{")
                    && l.contains("tenant=\"Northwind Electronics\""));
    assertThat(series)
        .anyMatch(l -> l.startsWith("enricher_pii_masked_total{") && l.contains("type=\"PAN\""));
    assertThat(series).anyMatch(l -> l.startsWith("enricher_records_polled_total{"));
    assertThat(series).anyMatch(l -> l.startsWith("enricher_ratelimit_concurrency_limit{"));
    for (String line : series) {
      if (!line.contains("_bucket{")) {
        System.out.println("PROMETHEUS " + line);
      }
    }

    assertThat(health.health().getStatus()).isEqualTo(Status.UP);
  }

  private List<SpanData> awaitSpans(int atLeast) throws InterruptedException {
    Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
    while (Instant.now().isBefore(deadline)) {
      List<SpanData> trace =
          spans.getFinishedSpanItems().stream()
              .filter(s -> s.getTraceId().equals(TRACE_ID))
              .toList();
      if (trace.size() >= atLeast) {
        return trace;
      }
      Thread.sleep(250);
    }
    throw new AssertionError("spans of trace " + TRACE_ID + ": " + spans.getFinishedSpanItems());
  }

  private static SpanData span(List<SpanData> trace, String name) {
    return trace.stream()
        .filter(s -> s.getName().equals(name))
        .findFirst()
        .orElseThrow(
            () ->
                new AssertionError(
                    "no span " + name + " in " + trace.stream().map(SpanData::getName).toList()));
  }

  private static String attribute(SpanData span, String key) {
    return span.getAttributes().get(AttributeKey.stringKey(key));
  }

  private static String header(ConsumerRecord<String, String> record, String name) {
    Header header = record.headers().lastHeader(name);
    assertThat(header).as("header %s", name).isNotNull();
    return new String(header.value(), StandardCharsets.UTF_8);
  }

  private static KafkaProducer<String, String> producer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return new KafkaProducer<>(props);
  }

  private static KafkaConsumer<String, String> consumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "observability-it");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    return new KafkaConsumer<>(props);
  }
}
