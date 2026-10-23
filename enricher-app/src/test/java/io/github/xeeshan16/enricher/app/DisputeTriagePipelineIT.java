package io.github.xeeshan16.enricher.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.xeeshan16.enricher.kafka.EnricherHeaders;
import io.github.xeeshan16.enricher.ledger.ResultLedger;
import io.github.xeeshan16.enricher.ledger.postgres.PostgresResultLedger;
import io.github.xeeshan16.enricher.llm.fake.FakeLlmProvider;
import io.github.xeeshan16.enricher.llm.fake.SchemaAwareResponder;
import io.github.xeeshan16.enricher.spring.PipelineRunner;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
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
 * End to end against a real broker and PostgreSQL, with Kafka transactions on: disputes in,
 * validated envelopes out, unusable answers to the dead-letter topic with a reason header, and a
 * re-published event answered from the result ledger without a second provider call.
 */
@SpringBootTest
@Testcontainers
class DisputeTriagePipelineIT {

  private static final String INPUT = "payments.disputes";
  private static final String OUTPUT = "payments.disputes.enriched";
  private static final String DLQ = "payments.disputes.enricher-dlq";
  private static final ObjectMapper MAPPER = new ObjectMapper();

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

  @Autowired FakeLlmProvider provider;
  @Autowired PipelineRunner runner;
  @Autowired ResultLedger ledger;

  /** The default fake, except that events containing POISON get an answer that is not JSON. */
  @TestConfiguration
  static class PoisonAwareProvider {
    @Bean
    FakeLlmProvider llmProvider() {
      SchemaAwareResponder responder = new SchemaAwareResponder();
      return new FakeLlmProvider(
          req ->
              req.userPrompt().contains("POISON") ? "Sorry, I cannot help." : responder.apply(req));
    }
  }

  @Test
  void enrichesDisputesAndDeadLettersUnusableAnswers() throws Exception {
    assertThat(runner.transactional()).isTrue();
    assertThat(ledger).isInstanceOf(PostgresResultLedger.class);

    Map<String, String> events =
        Map.of(
            "card-1001",
            "{\"disputeId\":\"D-1001\",\"merchant\":\"Acme"
                + " Streaming\",\"amount\":12.99,\"note\":\"I was charged twice this month, please"
                + " refund the duplicate charge.\"}",
            "card-1002",
            "{\"disputeId\":\"D-1002\",\"merchant\":\"Northwind Electronics\",\"amount\":349.00,"
                + "\"note\":\"I did not make this purchase, my card was used fraudulently.\"}",
            "card-1003",
            "{\"disputeId\":\"D-1003\",\"merchant\":\"POISON Ltd\",\"amount\":5.00,"
                + "\"note\":\"Test record that the model cannot answer.\"}");
    try (KafkaProducer<String, String> producer = producer()) {
      for (Map.Entry<String, String> e : events.entrySet()) {
        producer.send(new ProducerRecord<>(INPUT, e.getKey(), e.getValue())).get();
      }
    }

    List<ConsumerRecord<String, String>> enriched = new ArrayList<>();
    List<ConsumerRecord<String, String>> deadLettered = new ArrayList<>();
    try (KafkaConsumer<String, String> consumer = consumer()) {
      consumer.subscribe(List.of(OUTPUT, DLQ));
      Instant deadline = Instant.now().plus(Duration.ofSeconds(90));
      while ((enriched.size() < 2 || deadLettered.isEmpty()) && Instant.now().isBefore(deadline)) {
        for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
          (r.topic().equals(OUTPUT) ? enriched : deadLettered).add(r);
        }
      }

      assertThat(enriched).hasSize(2);
      assertThat(deadLettered).hasSize(1);
      long callsBeforeReplay = provider.calls();
      assertThat(callsBeforeReplay).isEqualTo(3);

      // Re-publish the first dispute with its fields in another order: same canonical input, so
      // the ledger answers it and no provider call is made.
      String reordered =
          "{\"note\":\"I was charged twice this month, please refund the duplicate charge.\","
              + "\"amount\":12.99,\"merchant\":\"Acme Streaming\",\"disputeId\":\"D-1001\"}";
      try (KafkaProducer<String, String> producer = producer()) {
        producer.send(new ProducerRecord<>(INPUT, "card-1001", reordered)).get();
      }
      ConsumerRecord<String, String> replayed = null;
      deadline = Instant.now().plus(Duration.ofSeconds(60));
      while (replayed == null && Instant.now().isBefore(deadline)) {
        for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
          replayed = r;
        }
      }
      assertThat(replayed).isNotNull();
      assertThat(replayed.topic()).isEqualTo(OUTPUT);
      assertThat(header(replayed, EnricherHeaders.RESULT_SOURCE)).isEqualTo("ledger");
      assertThat(provider.calls()).isEqualTo(callsBeforeReplay);
      JsonNode replayEnvelope = MAPPER.readTree(replayed.value());
      JsonNode firstEnvelope = MAPPER.readTree(valueOf(enriched, "card-1001"));
      assertThat(replayEnvelope.get("enrichment")).isEqualTo(firstEnvelope.get("enrichment"));
      assertThat(replayEnvelope.get("usage")).isEqualTo(firstEnvelope.get("usage"));
      assertThat(replayEnvelope.get("inputHash")).isEqualTo(firstEnvelope.get("inputHash"));
    }

    for (ConsumerRecord<String, String> r : enriched) {
      JsonNode envelope = MAPPER.readTree(r.value());
      assertThat(envelope.path("pipeline").asText()).isEqualTo("dispute-triage");
      assertThat(envelope.path("promptVersion").asText()).isEqualTo("v1");
      assertThat(envelope.path("source").path("topic").asText()).isEqualTo(INPUT);
      assertThat(envelope.path("source").path("key").asText()).isEqualTo(r.key());
      assertThat(envelope.path("inputHash").asText()).startsWith("sha256:");
      assertThat(header(r, EnricherHeaders.INPUT_HASH))
          .isEqualTo(envelope.path("inputHash").asText());
      assertThat(header(r, EnricherHeaders.PIPELINE)).isEqualTo("dispute-triage");
      assertThat(header(r, EnricherHeaders.RESULT_SOURCE)).isEqualTo("provider");
    }
    for (ConsumerRecord<String, String> r : enriched) {
      // Printed (one line each) so the README sample output can be taken from a real run.
      System.out.println("SAMPLE_ENVELOPE " + r.value());
    }
    assertThat(categoryOf(enriched, "card-1001")).isEqualTo("duplicate_charge");
    assertThat(categoryOf(enriched, "card-1002")).isEqualTo("fraud");

    ConsumerRecord<String, String> dead = deadLettered.getFirst();
    assertThat(dead.key()).isEqualTo("card-1003");
    assertThat(dead.value()).isEqualTo(events.get("card-1003"));
    assertThat(header(dead, EnricherHeaders.FAILURE_REASON)).isEqualTo("INVALID_JSON");
    assertThat(header(dead, EnricherHeaders.PIPELINE)).isEqualTo("dispute-triage");
  }

  private static String valueOf(List<ConsumerRecord<String, String>> records, String key) {
    return records.stream().filter(r -> key.equals(r.key())).findFirst().orElseThrow().value();
  }

  private static String categoryOf(List<ConsumerRecord<String, String>> records, String key)
      throws Exception {
    for (ConsumerRecord<String, String> r : records) {
      if (key.equals(r.key())) {
        return MAPPER.readTree(r.value()).path("enrichment").path("category").asText();
      }
    }
    throw new AssertionError("no enriched record for " + key);
  }

  private static String header(ConsumerRecord<String, String> record, String name) {
    var header = record.headers().lastHeader(name);
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
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-reader");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    return new KafkaConsumer<>(props);
  }
}
