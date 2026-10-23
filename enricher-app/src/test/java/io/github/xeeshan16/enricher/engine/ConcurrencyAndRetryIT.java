package io.github.xeeshan16.enricher.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.xeeshan16.enricher.kafka.EngineStats;
import io.github.xeeshan16.enricher.kafka.EnricherHeaders;
import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.LlmProviderException;
import io.github.xeeshan16.enricher.llm.fake.FakeLlmProvider;
import io.github.xeeshan16.enricher.llm.fake.SchemaAwareResponder;
import io.github.xeeshan16.enricher.spring.PipelineRunner;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The day-3 runtime against a real broker and PostgreSQL, with Kafka transactions on.
 *
 * <ul>
 *   <li>{@code ordered}: 8 keys on one partition, one call that takes 5 s while {@code
 *       max.poll.interval.ms} is 3 s. Other keys keep being enriched in parallel, every key's
 *       output stays in input order, and the consumer is never expelled from its group.
 *   <li>{@code flaky}: transient, permanent, endless and poison failures. Each lands where the
 *       routing table says: output after passing through the retry topics with their delays, or the
 *       dead-letter topic with the right reason, and the partition keeps moving.
 * </ul>
 */
@Testcontainers
class ConcurrencyAndRetryIT {

  private static final Logger log = LoggerFactory.getLogger(ConcurrencyAndRetryIT.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern ID = Pattern.compile("\\\\?\"id\\\\?\":\\\\?\"([a-z0-9-]+)");
  private static final int KEYS = 8;
  private static final int PER_KEY = 5;
  private static final String SLOW_ID = "ord-k0-0";
  private static final Duration SLOW = Duration.ofSeconds(5);
  private static final String NOTE = "I was charged twice this month, please refund the duplicate.";

  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

  /** Epoch milliseconds of every provider call, per record id. */
  static final Map<String, List<Long>> CALLS = new ConcurrentHashMap<>();

  static final AtomicInteger RUNNING = new AtomicInteger();
  static final AtomicInteger MAX_RUNNING = new AtomicInteger();
  static final AtomicInteger ORDERED_DONE = new AtomicInteger();
  static final AtomicLong DONE_WHEN_SLOW_RETURNED = new AtomicLong(-1);
  static final List<String> STARTED = new CopyOnWriteArrayList<>();

  static ConfigurableApplicationContext context;

  @SpringBootConfiguration
  @EnableAutoConfiguration
  static class Instance {

    @Bean
    LlmProvider scriptedProvider() {
      SchemaAwareResponder responder = new SchemaAwareResponder();
      return new FakeLlmProvider(
          request -> {
            String id = idOf(request.userPrompt());
            List<Long> calls = CALLS.computeIfAbsent(id, k -> new CopyOnWriteArrayList<>());
            calls.add(System.currentTimeMillis());
            if (id.startsWith("ord-")) {
              return ordered(id, () -> responder.apply(request));
            }
            if (id.startsWith("transient") && calls.size() <= 4) {
              throw new LlmProviderException("503 injected, call " + calls.size(), true);
            }
            if (id.startsWith("fatal")) {
              throw new LlmProviderException("400 injected", false);
            }
            if (id.startsWith("always")) {
              throw new LlmProviderException("503 injected, call " + calls.size(), true);
            }
            if (id.startsWith("poison")) {
              throw new IllegalStateException("bug: cannot handle " + id);
            }
            return responder.apply(request);
          });
    }
  }

  private static String ordered(String id, java.util.function.Supplier<String> answer) {
    STARTED.add(id);
    MAX_RUNNING.accumulateAndGet(RUNNING.incrementAndGet(), Math::max);
    try {
      sleep(id.equals(SLOW_ID) ? SLOW : Duration.ofMillis(150));
      if (id.equals(SLOW_ID)) {
        DONE_WHEN_SLOW_RETURNED.set(ORDERED_DONE.get());
      }
      return answer.get();
    } finally {
      RUNNING.decrementAndGet();
      ORDERED_DONE.incrementAndGet();
    }
  }

  @BeforeAll
  static void startInstance() {
    context =
        new SpringApplicationBuilder(Instance.class)
            .web(WebApplicationType.NONE)
            .run(
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + POSTGRES.getUsername(),
                "--spring.datasource.password=" + POSTGRES.getPassword(),
                "--spring.kafka.producer.transaction-id-prefix=it-concurrency-",
                "--spring.kafka.consumer.properties.max.poll.interval.ms=3000",
                "--enricher.delivery=exactly-once",
                "--enricher.ledger.store=postgres",
                "--enricher.topics.partitions=1",
                "--enricher.resilience.call-timeout=10s",
                "--enricher.resilience.max-attempts=2",
                "--enricher.resilience.initial-backoff=20ms",
                "--enricher.resilience.max-backoff=50ms",
                "--enricher.pipelines.ordered.input-topic=it.ordered",
                "--enricher.pipelines.ordered.output-topic=it.ordered.out",
                "--enricher.pipelines.ordered.max-in-flight=8",
                "--enricher.pipelines.ordered.user-template=Classify {{event}}",
                "--enricher.pipelines.ordered.output-schema=classpath:schemas/dispute-triage.v1.json",
                "--enricher.pipelines.flaky.input-topic=it.flaky",
                "--enricher.pipelines.flaky.output-topic=it.flaky.out",
                "--enricher.pipelines.flaky.retry-delays=1s,2s",
                "--enricher.pipelines.flaky.user-template=Classify {{event}}",
                "--enricher.pipelines.flaky.output-schema=classpath:schemas/dispute-triage.v1.json");
  }

  @AfterAll
  static void stopInstance() {
    if (context != null) {
      context.close();
    }
  }

  @Test
  void slowCallDelaysOnlyItsKeyAndNeverExpelsTheConsumer() throws Exception {
    long start = System.nanoTime();
    try (KafkaProducer<String, String> producer = producer()) {
      for (int seq = 0; seq < PER_KEY; seq++) {
        for (int k = 0; k < KEYS; k++) {
          String id = "ord-k" + k + "-" + seq;
          String value =
              "{\"id\":\""
                  + id
                  + "\",\"seq\":"
                  + seq
                  + ",\"merchant\":\"M"
                  + k
                  + "\",\"note\":\""
                  + NOTE
                  + "\"}";
          producer.send(new ProducerRecord<>("it.ordered", "k" + k, value)).get();
        }
      }
    }

    List<ConsumerRecord<String, String>> out =
        readUntil(List.of("it.ordered.out"), records -> records.size() >= KEYS * PER_KEY);
    double seconds = (System.nanoTime() - start) / 1e9;
    EngineStats.Snapshot stats = context.getBean(PipelineRunner.class).stats("ordered");
    log.info(
        "concurrency: records={} seconds={} maxConcurrentCalls={} doneWhileSlowCallRan={}"
            + " revoked={} lost={} commits={}",
        out.size(),
        String.format("%.1f", seconds),
        MAX_RUNNING.get(),
        DONE_WHEN_SLOW_RETURNED.get(),
        stats.partitionsRevoked(),
        stats.partitionsLost(),
        stats.commits());

    assertThat(out).hasSize(KEYS * PER_KEY);
    for (int k = 0; k < KEYS; k++) {
      String key = "k" + k;
      List<Integer> seqs = new ArrayList<>();
      for (ConsumerRecord<String, String> r : out) {
        if (key.equals(r.key())) {
          seqs.add(MAPPER.readTree(r.value()).path("input").path("seq").asInt());
        }
      }
      assertThat(seqs).as("output order of key %s", key).containsExactly(0, 1, 2, 3, 4);
    }
    // The 35 records of the other keys finished while the slow call was still running.
    assertThat(DONE_WHEN_SLOW_RETURNED.get()).isEqualTo((KEYS - 1) * PER_KEY);
    assertThat(MAX_RUNNING.get()).isBetween(2, 8);
    assertThat(stats.partitionsRevoked()).as("no rebalance").isZero();
    assertThat(stats.partitionsLost()).as("no poll timeout").isZero();
    assertThat(CALLS.keySet().stream().filter(id -> id.startsWith("ord-"))).hasSize(40);
    assertThat(
            CALLS.entrySet().stream()
                .filter(e -> e.getKey().startsWith("ord-"))
                .mapToInt(e -> e.getValue().size()))
        .as("one call per record")
        .containsOnly(1);
    List<String> k0 = STARTED.stream().filter(id -> id.startsWith("ord-k0-")).toList();
    assertThat(k0).containsExactly("ord-k0-0", "ord-k0-1", "ord-k0-2", "ord-k0-3", "ord-k0-4");
  }

  @Test
  void failuresAreRoutedThroughRetryTopicsAndTheDeadLetterTopic() throws Exception {
    List<String> ids =
        List.of("flaky-ok-1", "transient-1", "fatal-1", "always-1", "poison-1", "flaky-ok-2");
    try (KafkaProducer<String, String> producer = producer()) {
      for (String id : ids) {
        // The poison record and the record after it share a key: the key's lane must move on.
        String key = id.startsWith("poison") || id.equals("flaky-ok-2") ? "shared" : id;
        String value =
            "{\"id\":\"" + id + "\",\"merchant\":\"Flaky Ltd\",\"note\":\"" + NOTE + "\"}";
        producer.send(new ProducerRecord<>("it.flaky", key, value)).get();
      }
    }

    List<ConsumerRecord<String, String>> records =
        readUntil(
            List.of("it.flaky.out", "it.flaky.enricher-dlq"),
            rs ->
                rs.stream().filter(r -> r.topic().equals("it.flaky.out")).count() >= 3
                    && rs.stream().filter(r -> r.topic().equals("it.flaky.enricher-dlq")).count()
                        >= 3);
    Map<String, ConsumerRecord<String, String>> byId = new ConcurrentHashMap<>();
    for (ConsumerRecord<String, String> r : records) {
      byId.put(idOf(r.value()), r);
    }
    for (ConsumerRecord<String, String> r : records) {
      log.info(
          "routing: {} -> {} reason={} attempts={} calls={}",
          idOf(r.value()),
          r.topic(),
          EnricherHeaders.read(r.headers(), EnricherHeaders.FAILURE_REASON),
          EnricherHeaders.read(r.headers(), EnricherHeaders.ATTEMPTS),
          CALLS.get(idOf(r.value())).size());
    }

    assertThat(records).hasSize(6);
    assertThat(byId.get("flaky-ok-1").topic()).isEqualTo("it.flaky.out");
    assertThat(byId.get("flaky-ok-2").topic()).isEqualTo("it.flaky.out");

    ConsumerRecord<String, String> recovered = byId.get("transient-1");
    assertThat(recovered.topic()).isEqualTo("it.flaky.out");
    JsonNode source = MAPPER.readTree(recovered.value()).path("source");
    assertThat(source.path("topic").asText())
        .as("envelope points at the input")
        .isEqualTo("it.flaky");
    assertThat(source.path("offset").asLong()).isEqualTo(1);
    List<Long> transientCalls = CALLS.get("transient-1");
    assertThat(transientCalls).hasSize(5);
    assertThat(transientCalls.get(2) - transientCalls.get(1))
        .as("first retry topic waits about 1 s")
        .isGreaterThanOrEqualTo(950);
    assertThat(transientCalls.get(4) - transientCalls.get(3))
        .as("second retry topic waits about 2 s")
        .isGreaterThanOrEqualTo(1950);

    assertDeadLetter(byId.get("fatal-1"), "PROVIDER_ERROR", "1");
    assertThat(CALLS.get("fatal-1")).hasSize(1);
    assertDeadLetter(byId.get("always-1"), "RETRIES_EXHAUSTED", "3");
    assertThat(CALLS.get("always-1")).as("2 attempts x 3 stages").hasSize(6);
    assertDeadLetter(byId.get("poison-1"), "PROCESSING_ERROR", "1");
    assertThat(EnricherHeaders.read(byId.get("poison-1").headers(), EnricherHeaders.FAILURE_DETAIL))
        .contains("bug: cannot handle poison-1");
    ConsumerRecord<String, String> always = byId.get("always-1");
    assertThat(EnricherHeaders.read(always.headers(), EnricherHeaders.ORIGINAL_TOPIC))
        .isEqualTo("it.flaky");
    assertThat(EnricherHeaders.read(always.headers(), EnricherHeaders.ORIGINAL_OFFSET))
        .isEqualTo("3");

    List<ConsumerRecord<String, String>> retried =
        readUntil(
            List.of("it.flaky.enricher-retry-1", "it.flaky.enricher-retry-2"),
            rs -> rs.size() >= 4);
    assertThat(retried)
        .extracting(r -> r.topic() + ":" + idOf(r.value()))
        .containsExactlyInAnyOrder(
            "it.flaky.enricher-retry-1:transient-1",
            "it.flaky.enricher-retry-1:always-1",
            "it.flaky.enricher-retry-2:transient-1",
            "it.flaky.enricher-retry-2:always-1");
  }

  private static void assertDeadLetter(
      ConsumerRecord<String, String> record, String reason, String attempts) {
    assertThat(record.topic()).isEqualTo("it.flaky.enricher-dlq");
    assertThat(EnricherHeaders.read(record.headers(), EnricherHeaders.FAILURE_REASON))
        .isEqualTo(reason);
    assertThat(EnricherHeaders.read(record.headers(), EnricherHeaders.ATTEMPTS))
        .isEqualTo(attempts);
    assertThat(EnricherHeaders.read(record.headers(), EnricherHeaders.PIPELINE)).isEqualTo("flaky");
  }

  static String idOf(String text) {
    Matcher m = ID.matcher(text);
    if (!m.find()) {
      throw new IllegalStateException("no id in " + text);
    }
    return m.group(1);
  }

  private static List<ConsumerRecord<String, String>> readUntil(
      List<String> topics, Predicate<List<ConsumerRecord<String, String>>> done) {
    List<ConsumerRecord<String, String>> records = new ArrayList<>();
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-reader-" + String.join("-", topics));
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(topics);
      Instant deadline = Instant.now().plus(Duration.ofSeconds(90));
      while (!done.test(records) && Instant.now().isBefore(deadline)) {
        consumer.poll(Duration.ofMillis(250)).forEach(records::add);
      }
      // A little longer, so duplicates would show up.
      Instant settle = Instant.now().plus(Duration.ofSeconds(2));
      while (Instant.now().isBefore(settle)) {
        consumer.poll(Duration.ofMillis(250)).forEach(records::add);
      }
    }
    return records;
  }

  private static KafkaProducer<String, String> producer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return new KafkaProducer<>(props);
  }

  private static void sleep(Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
