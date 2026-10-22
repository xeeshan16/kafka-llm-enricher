package io.github.xeeshan16.enricher.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.xeeshan16.enricher.engine.EnrichmentOutcome;
import io.github.xeeshan16.enricher.engine.ResultSource;
import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.fake.FakeLlmProvider;
import io.github.xeeshan16.enricher.llm.fake.SchemaAwareResponder;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import io.github.xeeshan16.enricher.spring.EnrichedRecordSink;
import io.github.xeeshan16.enricher.spring.KafkaEnrichedRecordSink;
import io.github.xeeshan16.enricher.spring.PipelineRunner;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Two enricher instances in one consumer group, with Kafka transactions and the PostgreSQL ledger,
 * under injected failures:
 *
 * <ul>
 *   <li><b>Crash after the result is stored but before the Kafka transaction commits</b>: for every
 *       fifth dispute, the first publish attempt throws after writing the envelope, so the
 *       transaction aborts and the record is redelivered.
 *   <li><b>Zombie worker and rebalance</b>: the first worker to see one particular dispute hangs in
 *       the provider call for longer than {@code max.poll.interval.ms}. It is expelled from the
 *       group, the other instance takes over its partitions, finds the key leased and waits; when
 *       the zombie's call returns it stores the result, its own commit is fenced, and the new owner
 *       publishes the stored result.
 * </ul>
 *
 * <p>The assertions are the product's claims: a {@code read_committed} reader sees exactly one
 * envelope per input, and the provider is called exactly once per input.
 */
@Testcontainers
class ExactlyOnceChaosIT {

  private static final Logger log = LoggerFactory.getLogger(ExactlyOnceChaosIT.class);

  private static final String INPUT = "payments.disputes";
  private static final String OUTPUT = "payments.disputes.enriched";
  private static final String DLQ = "payments.disputes.enricher-dlq";
  private static final int DISPUTES = 40;
  private static final String HANG_ID = "D-0013";
  private static final Duration HANG = Duration.ofSeconds(14);
  private static final Pattern DISPUTE_ID = Pattern.compile("\"disputeId\":\"(D-\\d+)\"");

  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

  /** Paid provider calls per dispute id, across both instances. */
  static final Map<String, AtomicInteger> CALLS = new ConcurrentHashMap<>();

  static final AtomicBoolean HUNG = new AtomicBoolean();
  static final Set<String> CRASHED = ConcurrentHashMap.newKeySet();
  static final AtomicInteger INJECTED_CRASHES = new AtomicInteger();

  private final List<ConfigurableApplicationContext> instances = new ArrayList<>();

  /** One enricher instance: the standard auto-configuration plus the chaos provider and sink. */
  @SpringBootConfiguration
  @EnableAutoConfiguration
  static class ChaosInstance {

    @Bean
    LlmProvider chaosProvider() {
      SchemaAwareResponder responder = new SchemaAwareResponder();
      return new FakeLlmProvider(
          request -> {
            String id = disputeId(request.userPrompt());
            CALLS.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
            if (id.equals(HANG_ID) && HUNG.compareAndSet(false, true)) {
              log.info("chaos: hanging the provider call for {} for {}", id, HANG);
              sleep(HANG);
              log.info("chaos: provider call for {} returns", id);
            }
            return responder.apply(request);
          });
    }

    @Bean
    @SuppressWarnings("unchecked")
    EnrichedRecordSink chaosSink(KafkaTemplate<?, ?> template) {
      EnrichedRecordSink delegate =
          new KafkaEnrichedRecordSink(
              (KafkaTemplate<String, String>) template, Duration.ofSeconds(30));
      return (PipelineDefinition pipeline,
          String key,
          EnrichmentOutcome.Enriched outcome,
          ResultSource source) -> {
        delegate.publish(pipeline, key, outcome, source);
        int n = Integer.parseInt(key.substring(2));
        if (n % 5 == 0 && CRASHED.add(key)) {
          INJECTED_CRASHES.incrementAndGet();
          throw new IllegalStateException("chaos: crash after publishing " + key);
        }
      };
    }
  }

  @AfterEach
  void stopInstances() {
    instances.forEach(ConfigurableApplicationContext::close);
  }

  @Test
  void exactlyOneEnvelopeAndOnePaidCallPerInputUnderCrashesAndRebalance() throws Exception {
    ConfigurableApplicationContext a = start("a");
    ConfigurableApplicationContext b = start("b");
    awaitAssignment(a, b);

    try (KafkaProducer<String, String> producer = producer()) {
      for (int i = 1; i <= DISPUTES; i++) {
        String id = String.format("D-%04d", i);
        String note =
            i % 3 == 0
                ? "I did not make this purchase, my card was used fraudulently."
                : "I was charged twice this month, please refund the duplicate charge.";
        String value =
            "{\"disputeId\":\""
                + id
                + "\",\"merchant\":\"Merchant "
                + i
                + "\",\"amount\":"
                + (10 + i)
                + ".00,\"note\":\""
                + note
                + "\"}";
        producer.send(new ProducerRecord<>(INPUT, id, value)).get();
      }
    }

    Map<String, Integer> committed = new HashMap<>();
    Instant deadline = Instant.now().plus(Duration.ofSeconds(150));
    try (KafkaConsumer<String, String> reader = consumer("read_committed", "chaos-reader")) {
      reader.subscribe(List.of(OUTPUT, DLQ));
      while (committed.size() < DISPUTES && Instant.now().isBefore(deadline)) {
        for (ConsumerRecord<String, String> r : reader.poll(Duration.ofMillis(500))) {
          assertThat(r.topic()).as("nothing may be dead-lettered").isEqualTo(OUTPUT);
          committed.merge(r.key(), 1, Integer::sum);
        }
      }
      // Keep reading a little longer so late duplicates would be caught.
      Instant settle = Instant.now().plus(Duration.ofSeconds(8));
      while (Instant.now().isBefore(settle)) {
        for (ConsumerRecord<String, String> r : reader.poll(Duration.ofMillis(500))) {
          assertThat(r.topic()).as("nothing may be dead-lettered").isEqualTo(OUTPUT);
          committed.merge(r.key(), 1, Integer::sum);
        }
      }
    }

    int uncommitted = countAll("read_uncommitted");
    log.info(
        "chaos: committed={} uncommitted={} injectedCrashes={} hung={} calls={}",
        committed.values().stream().mapToInt(Integer::intValue).sum(),
        uncommitted,
        INJECTED_CRASHES.get(),
        HUNG.get(),
        CALLS);

    assertThat(HUNG).as("the zombie scenario ran").isTrue();
    assertThat(INJECTED_CRASHES.get()).isEqualTo(DISPUTES / 5);
    assertThat(committed).hasSize(DISPUTES);
    assertThat(committed.values()).as("exactly one envelope per input").containsOnly(1);
    assertThat(CALLS).hasSize(DISPUTES);
    assertThat(CALLS.values())
        .as("exactly one paid call per input")
        .allSatisfy(c -> assertThat(c.get()).isOne());
    // The aborted attempts are really in the log; read_committed readers just never see them.
    assertThat(uncommitted).isGreaterThan(DISPUTES + INJECTED_CRASHES.get() - 1);

    try (Connection c =
            DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        Statement s = c.createStatement();
        ResultSet rs =
            s.executeQuery(
                "SELECT count(*) FILTER (WHERE status = 'COMPLETED'), coalesce(sum(hits), 0)"
                    + " FROM enricher_result_ledger")) {
      rs.next();
      assertThat(rs.getLong(1)).isEqualTo(DISPUTES);
      // Every injected crash and the zombie's record were answered from the ledger on retry.
      assertThat(rs.getLong(2)).isGreaterThanOrEqualTo(INJECTED_CRASHES.get() + 1);
      log.info("chaos: ledger completed={} hits={}", rs.getLong(1), rs.getLong(2));
    }
  }

  private ConfigurableApplicationContext start(String name) {
    ConfigurableApplicationContext context =
        new SpringApplicationBuilder(ChaosInstance.class)
            .web(WebApplicationType.NONE)
            // Command-line arguments, so they win over application.yml; default properties would
            // not.
            .run(
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + POSTGRES.getUsername(),
                "--spring.datasource.password=" + POSTGRES.getPassword(),
                "--spring.kafka.producer.transaction-id-prefix=chaos-" + name + "-",
                "--spring.kafka.consumer.properties.max.poll.interval.ms=6000",
                "--spring.kafka.consumer.max-poll-records=5",
                "--enricher.delivery=exactly-once",
                "--enricher.ledger.store=postgres",
                "--enricher.ledger.owner=instance-" + name,
                "--enricher.ledger.lease-ttl=60s",
                "--enricher.ledger.busy-wait=1s",
                "--enricher.topics.partitions=4");
    instances.add(context);
    return context;
  }

  private static void awaitAssignment(ConfigurableApplicationContext... contexts)
      throws InterruptedException {
    Instant deadline = Instant.now().plus(Duration.ofSeconds(60));
    while (Instant.now().isBefore(deadline)) {
      boolean all = true;
      for (ConfigurableApplicationContext context : contexts) {
        PipelineRunner runner = context.getBean(PipelineRunner.class);
        boolean assigned =
            runner.containers().stream()
                .anyMatch(
                    c -> c.getAssignedPartitions() != null && !c.getAssignedPartitions().isEmpty());
        all &= assigned;
      }
      if (all) {
        return;
      }
      Thread.sleep(250);
    }
    throw new AssertionError("instances did not both get partitions");
  }

  private static int countAll(String isolation) {
    int count = 0;
    try (KafkaConsumer<String, String> reader = consumer(isolation, "chaos-" + isolation)) {
      reader.subscribe(List.of(OUTPUT));
      int emptyPolls = 0;
      while (emptyPolls < 6) {
        int n = reader.poll(Duration.ofMillis(500)).count();
        count += n;
        emptyPolls = n == 0 ? emptyPolls + 1 : 0;
      }
    }
    return count;
  }

  static String disputeId(String prompt) {
    Matcher m = DISPUTE_ID.matcher(prompt);
    if (!m.find()) {
      throw new IllegalStateException("no disputeId in prompt");
    }
    return m.group(1);
  }

  static void sleep(Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static KafkaProducer<String, String> producer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return new KafkaProducer<>(props);
  }

  private static KafkaConsumer<String, String> consumer(String isolation, String group) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    return new KafkaConsumer<>(props);
  }
}
