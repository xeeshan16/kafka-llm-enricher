package io.github.xeeshan16.enricher.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.xeeshan16.enricher.kafka.EngineStats;
import io.github.xeeshan16.enricher.kafka.KafkaClientFactory;
import io.github.xeeshan16.enricher.kafka.PropertiesKafkaClientFactory;
import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.fake.FakeLlmProvider;
import io.github.xeeshan16.enricher.llm.fake.SchemaAwareResponder;
import io.github.xeeshan16.enricher.spring.PipelineRunner;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
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
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Two enricher instances in one consumer group, with Kafka transactions and the PostgreSQL ledger,
 * under injected failures:
 *
 * <ul>
 *   <li><b>Crash after the result is stored but before the Kafka transaction commits</b>: the first
 *       commit attempt of every transaction that carries one of every fifth dispute's envelope
 *       throws, so the transaction aborts and its partitions are rewound and processed again.
 *   <li><b>A slow call across a rebalance</b>: the first worker to see one particular dispute hangs
 *       in the provider call for 14 s, more than twice {@code max.poll.interval.ms}. While it hangs
 *       a third instance joins the group, so partitions move mid-call. The poll loop keeps polling,
 *       so nobody is expelled; whoever owns the record next finds its key leased, waits, and
 *       publishes the stored result once the slow call returns.
 * </ul>
 *
 * <p>The assertions are the product's claims: a {@code read_committed} reader sees exactly one
 * envelope per input, the provider is called exactly once per input, and no consumer lost its
 * partitions to a poll timeout.
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
  static final CountDownLatch HANG_STARTED = new CountDownLatch(1);
  static final Set<String> CRASHED = ConcurrentHashMap.newKeySet();
  static final AtomicInteger INJECTED_CRASHES = new AtomicInteger();

  private final List<ConfigurableApplicationContext> instances = new ArrayList<>();

  /** One enricher instance: the standard auto-configuration plus the chaos provider and clients. */
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
              HANG_STARTED.countDown();
              sleep(HANG);
              log.info("chaos: provider call for {} returns", id);
            }
            return responder.apply(request);
          });
    }

    /** Real Kafka clients, with producers that fail some commits on purpose. */
    @Bean
    KafkaClientFactory chaosClients(
        ConsumerFactory<?, ?> consumerFactory, ProducerFactory<?, ?> producerFactory) {
      PropertiesKafkaClientFactory real =
          new PropertiesKafkaClientFactory(
              consumerFactory.getConfigurationProperties(),
              producerFactory.getConfigurationProperties());
      return new KafkaClientFactory() {
        @Override
        public Consumer<String, String> createConsumer(String groupId, String clientId) {
          return real.createConsumer(groupId, clientId);
        }

        @Override
        public Producer<String, String> createProducer(String clientId, String transactionalId) {
          return crashing(real.createProducer(clientId, transactionalId));
        }
      };
    }
  }

  /**
   * Wraps a transactional producer so that the first commit of a transaction containing an envelope
   * for dispute 5, 10, 15, ... throws instead of committing, as if the process died after the
   * ledger stored the result. The loop aborts the transaction and rewinds.
   */
  @SuppressWarnings("unchecked")
  static Producer<String, String> crashing(Producer<String, String> delegate) {
    List<String> keysInTransaction = new ArrayList<>();
    return (Producer<String, String>)
        Proxy.newProxyInstance(
            Producer.class.getClassLoader(),
            new Class<?>[] {Producer.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "beginTransaction" -> keysInTransaction.clear();
                case "send" ->
                    keysInTransaction.add((String) ((ProducerRecord<?, ?>) args[0]).key());
                case "commitTransaction" -> {
                  List<String> crash =
                      keysInTransaction.stream()
                          .filter(k -> Integer.parseInt(k.substring(2)) % 5 == 0)
                          .filter(k -> !CRASHED.contains(k))
                          .toList();
                  if (!crash.isEmpty()) {
                    CRASHED.addAll(crash);
                    INJECTED_CRASHES.incrementAndGet();
                    throw new KafkaException("chaos: crash before committing " + crash);
                  }
                }
                default -> {}
              }
              try {
                return method.invoke(delegate, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
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

    // Move partitions while the slow call is in flight.
    assertThat(HANG_STARTED.await(60, TimeUnit.SECONDS)).as("the slow call started").isTrue();
    log.info("chaos: starting a third instance during the slow call");
    start("c");

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
    EngineStats.Snapshot ab = stats(a).plus(stats(b));
    log.info(
        "chaos: committed={} uncommitted={} crashedKeys={} abortedTransactions={} hung={}"
            + " revoked={} lost={} calls={}",
        committed.values().stream().mapToInt(Integer::intValue).sum(),
        uncommitted,
        CRASHED.size(),
        INJECTED_CRASHES.get(),
        HUNG.get(),
        ab.partitionsRevoked(),
        ab.partitionsLost(),
        CALLS);

    assertThat(HUNG).as("the slow-call scenario ran").isTrue();
    assertThat(CRASHED).as("every fifth dispute crashed once").hasSize(DISPUTES / 5);
    assertThat(INJECTED_CRASHES.get()).isPositive();
    assertThat(ab.partitionsRevoked()).as("the third instance moved partitions").isPositive();
    assertThat(ab.partitionsLost())
        .as("a 14 s call did not expel anyone from the group (max.poll.interval.ms = 6 s)")
        .isZero();
    assertThat(committed).hasSize(DISPUTES);
    assertThat(committed.values()).as("exactly one envelope per input").containsOnly(1);
    assertThat(CALLS).hasSize(DISPUTES);
    assertThat(CALLS.values())
        .as("exactly one paid call per input")
        .allSatisfy(c -> assertThat(c.get()).isOne());
    // The aborted attempts are really in the log; read_committed readers just never see them.
    assertThat(uncommitted).isGreaterThanOrEqualTo(DISPUTES + CRASHED.size());

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
      // Every crashed record was answered from the ledger when it was processed again.
      assertThat(rs.getLong(2)).isGreaterThanOrEqualTo(CRASHED.size());
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
                "--enricher.resilience.call-timeout=30s",
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
        all &= !context.getBean(PipelineRunner.class).assignedPartitions().isEmpty();
      }
      if (all) {
        return;
      }
      Thread.sleep(250);
    }
    throw new AssertionError("instances did not both get partitions");
  }

  private static EngineStats.Snapshot stats(ConfigurableApplicationContext context) {
    return context.getBean(PipelineRunner.class).stats("dispute-triage");
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
