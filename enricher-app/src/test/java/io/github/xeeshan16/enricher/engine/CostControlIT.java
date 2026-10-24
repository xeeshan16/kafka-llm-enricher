package io.github.xeeshan16.enricher.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.xeeshan16.enricher.cost.UsageAccounting;
import io.github.xeeshan16.enricher.kafka.EnricherHeaders;
import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.LlmThrottledException;
import io.github.xeeshan16.enricher.llm.fake.FakeLlmProvider;
import io.github.xeeshan16.enricher.llm.fake.SchemaAwareResponder;
import io.github.xeeshan16.enricher.llm.ratelimit.ProviderRateLimiter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
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
 * The day-4 cost and rate controls against a real broker and PostgreSQL, with Kafka transactions
 * on.
 *
 * <ul>
 *   <li>{@code costed}: three merchants (tenants). {@code acme} has a 0.012 USD daily cap, so it is
 *       enriched with {@code fake-1}, then downgraded to the cheaper {@code fake-mini}, then
 *       deferred until the budget resets. {@code globex} has no cap. {@code initech} sends only
 *       small amounts, which a skip rule keeps away from the model. Replays of already-enriched
 *       {@code acme} events are free ledger hits even though acme is out of budget.
 *   <li>{@code throttled}: a provider that answers 429 whenever more than 3 calls run at once, while
 *       the pipeline allows 16 in flight. The adaptive limit backs off and every record still gets
 *       enriched, none dead-lettered.
 * </ul>
 */
@Testcontainers
class CostControlIT {

  private static final Logger log = LoggerFactory.getLogger(CostControlIT.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern ID = Pattern.compile("\\\\?\"id\\\\?\":\\\\?\"([a-z0-9-]+)");
  private static final String NOTE = "I was charged twice this month, please refund the duplicate.";
  private static final long ACME_CAP_MICROS = 12_000;
  private static final int PROVIDER_CAPACITY = 3;

  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

  /** Paid calls per record id and model. */
  static final Map<String, LongAdder> CALLS = new ConcurrentHashMap<>();

  static final AtomicInteger RUNNING = new AtomicInteger();
  static final AtomicInteger MAX_RUNNING = new AtomicInteger();
  static final AtomicInteger THROTTLED = new AtomicInteger();

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
            if (id.startsWith("rl-")) {
              int now = RUNNING.incrementAndGet();
              try {
                if (now > PROVIDER_CAPACITY) {
                  THROTTLED.incrementAndGet();
                  throw new LlmThrottledException("429 injected", Duration.ofMillis(100));
                }
                MAX_RUNNING.accumulateAndGet(now, Math::max);
                sleep(Duration.ofMillis(100));
              } finally {
                RUNNING.decrementAndGet();
              }
            }
            CALLS.computeIfAbsent(id + "@" + request.model(), k -> new LongAdder()).increment();
            return responder.apply(request);
          });
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
                "--spring.kafka.producer.transaction-id-prefix=it-cost-",
                "--enricher.delivery=exactly-once",
                "--enricher.ledger.store=postgres",
                "--enricher.budgets.store=postgres",
                "--enricher.topics.partitions=1",
                "--enricher.resilience.max-attempts=8",
                "--enricher.resilience.initial-backoff=50ms",
                "--enricher.resilience.max-backoff=400ms",
                "--enricher.rate-limit.requests-per-minute=6000",
                "--enricher.rate-limit.initial-concurrency=16",
                "--enricher.rate-limit.max-concurrency=16",
                "--enricher.rate-limit.decrease-cooldown=200ms",
                "--enricher.pricing.fake-1.input-per-million=10",
                "--enricher.pricing.fake-1.output-per-million=40",
                "--enricher.pricing.fake-mini.input-per-million=5",
                "--enricher.pricing.fake-mini.output-per-million=20",
                // Cost controls: tenant from the event, a skip rule, a per-tenant daily cap.
                "--enricher.pipelines.costed.input-topic=it.costed",
                "--enricher.pipelines.costed.output-topic=it.costed.out",
                "--enricher.pipelines.costed.max-output-tokens=64",
                "--enricher.pipelines.costed.user-template=Classify {{event}}",
                "--enricher.pipelines.costed.output-schema=classpath:schemas/dispute-triage.v1.json",
                "--enricher.pipelines.costed.tenant=event.merchantId",
                "--enricher.pipelines.costed.skip.small-amount=event.amount != null and event.amount < 5",
                "--enricher.pipelines.costed.budget.max-cost-usd=0.012",
                "--enricher.pipelines.costed.budget.tenants.globex.max-cost-usd=-1",
                "--enricher.pipelines.costed.budget.on-exceeded=downgrade",
                "--enricher.pipelines.costed.budget.fallback-model=fake-mini",
                "--enricher.pipelines.costed.budget.after-downgrade=defer",
                // Adaptive concurrency against a provider that throttles above 3 concurrent calls.
                "--enricher.pipelines.throttled.input-topic=it.throttled",
                "--enricher.pipelines.throttled.output-topic=it.throttled.out",
                "--enricher.pipelines.throttled.max-in-flight=16",
                "--enricher.pipelines.throttled.retry-delays=1s,2s",
                "--enricher.pipelines.throttled.user-template=Classify {{event}}",
                "--enricher.pipelines.throttled.output-schema=classpath:schemas/dispute-triage.v1.json");
  }

  @AfterAll
  static void stopInstance() {
    if (context != null) {
      context.close();
    }
  }

  @Test
  void budgetsDowngradeThenDeferSkipRulesSkipAndReplaysStayFree() throws Exception {
    List<String> acme = new ArrayList<>();
    try (KafkaProducer<String, String> producer = producer()) {
      for (int i = 0; i < 12; i++) {
        String id = "acme-" + i;
        acme.add(id);
        send(producer, "it.costed", "acme", dispute(id, "acme", 40 + i));
      }
      for (int i = 0; i < 6; i++) {
        send(producer, "it.costed", "globex", dispute("globex-" + i, "globex", 80 + i));
        send(producer, "it.costed", "initech", dispute("initech-" + i, "initech", 2.0 + i * 0.25));
      }
    }
    List<ConsumerRecord<String, String>> first =
        readUntil(
            "costed-1",
            List.of("it.costed.out", "it.costed.enricher-deferred", "it.costed.enricher-dlq"),
            rs -> rs.size() >= 24);
    assertThat(first).hasSize(24);
    assertThat(first).noneMatch(r -> r.topic().endsWith("enricher-dlq"));

    // acme's first three events again, byte for byte: answered from the ledger at no cost.
    try (KafkaProducer<String, String> producer = producer()) {
      for (int i = 0; i < 3; i++) {
        send(producer, "it.costed", "acme", dispute("acme-" + i, "acme", 40 + i));
      }
    }
    List<ConsumerRecord<String, String>> all =
        readUntil(
            "costed-2",
            List.of("it.costed.out", "it.costed.enricher-deferred", "it.costed.enricher-dlq"),
            rs -> rs.size() >= 27);
    assertThat(all).hasSize(27);

    Map<String, Integer> outcomes = new TreeMap<>();
    long resetMillis = Instant.now().truncatedTo(ChronoUnit.DAYS).plus(1, ChronoUnit.DAYS).toEpochMilli();
    List<String> replayed = new ArrayList<>();
    for (ConsumerRecord<String, String> r : all) {
      String id = idOf(r.value());
      String tenant = EnricherHeaders.read(r.headers(), EnricherHeaders.TENANT);
      assertThat(tenant).isEqualTo(id.substring(0, id.indexOf('-')));
      String label;
      if (r.topic().equals("it.costed.enricher-deferred")) {
        assertThat(EnricherHeaders.read(r.headers(), EnricherHeaders.FAILURE_REASON))
            .isEqualTo("BUDGET_EXCEEDED");
        assertThat(EnricherHeaders.read(r.headers(), EnricherHeaders.RETRY_NOT_BEFORE))
            .isEqualTo(Long.toString(resetMillis));
        label = "deferred";
      } else {
        JsonNode envelope = MAPPER.readTree(r.value());
        String source = EnricherHeaders.read(r.headers(), EnricherHeaders.RESULT_SOURCE);
        if (envelope.get("enrichment").isNull()) {
          assertThat(envelope.path("skipped").path("reason").asText()).isEqualTo("rule:small-amount");
          label = "skipped";
        } else {
          label = "enriched:" + envelope.path("model").asText() + ":" + source;
          if ("ledger".equals(source)) {
            replayed.add(id);
          }
        }
      }
      outcomes.merge(tenant + "/" + label, 1, Integer::sum);
    }

    long acmeSpend = spendMicros("costed", "acme");
    UsageAccounting.Line total = context.getBean(UsageAccounting.class).total("costed");
    log.info(
        "cost: outcomes={} acmeSpendMicros={} acmeCapMicros={} paidCalls={} costMicros={}"
            + " ledgerHits={} savedMicros={} skipped={} deferred={} downgraded={}",
        outcomes,
        acmeSpend,
        ACME_CAP_MICROS,
        total.calls(),
        total.costMicros(),
        total.ledgerHits(),
        total.savedCostMicros(),
        total.skipped(),
        total.deferred(),
        total.downgraded());

    // initech never reached the model.
    assertThat(outcomes).containsEntry("initech/skipped", 6);
    assertThat(CALLS.keySet()).noneMatch(k -> k.startsWith("initech-"));
    // globex has no cap: everything on the primary model.
    assertThat(outcomes).containsEntry("globex/enriched:fake-1:provider", 6);
    // acme: primary model first, then the fallback, then deferral; the cap was never crossed.
    assertThat(outcomes.get("acme/enriched:fake-1:provider")).isPositive();
    assertThat(outcomes.get("acme/enriched:fake-mini:provider")).isPositive();
    assertThat(outcomes.get("acme/deferred")).isPositive();
    assertThat(acmeSpend).isPositive().isLessThanOrEqualTo(ACME_CAP_MICROS);
    // The replays were free: ledger hits, no second call for any record.
    assertThat(replayed).containsExactlyInAnyOrder("acme-0", "acme-1", "acme-2");
    assertThat(CALLS.entrySet())
        .filteredOn(e -> !e.getKey().startsWith("rl-"))
        .allSatisfy(e -> assertThat(e.getValue().sum()).as(e.getKey()).isOne());
    assertThat(total.ledgerHits()).isEqualTo(3);
    assertThat(total.skipped()).isEqualTo(6);
  }

  @Test
  void adaptiveConcurrencyBacksOffOn429sAndLosesNothing() throws Exception {
    int records = 40;
    long start = System.nanoTime();
    try (KafkaProducer<String, String> producer = producer()) {
      for (int i = 0; i < records; i++) {
        String id = "rl-" + i;
        send(producer, "it.throttled", id, dispute(id, "throttle-co", 100 + i));
      }
    }
    List<ConsumerRecord<String, String>> out =
        readUntil(
            "throttled",
            List.of("it.throttled.out", "it.throttled.enricher-dlq"),
            rs -> rs.size() >= records);
    double seconds = (System.nanoTime() - start) / 1e9;
    ProviderRateLimiter.Stats stats = context.getBean(ProviderRateLimiter.class).stats();
    log.info(
        "ratelimit: records={} seconds={} providerCapacity={} maxConcurrentServed={} throttled={}"
            + " limitDecreases={} finalLimit={} dlq={}",
        out.size(),
        String.format("%.1f", seconds),
        PROVIDER_CAPACITY,
        MAX_RUNNING.get(),
        stats.throttled(),
        stats.decreases(),
        stats.concurrencyLimit(),
        out.stream().filter(r -> r.topic().endsWith("enricher-dlq")).count());

    assertThat(out).hasSize(records);
    assertThat(out).allMatch(r -> r.topic().equals("it.throttled.out"));
    assertThat(THROTTLED.get()).isPositive();
    assertThat(stats.throttled()).isGreaterThanOrEqualTo(THROTTLED.get());
    assertThat(stats.decreases()).isPositive();
    assertThat(stats.concurrencyLimit()).isLessThan(16);
    assertThat(MAX_RUNNING.get()).isLessThanOrEqualTo(PROVIDER_CAPACITY);
  }

  private static String dispute(String id, String merchant, double amount) {
    return "{\"id\":\""
        + id
        + "\",\"merchantId\":\""
        + merchant
        + "\",\"merchant\":\""
        + merchant
        + "\",\"amount\":"
        + amount
        + ",\"note\":\""
        + NOTE
        + "\"}";
  }

  private static long spendMicros(String pipeline, String tenant) throws Exception {
    try (Connection c =
            DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        PreparedStatement ps =
            c.prepareStatement(
                "SELECT coalesce(sum(cost_micros), 0) FROM enricher_budget_usage"
                    + " WHERE pipeline = ? AND tenant = ?")) {
      ps.setString(1, pipeline);
      ps.setString(2, tenant);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  static String idOf(String text) {
    Matcher m = ID.matcher(text);
    if (!m.find()) {
      throw new IllegalStateException("no id in " + text);
    }
    return m.group(1);
  }

  private static void send(
      KafkaProducer<String, String> producer, String topic, String key, String value)
      throws Exception {
    producer.send(new ProducerRecord<>(topic, key, value)).get();
  }

  private static List<ConsumerRecord<String, String>> readUntil(
      String group, List<String> topics, Predicate<List<ConsumerRecord<String, String>>> done) {
    List<ConsumerRecord<String, String>> records = new ArrayList<>();
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-reader-" + group);
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
      throw new IllegalStateException(e);
    }
  }
}
