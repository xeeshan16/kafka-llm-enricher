package io.github.xeeshan16.enricher.spring;

import io.github.xeeshan16.enricher.kafka.OrderingMode;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.core.io.Resource;

/**
 * Configuration under {@code enricher.*}.
 *
 * @param autoStartup start the pipeline consumers with the application context
 * @param provider LLM provider to use when the application defines no {@code LlmProvider} bean
 * @param delivery delivery guarantee towards the output topic
 * @param resilience timeout and in-place retries of each provider call
 * @param engine settings shared by every pipeline's consumer loops
 * @param ledger the result ledger that stops duplicate paid calls
 * @param topics optional creation of the topics the pipelines use
 * @param pipelines pipelines by name
 */
@ConfigurationProperties(prefix = "enricher")
public record EnricherProperties(
    @DefaultValue("true") boolean autoStartup,
    @DefaultValue("fake") String provider,
    @DefaultValue("auto") Delivery delivery,
    @DefaultValue Resilience resilience,
    @DefaultValue Engine engine,
    @DefaultValue Ledger ledger,
    @DefaultValue Topics topics,
    Map<String, Pipeline> pipelines) {

  public EnricherProperties {
    delivery = delivery == null ? Delivery.AUTO : delivery;
    resilience =
        resilience == null
            ? new Resilience(
                Duration.ofSeconds(30), 3, Duration.ofMillis(200), Duration.ofSeconds(5))
            : resilience;
    engine =
        engine == null
            ? new Engine(Duration.ofMillis(100), Duration.ofSeconds(10), Duration.ofSeconds(30))
            : engine;
    ledger =
        ledger == null
            ? new Ledger(
                LedgerStore.AUTO,
                Duration.ofMinutes(2),
                Duration.ofSeconds(2),
                Duration.ofMillis(200),
                Duration.ofMillis(500),
                true,
                null)
            : ledger;
    topics = topics == null ? new Topics(false, 3, (short) 1) : topics;
    pipelines =
        pipelines == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(pipelines));
  }

  /** Delivery guarantee towards the output and dead-letter topics. */
  public enum Delivery {
    /**
     * Exactly-once when Spring Boot configured a Kafka transaction manager (that is, when {@code
     * spring.kafka.producer.transaction-id-prefix} is set), at-least-once otherwise.
     */
    AUTO,
    /** Read-process-write in Kafka transactions; startup fails without a transaction manager. */
    EXACTLY_ONCE,
    /** Publish, then commit the offset. Re-deliveries can publish the same envelope again. */
    AT_LEAST_ONCE
  }

  /**
   * Timeout and in-place retries of each provider call, applied around whatever {@code LlmProvider}
   * is configured. Retryable failures that survive these attempts move to the pipeline's retry
   * topics (if any), then to the dead-letter topic.
   *
   * @param callTimeout limit for one attempt; {@code 0} disables it
   * @param maxAttempts attempts per delivery, including the first
   * @param initialBackoff ceiling of the first full-jitter wait
   * @param maxBackoff ceiling of every full-jitter wait
   */
  public record Resilience(
      @DefaultValue("30s") Duration callTimeout,
      @DefaultValue("3") int maxAttempts,
      @DefaultValue("200ms") Duration initialBackoff,
      @DefaultValue("5s") Duration maxBackoff) {}

  /**
   * Consumer loop settings shared by every pipeline.
   *
   * @param pollTimeout longest wait in {@code poll()} while nothing is running
   * @param shutdownTimeout how long a stopping loop waits for running records before committing
   *     what finished
   * @param sendTimeout how long an at-least-once commit waits for output acknowledgements
   */
  public record Engine(
      @DefaultValue("100ms") Duration pollTimeout,
      @DefaultValue("10s") Duration shutdownTimeout,
      @DefaultValue("30s") Duration sendTimeout) {}

  /** Where the result ledger lives. */
  public enum LedgerStore {
    /** PostgreSQL when a {@code DataSource} and the PostgreSQL ledger module are present. */
    AUTO,
    /** PostgreSQL: shared by every instance, survives restarts. */
    POSTGRES,
    /** Process memory: protects retries within one instance only. */
    MEMORY,
    /** No ledger: every delivery calls the provider. */
    NONE
  }

  /**
   * The result ledger.
   *
   * @param store where results and leases are kept
   * @param leaseTtl how long a worker may hold a key while calling the provider; keep it above
   *     {@code callTimeout * maxAttempts} plus the back-off waits
   * @param busyWait how long one ledger claim waits in place for another worker's lease
   * @param pollInterval how often a waiting claim re-checks a busy key
   * @param busyRetryInterval pause between claims while a key stays busy; a record waits for about
   *     {@code leaseTtl + busyWait} in total before it moves to a retry topic as {@code
   *     LEDGER_BUSY}
   * @param initializeSchema create or upgrade the PostgreSQL table at startup
   * @param owner name stored with leases for diagnostics; defaults to the host name
   */
  public record Ledger(
      @DefaultValue("auto") LedgerStore store,
      @DefaultValue("2m") Duration leaseTtl,
      @DefaultValue("2s") Duration busyWait,
      @DefaultValue("200ms") Duration pollInterval,
      @DefaultValue("500ms") Duration busyRetryInterval,
      @DefaultValue("true") boolean initializeSchema,
      String owner) {}

  /**
   * Topic creation through Spring Kafka's {@code KafkaAdmin}. Off by default: production topics are
   * usually managed elsewhere.
   *
   * @param create create missing input, output and dead-letter topics at startup
   * @param partitions partitions for created topics
   * @param replicationFactor replication factor for created topics
   */
  public record Topics(
      @DefaultValue("false") boolean create,
      @DefaultValue("3") int partitions,
      @DefaultValue("1") short replicationFactor) {}

  /**
   * One pipeline.
   *
   * @param inputTopic topic to consume (required)
   * @param outputTopic topic for enriched envelopes (required)
   * @param deadLetterTopic topic for failed records; defaults to {@code <inputTopic>.enricher-dlq}
   * @param groupId consumer group; defaults to {@code enricher-<pipeline name>}
   * @param concurrency consumers (poll loops) for this pipeline in this instance
   * @param ordering which records run one after another: {@code key} (per key within a partition),
   *     {@code partition} or {@code unordered}
   * @param maxInFlight records processed concurrently by each consumer
   * @param maxPendingPerPartition uncommitted records buffered per partition before it is paused
   * @param commitInterval how often finished records are committed, as one Kafka transaction
   * @param retryDelays delay before each retry topic ({@code <input-topic>.enricher-retry-N});
   *     empty sends exhausted retryable failures straight to the dead-letter topic
   * @param promptVersion version label of prompt plus schema; bump it when either changes meaning
   * @param model model name passed to the provider
   * @param systemPrompt system instructions
   * @param userTemplate user prompt template with {@code {{event}}} style placeholders (required)
   * @param outputSchema JSON Schema resource the answer must satisfy, for example {@code
   *     classpath:schemas/dispute-triage.v1.json} (required)
   * @param maxOutputTokens output token cap per call
   */
  public record Pipeline(
      String inputTopic,
      String outputTopic,
      String deadLetterTopic,
      String groupId,
      @DefaultValue("1") int concurrency,
      @DefaultValue("key") OrderingMode ordering,
      @DefaultValue("16") int maxInFlight,
      @DefaultValue("500") int maxPendingPerPartition,
      @DefaultValue("100ms") Duration commitInterval,
      List<Duration> retryDelays,
      @DefaultValue("v1") String promptVersion,
      @DefaultValue("fake-1") String model,
      String systemPrompt,
      String userTemplate,
      Resource outputSchema,
      @DefaultValue("512") int maxOutputTokens) {}
}
