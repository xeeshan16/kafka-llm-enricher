package io.github.xeeshan16.enricher.spring;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
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
 * @param ledger the result ledger that stops duplicate paid calls
 * @param topics optional creation of the topics the pipelines use
 * @param pipelines pipelines by name
 */
@ConfigurationProperties(prefix = "enricher")
public record EnricherProperties(
    @DefaultValue("true") boolean autoStartup,
    @DefaultValue("fake") String provider,
    @DefaultValue("auto") Delivery delivery,
    @DefaultValue Ledger ledger,
    @DefaultValue Topics topics,
    Map<String, Pipeline> pipelines) {

  public EnricherProperties {
    delivery = delivery == null ? Delivery.AUTO : delivery;
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
   * @param leaseTtl how long a worker may hold a key while calling the provider; keep it above the
   *     provider timeout including retries
   * @param busyWait how long a worker waits in place for another worker's lease before retrying the
   *     record through the error handler; keep it well below {@code max.poll.interval.ms}
   * @param pollInterval how often a waiting worker re-checks a busy key
   * @param busyRetryInterval back-off between redeliveries of a record whose key stays busy
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
   * @param concurrency consumer threads for this pipeline
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
      @DefaultValue("v1") String promptVersion,
      @DefaultValue("fake-1") String model,
      String systemPrompt,
      String userTemplate,
      Resource outputSchema,
      @DefaultValue("512") int maxOutputTokens) {}
}
