package io.github.xeeshan16.enricher.spring;

import io.github.xeeshan16.enricher.engine.Enricher;
import io.github.xeeshan16.enricher.engine.LedgeredEnricher;
import io.github.xeeshan16.enricher.ledger.InMemoryResultLedger;
import io.github.xeeshan16.enricher.ledger.NoopResultLedger;
import io.github.xeeshan16.enricher.ledger.ResultLedger;
import io.github.xeeshan16.enricher.llm.EmbeddingProvider;
import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.fake.FakeEmbeddingProvider;
import io.github.xeeshan16.enricher.llm.fake.FakeLlmProvider;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.transaction.KafkaAwareTransactionManager;
import org.springframework.util.ClassUtils;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Wires enrichment pipelines into a Spring Boot application.
 *
 * <p>Every bean backs off when the application defines its own, so teams can swap the provider, the
 * sink or the retry back-off without forking. Kafka clients come from Spring Boot's {@code
 * spring.kafka.*} configuration; keys and values are read and written as strings.
 *
 * <p>Delivery is exactly-once when Spring Boot configured a Kafka transaction manager ({@code
 * spring.kafka.producer.transaction-id-prefix} is set). The result ledger is PostgreSQL when a
 * {@code DataSource} and the {@code enricher-ledger-postgres} module are present, otherwise in
 * memory; see {@code enricher.delivery} and {@code enricher.ledger.*}.
 */
@AutoConfiguration(
    after = KafkaAutoConfiguration.class,
    afterName = "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration")
@ConditionalOnClass(KafkaTemplate.class)
@EnableConfigurationProperties(EnricherProperties.class)
public class EnricherAutoConfiguration {

  private static final Logger log = LoggerFactory.getLogger(EnricherAutoConfiguration.class);

  @Bean
  @ConditionalOnMissingBean
  public LlmProvider enricherLlmProvider(EnricherProperties properties) {
    if (!"fake".equals(properties.provider())) {
      throw new IllegalStateException(
          "enricher.provider='"
              + properties.provider()
              + "' is not available in this version; supported: fake");
    }
    return new FakeLlmProvider();
  }

  @Bean
  @ConditionalOnMissingBean
  public EmbeddingProvider enricherEmbeddingProvider() {
    return new FakeEmbeddingProvider(256);
  }

  @Bean
  @ConditionalOnMissingBean
  public Enricher enricher(LlmProvider provider) {
    return new Enricher(provider);
  }

  @Bean
  @ConditionalOnMissingBean
  public ResultLedger enricherResultLedger(
      EnricherProperties properties, ObjectProvider<DataSource> dataSources) {
    EnricherProperties.Ledger ledger = properties.ledger();
    boolean postgresModule =
        ClassUtils.isPresent(PostgresLedgerSupport.LEDGER_CLASS, getClass().getClassLoader());
    DataSource dataSource = dataSources.getIfUnique();
    EnricherProperties.LedgerStore store = ledger.store();
    if (store == EnricherProperties.LedgerStore.AUTO) {
      store =
          postgresModule && dataSource != null
              ? EnricherProperties.LedgerStore.POSTGRES
              : EnricherProperties.LedgerStore.MEMORY;
    }
    ResultLedger result =
        switch (store) {
          case NONE -> new NoopResultLedger();
          case MEMORY -> new InMemoryResultLedger();
          case POSTGRES -> {
            if (!postgresModule) {
              throw new IllegalStateException(
                  "enricher.ledger.store=postgres needs the enricher-ledger-postgres module on the"
                      + " classpath");
            }
            if (dataSource == null) {
              throw new IllegalStateException(
                  "enricher.ledger.store=postgres needs a single DataSource (spring.datasource.*)");
            }
            yield PostgresLedgerSupport.create(
                dataSource, ledgerOwner(ledger), ledger.initializeSchema());
          }
          case AUTO -> throw new IllegalStateException("unreachable");
        };
    log.info("result ledger: {}", store.name().toLowerCase(Locale.ROOT));
    if (store == EnricherProperties.LedgerStore.MEMORY && dataSource != null && !postgresModule) {
      log.warn(
          "a DataSource is present but enricher-ledger-postgres is not on the classpath; the"
              + " in-memory ledger does not stop duplicate paid calls across instances");
    }
    return result;
  }

  static String ledgerOwner(EnricherProperties.Ledger ledger) {
    if (ledger.owner() != null && !ledger.owner().isBlank()) {
      return ledger.owner();
    }
    String host = System.getenv("HOSTNAME");
    if (host != null && !host.isBlank()) {
      return host;
    }
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      return "unknown-host";
    }
  }

  @Bean
  @ConditionalOnMissingBean
  public LedgeredEnricher enricherLedgeredEnricher(
      Enricher enricher, ResultLedger ledger, EnricherProperties properties) {
    EnricherProperties.Ledger l = properties.ledger();
    return new LedgeredEnricher(
        enricher,
        ledger,
        new LedgeredEnricher.Settings(l.leaseTtl(), l.busyWait(), l.pollInterval()));
  }

  @Bean
  @ConditionalOnMissingBean
  public PipelineRegistry enricherPipelineRegistry(EnricherProperties properties) {
    return PipelineRegistry.from(properties);
  }

  @Bean
  @ConditionalOnMissingBean(name = "enricherRetryBackOff")
  public BackOff enricherRetryBackOff() {
    return new FixedBackOff(500L, 2L);
  }

  @Bean
  @ConditionalOnMissingBean
  @SuppressWarnings("unchecked")
  public EnrichedRecordSink enricherRecordSink(KafkaTemplate<?, ?> kafkaTemplate) {
    return new KafkaEnrichedRecordSink(
        (KafkaTemplate<String, String>) kafkaTemplate, Duration.ofSeconds(30));
  }

  @Bean
  @ConditionalOnMissingBean
  @SuppressWarnings("unchecked")
  public PipelineRunner enricherPipelineRunner(
      EnricherProperties properties,
      PipelineRegistry registry,
      LedgeredEnricher enricher,
      ConsumerFactory<?, ?> consumerFactory,
      KafkaTemplate<?, ?> kafkaTemplate,
      EnrichedRecordSink sink,
      BackOff enricherRetryBackOff,
      ObjectProvider<KafkaAwareTransactionManager<?, ?>> transactionManagers) {
    KafkaAwareTransactionManager<?, ?> transactionManager =
        transactionManager(properties.delivery(), kafkaTemplate, transactionManagers);
    return new PipelineRunner(
        registry,
        enricher,
        (ConsumerFactory<String, String>) consumerFactory,
        (KafkaTemplate<String, String>) kafkaTemplate,
        sink,
        enricherRetryBackOff,
        busyBackOff(properties.ledger()),
        transactionManager,
        properties.autoStartup());
  }

  /** Resolves the delivery mode to a transaction manager, or {@code null} for at-least-once. */
  static KafkaAwareTransactionManager<?, ?> transactionManager(
      EnricherProperties.Delivery delivery,
      KafkaTemplate<?, ?> template,
      ObjectProvider<KafkaAwareTransactionManager<?, ?>> transactionManagers) {
    KafkaAwareTransactionManager<?, ?> available = transactionManagers.getIfUnique();
    return switch (delivery) {
      case AUTO -> available;
      case EXACTLY_ONCE -> {
        if (available == null) {
          throw new IllegalStateException(
              "enricher.delivery=exactly-once needs a Kafka transaction manager; set"
                  + " spring.kafka.producer.transaction-id-prefix (unique per instance)");
        }
        yield available;
      }
      case AT_LEAST_ONCE -> {
        if (template.isTransactional() && !template.isAllowNonTransactional()) {
          throw new IllegalStateException(
              "enricher.delivery=at-least-once cannot publish with a transactional KafkaTemplate;"
                  + " remove spring.kafka.producer.transaction-id-prefix");
        }
        yield null;
      }
    };
  }

  /**
   * Back-off for records whose ledger key stays leased by another worker: keep retrying for about
   * one lease lifetime, after which the lease has lapsed and the retry can claim it.
   */
  static BackOff busyBackOff(EnricherProperties.Ledger ledger) {
    long cycleMillis = Math.max(1, ledger.busyRetryInterval().plus(ledger.busyWait()).toMillis());
    long attempts = ledger.leaseTtl().toMillis() / cycleMillis + 3;
    return new FixedBackOff(ledger.busyRetryInterval().toMillis(), attempts);
  }

  @Bean
  @ConditionalOnProperty(prefix = "enricher.topics", name = "create", havingValue = "true")
  public KafkaAdmin.NewTopics enricherTopics(
      EnricherProperties properties, PipelineRegistry registry) {
    return new KafkaAdmin.NewTopics(
        topicsToCreate(registry, properties.topics()).toArray(NewTopic[]::new));
  }

  /** Input, output and dead-letter topics of every pipeline, each once, in declaration order. */
  static List<NewTopic> topicsToCreate(
      PipelineRegistry registry, EnricherProperties.Topics topics) {
    Set<String> names = new LinkedHashSet<>();
    for (PipelineDefinition pipeline : registry.pipelines()) {
      names.add(pipeline.inputTopic());
      names.add(pipeline.outputTopic());
      names.add(pipeline.deadLetterTopic());
    }
    return names.stream()
        .map(
            name ->
                TopicBuilder.name(name)
                    .partitions(topics.partitions())
                    .replicas(topics.replicationFactor())
                    .build())
        .toList();
  }
}
