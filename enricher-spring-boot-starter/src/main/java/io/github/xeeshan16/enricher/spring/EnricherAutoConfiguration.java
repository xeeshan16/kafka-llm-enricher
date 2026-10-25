package io.github.xeeshan16.enricher.spring;

import io.github.xeeshan16.enricher.cost.BudgetStore;
import io.github.xeeshan16.enricher.cost.InMemoryBudgetStore;
import io.github.xeeshan16.enricher.cost.PriceTable;
import io.github.xeeshan16.enricher.cost.UsageAccounting;
import io.github.xeeshan16.enricher.cost.UsageMeter;
import io.github.xeeshan16.enricher.engine.Enricher;
import io.github.xeeshan16.enricher.engine.LedgeredEnricher;
import io.github.xeeshan16.enricher.kafka.EnvelopeRecordFactory;
import io.github.xeeshan16.enricher.kafka.KafkaClientFactory;
import io.github.xeeshan16.enricher.kafka.OutputRecordFactory;
import io.github.xeeshan16.enricher.kafka.PropertiesKafkaClientFactory;
import io.github.xeeshan16.enricher.ledger.InMemoryResultLedger;
import io.github.xeeshan16.enricher.ledger.NoopResultLedger;
import io.github.xeeshan16.enricher.ledger.ResultLedger;
import io.github.xeeshan16.enricher.llm.EmbeddingProvider;
import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.fake.FakeEmbeddingProvider;
import io.github.xeeshan16.enricher.llm.ratelimit.ProviderRateLimiter;
import io.github.xeeshan16.enricher.llm.ratelimit.RateLimitedLlmProvider;
import io.github.xeeshan16.enricher.llm.resilience.ResilientLlmProvider;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.util.ClassUtils;

/**
 * Wires enrichment pipelines into a Spring Boot application.
 *
 * <p>Every bean backs off when the application defines its own, so teams can swap the provider, the
 * Kafka clients or the output format without forking. Kafka client settings come from Spring Boot's
 * {@code spring.kafka.*} configuration; keys and values are read and written as strings.
 *
 * <p>Delivery is exactly-once when {@code spring.kafka.producer.transaction-id-prefix} is set
 * (Spring Boot's producer factory is then transaction-capable). The result ledger is PostgreSQL
 * when a {@code DataSource} and the {@code enricher-ledger-postgres} module are present, otherwise
 * in memory; see {@code enricher.delivery} and {@code enricher.ledger.*}.
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
    return ProviderFactory.create(properties.provider(), properties.providers());
  }

  @Bean
  @ConditionalOnMissingBean
  public EmbeddingProvider enricherEmbeddingProvider() {
    return new FakeEmbeddingProvider(256);
  }

  /** Provider quotas and adaptive concurrency from {@code enricher.rate-limit.*}. */
  @Bean
  @ConditionalOnMissingBean
  public ProviderRateLimiter enricherRateLimiter(EnricherProperties properties) {
    return new ProviderRateLimiter(limits(properties.rateLimit()));
  }

  static ProviderRateLimiter.Limits limits(EnricherProperties.RateLimit r) {
    return new ProviderRateLimiter.Limits(
        r.requestsPerMinute(),
        r.tokensPerMinute(),
        r.maxWait(),
        r.initialConcurrency(),
        r.minConcurrency(),
        r.maxConcurrency(),
        r.decreaseFactor(),
        r.decreaseCooldown());
  }

  /** Model prices from {@code enricher.pricing.*}. */
  @Bean
  @ConditionalOnMissingBean
  public PriceTable enricherPriceTable(EnricherProperties properties) {
    Map<String, PriceTable.Price> prices = new LinkedHashMap<>();
    properties
        .pricing()
        .forEach(
            (model, price) ->
                prices.put(
                    model,
                    new PriceTable.Price(price.inputPerMillion(), price.outputPerMillion())));
    return new PriceTable(prices);
  }

  /** Token, cost and savings totals; replace it by defining your own {@link UsageMeter}. */
  @Bean
  @ConditionalOnMissingBean(UsageMeter.class)
  public UsageAccounting enricherUsageAccounting(PriceTable prices) {
    return new UsageAccounting(prices);
  }

  /**
   * The enricher. Each provider call goes through {@link ResilientLlmProvider} (per-call timeout
   * and full-jitter retries from {@code enricher.resilience.*}) and, inside it, {@link
   * RateLimitedLlmProvider}, so every attempt is admitted by the rate limiter and every 429 feeds
   * the adaptive concurrency limit. The {@code LlmProvider} bean itself stays unwrapped.
   */
  @Bean
  @ConditionalOnMissingBean
  public Enricher enricher(
      LlmProvider provider, EnricherProperties properties, ProviderRateLimiter rateLimiter) {
    return new Enricher(
        new ResilientLlmProvider(
            new RateLimitedLlmProvider(provider, rateLimiter), policy(properties.resilience())));
  }

  /**
   * Tenant budget counters: PostgreSQL when a {@code DataSource} and {@code
   * enricher-ledger-postgres} are present (one cap across all instances), otherwise memory.
   */
  @Bean
  @ConditionalOnMissingBean
  public BudgetStore enricherBudgetStore(
      EnricherProperties properties, ObjectProvider<DataSource> dataSources) {
    boolean postgresModule =
        ClassUtils.isPresent(PostgresLedgerSupport.LEDGER_CLASS, getClass().getClassLoader());
    DataSource dataSource = dataSources.getIfUnique();
    EnricherProperties.BudgetStoreKind kind = properties.budgets().store();
    if (kind == EnricherProperties.BudgetStoreKind.AUTO) {
      kind =
          postgresModule && dataSource != null
              ? EnricherProperties.BudgetStoreKind.POSTGRES
              : EnricherProperties.BudgetStoreKind.MEMORY;
    }
    boolean budgeted = properties.pipelines().values().stream().anyMatch(CostPolicies::hasBudget);
    if (kind == EnricherProperties.BudgetStoreKind.POSTGRES) {
      if (!postgresModule || dataSource == null) {
        throw new IllegalStateException(
            "enricher.budgets.store=postgres needs the enricher-ledger-postgres module and a single"
                + " DataSource");
      }
      if (budgeted) {
        log.info("tenant budgets: postgres");
      }
      return PostgresLedgerSupport.createBudgetStore(
          dataSource, budgeted && properties.ledger().initializeSchema());
    }
    if (budgeted) {
      log.info(
          "tenant budgets: memory (each instance counts on its own; use PostgreSQL for one cap"
              + " across instances)");
    }
    return new InMemoryBudgetStore();
  }

  /** Skip rules, tenants and budgets of every pipeline. */
  @Bean
  @ConditionalOnMissingBean
  public CostPolicies enricherCostPolicies(
      EnricherProperties properties, BudgetStore budgetStore, PriceTable prices) {
    return CostPolicies.from(properties, budgetStore, prices, Clock.systemUTC());
  }

  static ResilientLlmProvider.Policy policy(EnricherProperties.Resilience resilience) {
    return new ResilientLlmProvider.Policy(
        resilience.callTimeout(),
        resilience.maxAttempts(),
        resilience.initialBackoff(),
        resilience.maxBackoff());
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
      Enricher enricher,
      ResultLedger ledger,
      EnricherProperties properties,
      ObjectProvider<UsageMeter> meters) {
    EnricherProperties.Ledger l = properties.ledger();
    Duration worstLease = worstLease(properties);
    if (l.leaseTtl().compareTo(worstLease) < 0) {
      log.warn(
          "enricher.ledger.lease-ttl ({}) is shorter than the worst-case provider call with retries"
              + " and repair calls ({}); a lease can lapse mid-call and let a second worker pay for"
              + " the same key",
          l.leaseTtl(),
          worstLease);
    }
    return new LedgeredEnricher(
        enricher,
        ledger,
        new LedgeredEnricher.Settings(l.leaseTtl(), l.busyWait(), l.pollInterval()),
        meters.getIfUnique(() -> UsageMeter.NOOP));
  }

  /**
   * Longest one provider call can take inside a ledger lease: every attempt may wait up to the rate
   * limiter's {@code max-wait} to be admitted, then run up to its timeout, with back-off waits
   * between attempts.
   */
  static Duration worstCall(EnricherProperties properties) {
    return policy(properties.resilience())
        .worstCase()
        .plus(
            properties
                .rateLimit()
                .maxWait()
                .multipliedBy(Math.max(1, properties.resilience().maxAttempts())));
  }

  /**
   * Longest a ledger lease must last: the first call and every repair call of the pipeline with the
   * most repair attempts, each as long as {@link #worstCall}.
   */
  static Duration worstLease(EnricherProperties properties) {
    int repairs =
        properties.pipelines().values().stream()
            .mapToInt(EnricherProperties.Pipeline::repairAttempts)
            .max()
            .orElse(0);
    return worstCall(properties).multipliedBy(1L + Math.max(0, repairs));
  }

  @Bean
  @ConditionalOnMissingBean
  public PipelineRegistry enricherPipelineRegistry(EnricherProperties properties) {
    return PipelineRegistry.from(properties);
  }

  @Bean
  @ConditionalOnMissingBean
  public OutputRecordFactory enricherOutputRecordFactory() {
    return new EnvelopeRecordFactory();
  }

  /**
   * Kafka clients for the consumer loops, built from Spring Boot's consumer and producer
   * configuration ({@code spring.kafka.*}), with string (de)serialisers and manual commits forced.
   */
  @Bean
  @ConditionalOnMissingBean
  public KafkaClientFactory enricherKafkaClientFactory(
      ConsumerFactory<?, ?> consumerFactory, ProducerFactory<?, ?> producerFactory) {
    return new PropertiesKafkaClientFactory(
        consumerFactory.getConfigurationProperties(), producerFactory.getConfigurationProperties());
  }

  @Bean
  @ConditionalOnMissingBean
  public PipelineRunner enricherPipelineRunner(
      EnricherProperties properties,
      PipelineRegistry registry,
      LedgeredEnricher enricher,
      KafkaClientFactory clients,
      OutputRecordFactory outputs,
      ProducerFactory<?, ?> producerFactory,
      CostPolicies costPolicies) {
    String transactionIdPrefix =
        exactlyOnce(properties.delivery(), producerFactory)
            ? producerFactory.getTransactionIdPrefix()
            : null;
    return new PipelineRunner(
        registry,
        enricher,
        clients,
        outputs,
        transactionIdPrefix,
        properties.ledger().busyRetryInterval(),
        busyBudget(properties.ledger()),
        properties.autoStartup(),
        costPolicies);
  }

  /** Resolves the delivery mode: whether the loops use Kafka transactions. */
  static boolean exactlyOnce(EnricherProperties.Delivery delivery, ProducerFactory<?, ?> factory) {
    boolean capable = factory.transactionCapable();
    return switch (delivery) {
      case AUTO -> capable;
      case EXACTLY_ONCE -> {
        if (!capable) {
          throw new IllegalStateException(
              "enricher.delivery=exactly-once needs Kafka transactions; set"
                  + " spring.kafka.producer.transaction-id-prefix (unique per instance)");
        }
        yield true;
      }
      case AT_LEAST_ONCE -> false;
    };
  }

  /**
   * How long a record waits for a key leased by another worker before moving to a retry topic: one
   * lease lifetime plus one in-place wait, after which the lease has lapsed or been completed.
   */
  static Duration busyBudget(EnricherProperties.Ledger ledger) {
    return ledger.leaseTtl().plus(ledger.busyWait());
  }

  @Bean
  @ConditionalOnProperty(prefix = "enricher.topics", name = "create", havingValue = "true")
  public KafkaAdmin.NewTopics enricherTopics(
      EnricherProperties properties, PipelineRegistry registry) {
    return new KafkaAdmin.NewTopics(
        topicsToCreate(registry, properties.topics()).toArray(NewTopic[]::new));
  }

  /**
   * Input, retry, deferred, output and dead-letter topics of every pipeline, each once, in
   * declaration order.
   */
  static List<NewTopic> topicsToCreate(
      PipelineRegistry registry, EnricherProperties.Topics topics) {
    Set<String> names = new LinkedHashSet<>();
    for (PipelineDefinition pipeline : registry.pipelines()) {
      names.add(pipeline.inputTopic());
      PipelineRegistry.PipelineRuntime runtime = registry.runtime(pipeline.name());
      names.addAll(runtime.retryTopics().topics());
      if (runtime.deferredTopic() != null) {
        names.add(runtime.deferredTopic());
      }
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
