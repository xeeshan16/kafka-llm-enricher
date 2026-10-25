package io.github.xeeshan16.enricher.spring;

import io.github.xeeshan16.enricher.cost.BudgetPeriod;
import io.github.xeeshan16.enricher.cost.OverBudgetAction;
import io.github.xeeshan16.enricher.kafka.OrderingMode;
import io.github.xeeshan16.enricher.llm.http.AzureOpenAiProvider;
import io.github.xeeshan16.enricher.llm.http.OpenAiCompatibleProvider.StructuredOutput;
import io.github.xeeshan16.enricher.privacy.OutputPiiAction;
import java.math.BigDecimal;
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
 * @param provider LLM provider to use when the application defines no {@code LlmProvider} bean:
 *     {@code fake}, {@code openai} (any OpenAI-compatible server, including Ollama and vLLM),
 *     {@code anthropic} or {@code azure-openai}
 * @param providers connection settings of the HTTP providers
 * @param delivery delivery guarantee towards the output topic
 * @param resilience timeout and in-place retries of each provider call
 * @param rateLimit provider quotas and adaptive concurrency
 * @param pricing USD per million tokens by model name, for cost accounting and cost budgets
 * @param budgets where tenant budget counters are kept
 * @param engine settings shared by every pipeline's consumer loops
 * @param ledger the result ledger that stops duplicate paid calls
 * @param topics optional creation of the topics the pipelines use
 * @param pipelines pipelines by name
 */
@ConfigurationProperties(prefix = "enricher")
public record EnricherProperties(
    @DefaultValue("true") boolean autoStartup,
    @DefaultValue("fake") String provider,
    @DefaultValue Providers providers,
    @DefaultValue("auto") Delivery delivery,
    @DefaultValue Resilience resilience,
    @DefaultValue RateLimit rateLimit,
    Map<String, ModelPrice> pricing,
    @DefaultValue Budgets budgets,
    @DefaultValue Engine engine,
    @DefaultValue Ledger ledger,
    @DefaultValue Topics topics,
    Map<String, Pipeline> pipelines) {

  public EnricherProperties {
    providers = providers == null ? Providers.defaults() : providers;
    delivery = delivery == null ? Delivery.AUTO : delivery;
    resilience =
        resilience == null
            ? new Resilience(
                Duration.ofSeconds(30), 3, Duration.ofMillis(200), Duration.ofSeconds(5))
            : resilience;
    rateLimit =
        rateLimit == null
            ? new RateLimit(0, 0, Duration.ofSeconds(5), 16, 1, 64, 0.5, Duration.ofSeconds(1))
            : rateLimit;
    pricing =
        pricing == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(pricing));
    budgets = budgets == null ? new Budgets(BudgetStoreKind.AUTO) : budgets;
    engine =
        engine == null
            ? new Engine(Duration.ofMillis(100), Duration.ofSeconds(10), Duration.ofSeconds(30))
            : engine;
    ledger =
        ledger == null
            ? new Ledger(
                LedgerStore.AUTO,
                Duration.ofMinutes(4),
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

  /**
   * Connection settings of the HTTP providers; only the one named by {@code enricher.provider} is
   * used.
   */
  public record Providers(
      @DefaultValue OpenAi openai,
      @DefaultValue Anthropic anthropic,
      @DefaultValue AzureOpenAi azureOpenai) {

    static Providers defaults() {
      return new Providers(null, null, null);
    }

    public Providers {
      openai =
          openai == null
              ? new OpenAi(
                  "https://api.openai.com/v1",
                  null,
                  StructuredOutput.JSON_SCHEMA,
                  "0",
                  false,
                  Duration.ofSeconds(10),
                  Duration.ofSeconds(60),
                  null)
              : openai;
      anthropic =
          anthropic == null
              ? new Anthropic(
                  "https://api.anthropic.com",
                  null,
                  "0",
                  Duration.ofSeconds(10),
                  Duration.ofSeconds(60),
                  null)
              : anthropic;
      azureOpenai =
          azureOpenai == null
              ? new AzureOpenAi(
                  null,
                  null,
                  AzureOpenAiProvider.DEFAULT_API_VERSION,
                  StructuredOutput.JSON_SCHEMA,
                  "0",
                  Duration.ofSeconds(10),
                  Duration.ofSeconds(60),
                  null)
              : azureOpenai;
    }
  }

  /**
   * An OpenAI-compatible Chat Completions server: OpenAI, Ollama ({@code
   * http://localhost:11434/v1}), vLLM ({@code http://host:8000/v1}) or a gateway.
   *
   * @param baseUrl API root, up to and including {@code /v1}
   * @param apiKey bearer token; leave empty for servers without authentication
   * @param structuredOutput {@code json-schema}, {@code json-object} or {@code none}
   * @param temperature sampling temperature, or {@code none} to leave it to the server
   * @param maxCompletionTokens send {@code max_completion_tokens} instead of {@code max_tokens}
   *     (OpenAI reasoning models)
   * @param connectTimeout connect timeout
   * @param requestTimeout limit for one HTTP exchange
   * @param headers extra headers for every request
   */
  public record OpenAi(
      @DefaultValue("https://api.openai.com/v1") String baseUrl,
      String apiKey,
      @DefaultValue("json-schema") StructuredOutput structuredOutput,
      @DefaultValue("0") String temperature,
      @DefaultValue("false") boolean maxCompletionTokens,
      @DefaultValue("10s") Duration connectTimeout,
      @DefaultValue("60s") Duration requestTimeout,
      Map<String, String> headers) {}

  /**
   * The Anthropic Messages API.
   *
   * @param baseUrl API root, without {@code /v1}
   * @param apiKey the {@code x-api-key} credential
   * @param temperature sampling temperature, or {@code none} to leave it to the server
   * @param connectTimeout connect timeout
   * @param requestTimeout limit for one HTTP exchange
   * @param headers extra headers for every request
   */
  public record Anthropic(
      @DefaultValue("https://api.anthropic.com") String baseUrl,
      String apiKey,
      @DefaultValue("0") String temperature,
      @DefaultValue("10s") Duration connectTimeout,
      @DefaultValue("60s") Duration requestTimeout,
      Map<String, String> headers) {}

  /**
   * Azure OpenAI. A pipeline's {@code model} is the deployment name.
   *
   * @param endpoint resource endpoint, for example {@code https://my-resource.openai.azure.com}
   * @param apiKey the {@code api-key} credential
   * @param apiVersion the {@code api-version} query parameter
   * @param structuredOutput {@code json-schema}, {@code json-object} or {@code none}
   * @param temperature sampling temperature, or {@code none} to leave it to the server
   * @param connectTimeout connect timeout
   * @param requestTimeout limit for one HTTP exchange
   * @param headers extra headers for every request
   */
  public record AzureOpenAi(
      String endpoint,
      String apiKey,
      @DefaultValue(AzureOpenAiProvider.DEFAULT_API_VERSION) String apiVersion,
      @DefaultValue("json-schema") StructuredOutput structuredOutput,
      @DefaultValue("0") String temperature,
      @DefaultValue("10s") Duration connectTimeout,
      @DefaultValue("60s") Duration requestTimeout,
      Map<String, String> headers) {}

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
   * Quotas of the provider, enforced in this process before each call (retries included). With N
   * replicas sharing one API key, give each about 1/N of the key's quota.
   *
   * @param requestsPerMinute requests a minute; {@code 0} for no limit
   * @param tokensPerMinute input plus output tokens a minute (estimated before the call, settled
   *     after); {@code 0} for no limit
   * @param maxWait longest a call waits to be admitted before it fails as retryable
   * @param initialConcurrency concurrent provider calls allowed at start
   * @param minConcurrency floor of the adaptive limit
   * @param maxConcurrency ceiling of the adaptive limit
   * @param decreaseFactor multiplier applied to the limit on each provider 429
   * @param decreaseCooldown minimum spacing between two decreases
   */
  public record RateLimit(
      @DefaultValue("0") int requestsPerMinute,
      @DefaultValue("0") long tokensPerMinute,
      @DefaultValue("5s") Duration maxWait,
      @DefaultValue("16") int initialConcurrency,
      @DefaultValue("1") int minConcurrency,
      @DefaultValue("64") int maxConcurrency,
      @DefaultValue("0.5") double decreaseFactor,
      @DefaultValue("1s") Duration decreaseCooldown) {}

  /**
   * Price of one model. Map keys are the model names pipelines use; put names with dots or colons
   * in brackets, for example {@code enricher.pricing.[llama3.1:8b].input-per-million}.
   *
   * @param inputPerMillion USD per million input tokens
   * @param outputPerMillion USD per million output tokens
   */
  public record ModelPrice(
      @DefaultValue("0") BigDecimal inputPerMillion,
      @DefaultValue("0") BigDecimal outputPerMillion) {}

  /** Where tenant budget counters live. */
  public enum BudgetStoreKind {
    /** PostgreSQL when a {@code DataSource} and the PostgreSQL module are present, else memory. */
    AUTO,
    /** PostgreSQL: one cap across every instance. */
    POSTGRES,
    /** Process memory: each instance counts on its own. */
    MEMORY
  }

  /**
   * Tenant budget storage, shared by every pipeline with a {@code budget} block.
   *
   * @param store where counters are kept
   */
  public record Budgets(@DefaultValue("auto") BudgetStoreKind store) {}

  /**
   * A pipeline's per-tenant budget. Caps of {@code -1} (or left out) mean no cap on that dimension.
   *
   * @param period window the caps apply to: {@code daily} or {@code monthly}, in UTC
   * @param maxTokens input plus output tokens per tenant and window
   * @param maxCostUsd cost per tenant and window, priced with {@code enricher.pricing}
   * @param tenants caps for specific tenants; a dimension a tenant leaves out inherits the default
   * @param onExceeded {@code defer}, {@code skip} or {@code downgrade}
   * @param fallbackModel the cheaper model used by {@code downgrade}
   * @param afterDowngrade {@code defer} or {@code skip}, when even the fallback model does not fit
   */
  public record Budget(
      @DefaultValue("daily") BudgetPeriod period,
      Long maxTokens,
      BigDecimal maxCostUsd,
      Map<String, TenantBudget> tenants,
      @DefaultValue("defer") OverBudgetAction onExceeded,
      String fallbackModel,
      @DefaultValue("defer") OverBudgetAction afterDowngrade) {

    public Budget {
      tenants =
          tenants == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(tenants));
      onExceeded = onExceeded == null ? OverBudgetAction.DEFER : onExceeded;
      afterDowngrade = afterDowngrade == null ? OverBudgetAction.DEFER : afterDowngrade;
      period = period == null ? BudgetPeriod.DAILY : period;
    }
  }

  /**
   * Caps of one tenant.
   *
   * @param maxTokens tokens per window; inherits the pipeline default when left out
   * @param maxCostUsd cost per window; inherits the pipeline default when left out
   */
  public record TenantBudget(Long maxTokens, BigDecimal maxCostUsd) {}

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
   * @param leaseTtl how long a worker may hold a key while calling the provider; keep it above the
   *     worst case of the first call and every repair call, each {@code callTimeout * maxAttempts}
   *     plus the rate limiter's {@code max-wait} per attempt and the back-off waits
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
      @DefaultValue("4m") Duration leaseTtl,
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
   * @param tenant SpEL expression giving the tenant a record is charged to, for example {@code
   *     event.merchantId}; records without one, or pipelines without the setting, use {@code
   *     default}
   * @param skip skip rules by name: SpEL conditions under which a record is not sent to the model,
   *     for example {@code event.amount != null and event.amount < 5}
   * @param emitSkipped write a skipped envelope ({@code "enrichment": null}) for skipped records,
   *     so the output topic keeps one record per input; {@code false} writes nothing
   * @param budget per-tenant token and cost caps; none when left out
   * @param maskPii personal data replaced with placeholders before the prompt is rendered: any of
   *     {@code pan}, {@code iban}, {@code email}, {@code phone}, or {@code none}
   * @param maskFields event fields replaced as a whole with {@code [REDACTED]}, as dot-separated
   *     paths such as {@code customer.name}
   * @param promptGuard prepend the injection guard to the system prompt
   * @param outputPii personal data found in a validated answer: {@code mask}, {@code reject} (dead
   *     letter as {@code PII_IN_OUTPUT}) or {@code allow}
   * @param repairAttempts repair prompts sent for an answer that is not JSON or breaks the schema
   *     (0 to 2); each one is a paid call
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
      @DefaultValue("512") int maxOutputTokens,
      String tenant,
      Map<String, String> skip,
      @DefaultValue("true") boolean emitSkipped,
      Budget budget,
      @DefaultValue("pan,iban,email,phone") List<String> maskPii,
      List<String> maskFields,
      @DefaultValue("true") boolean promptGuard,
      @DefaultValue("mask") OutputPiiAction outputPii,
      @DefaultValue("1") int repairAttempts) {

    public Pipeline {
      skip = skip == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(skip));
      maskPii = maskPii == null ? List.of("pan", "iban", "email", "phone") : List.copyOf(maskPii);
      maskFields = maskFields == null ? List.of() : List.copyOf(maskFields);
      outputPii = outputPii == null ? OutputPiiAction.MASK : outputPii;
    }
  }
}
