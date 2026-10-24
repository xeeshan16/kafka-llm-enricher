package io.github.xeeshan16.enricher.spring;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.xeeshan16.enricher.cost.BudgetLimit;
import io.github.xeeshan16.enricher.cost.BudgetPeriod;
import io.github.xeeshan16.enricher.cost.BudgetStore;
import io.github.xeeshan16.enricher.cost.CostPolicy;
import io.github.xeeshan16.enricher.cost.InMemoryBudgetStore;
import io.github.xeeshan16.enricher.cost.OverBudgetAction;
import io.github.xeeshan16.enricher.cost.PriceTable;
import io.github.xeeshan16.enricher.cost.SkipRule;
import io.github.xeeshan16.enricher.cost.TenantBudgets;
import io.github.xeeshan16.enricher.cost.UsageAccounting;
import io.github.xeeshan16.enricher.engine.Enricher;
import io.github.xeeshan16.enricher.engine.LedgeredEnricher;
import io.github.xeeshan16.enricher.kafka.OrderingMode;
import io.github.xeeshan16.enricher.ledger.InMemoryResultLedger;
import io.github.xeeshan16.enricher.ledger.NoopResultLedger;
import io.github.xeeshan16.enricher.ledger.ResultLedger;
import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.TokenUsage;
import io.github.xeeshan16.enricher.llm.fake.FakeLlmProvider;
import io.github.xeeshan16.enricher.llm.ratelimit.ProviderRateLimiter;
import io.github.xeeshan16.enricher.llm.ratelimit.RateLimitedLlmProvider;
import io.github.xeeshan16.enricher.llm.resilience.ResilientLlmProvider;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.KafkaAdmin;

class EnricherAutoConfigurationTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(
              AutoConfigurations.of(KafkaAutoConfiguration.class, EnricherAutoConfiguration.class))
          .withPropertyValues(
              "enricher.auto-startup=false",
              "enricher.pipelines.triage.input-topic=in",
              "enricher.pipelines.triage.output-topic=out",
              "enricher.pipelines.triage.user-template=Classify {{event}}",
              "enricher.pipelines.triage.output-schema=classpath:schemas/test.json");

  @Test
  void bindsPipelineWithDefaults() {
    runner.run(
        context -> {
          assertThat(context).hasNotFailed();
          assertThat(context.getBean(LlmProvider.class)).isInstanceOf(FakeLlmProvider.class);
          PipelineRegistry registry = context.getBean(PipelineRegistry.class);
          assertThat(registry.pipelines()).hasSize(1);
          PipelineDefinition pipeline = registry.pipelines().getFirst();
          assertThat(pipeline.name()).isEqualTo("triage");
          assertThat(pipeline.deadLetterTopic()).isEqualTo("in.enricher-dlq");
          assertThat(pipeline.promptVersion()).isEqualTo("v1");
          assertThat(pipeline.model()).isEqualTo("fake-1");
          assertThat(pipeline.maxOutputTokens()).isEqualTo(512);
          assertThat(registry.groupId("triage")).isEqualTo("enricher-triage");
          assertThat(registry.concurrency("triage")).isEqualTo(1);
          assertThat(context.getBean(PipelineRunner.class).isRunning()).isFalse();
          assertThat(context).doesNotHaveBean(KafkaAdmin.NewTopics.class);
          assertThat(context.getBean(ResultLedger.class)).isInstanceOf(InMemoryResultLedger.class);
          assertThat(context.getBean(PipelineRunner.class).transactional()).isFalse();
          assertThat(context.getBean(LedgeredEnricher.class).settings().leaseTtl())
              .isEqualTo(Duration.ofMinutes(2));
          PipelineRegistry.PipelineRuntime runtime = registry.runtime("triage");
          assertThat(runtime.engine().ordering()).isEqualTo(OrderingMode.KEY);
          assertThat(runtime.engine().maxInFlight()).isEqualTo(16);
          assertThat(runtime.engine().maxPendingPerPartition()).isEqualTo(500);
          assertThat(runtime.engine().commitInterval()).isEqualTo(Duration.ofMillis(100));
          assertThat(runtime.retryTopics().stages()).isZero();
          assertThat(runtime.retryTopics().subscriptions()).containsExactly("in");
          // The provider bean stays as defined; the enricher calls it through the resilience layer.
          assertThat(context.getBean(Enricher.class).provider())
              .isInstanceOfSatisfying(
                  ResilientLlmProvider.class,
                  r -> {
                    assertThat(r.delegate())
                        .isInstanceOfSatisfying(
                            RateLimitedLlmProvider.class,
                            limited -> {
                              assertThat(limited.delegate())
                                  .isSameAs(context.getBean(LlmProvider.class));
                              assertThat(limited.limiter())
                                  .isSameAs(context.getBean(ProviderRateLimiter.class));
                            });
                    assertThat(r.policy()).isEqualTo(ResilientLlmProvider.Policy.defaults());
                  });
          // No cost controls unless configured.
          assertThat(runtime.deferredTopic()).isNull();
          assertThat(runtime.emitSkipped()).isTrue();
          CostPolicy policy = context.getBean(CostPolicies.class).policy("triage");
          assertThat(policy.skipRules()).isEmpty();
          assertThat(policy.budgets()).isNull();
          assertThat(context.getBean(ProviderRateLimiter.class).limits())
              .isEqualTo(ProviderRateLimiter.Limits.defaults());
          assertThat(context.getBean(LedgeredEnricher.class).meter())
              .isSameAs(context.getBean(UsageAccounting.class));
          assertThat(context.getBean(BudgetStore.class)).isInstanceOf(InMemoryBudgetStore.class);
        });
  }

  @Test
  void bindsEngineRetryAndResilienceSettings() {
    runner
        .withPropertyValues(
            "enricher.pipelines.triage.ordering=partition",
            "enricher.pipelines.triage.max-in-flight=4",
            "enricher.pipelines.triage.max-pending-per-partition=50",
            "enricher.pipelines.triage.commit-interval=250ms",
            "enricher.pipelines.triage.concurrency=3",
            "enricher.pipelines.triage.retry-delays=5s,1m",
            "enricher.engine.shutdown-timeout=3s",
            "enricher.resilience.call-timeout=10s",
            "enricher.resilience.max-attempts=5",
            "enricher.resilience.initial-backoff=1s",
            "enricher.resilience.max-backoff=20s")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              PipelineRegistry.PipelineRuntime runtime =
                  context.getBean(PipelineRegistry.class).runtime("triage");
              assertThat(runtime.concurrency()).isEqualTo(3);
              assertThat(runtime.engine().ordering()).isEqualTo(OrderingMode.PARTITION);
              assertThat(runtime.engine().maxInFlight()).isEqualTo(4);
              assertThat(runtime.engine().maxPendingPerPartition()).isEqualTo(50);
              assertThat(runtime.engine().commitInterval()).isEqualTo(Duration.ofMillis(250));
              assertThat(runtime.engine().shutdownTimeout()).isEqualTo(Duration.ofSeconds(3));
              assertThat(runtime.retryTopics().delays())
                  .containsExactly(Duration.ofSeconds(5), Duration.ofMinutes(1));
              assertThat(runtime.retryTopics().subscriptions())
                  .containsExactly("in", "in.enricher-retry-1", "in.enricher-retry-2");
              ResilientLlmProvider provider =
                  (ResilientLlmProvider) context.getBean(Enricher.class).provider();
              assertThat(provider.policy())
                  .isEqualTo(
                      new ResilientLlmProvider.Policy(
                          Duration.ofSeconds(10),
                          5,
                          Duration.ofSeconds(1),
                          Duration.ofSeconds(20)));
            });
  }

  @Test
  void invalidEngineSettingFailsStartupWithPipelineName() {
    runner
        .withPropertyValues("enricher.pipelines.triage.max-in-flight=0")
        .run(
            context ->
                assertThat(context)
                    .hasFailed()
                    .getFailure()
                    .hasStackTraceContaining("enricher.pipelines.triage")
                    .hasStackTraceContaining("maxInFlight"));
  }

  @Test
  void transactionIdPrefixMakesDeliveryExactlyOnce() {
    runner
        .withPropertyValues("spring.kafka.producer.transaction-id-prefix=enricher-test-")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context.getBean(PipelineRunner.class).transactional()).isTrue();
            });
  }

  @Test
  void exactlyOnceWithoutTransactionsFailsStartup() {
    runner
        .withPropertyValues("enricher.delivery=exactly-once")
        .run(
            context ->
                assertThat(context)
                    .hasFailed()
                    .getFailure()
                    .rootCause()
                    .hasMessageContaining("transaction-id-prefix"));
  }

  @Test
  void atLeastOnceDoesNotUseTransactionsEvenWithAPrefix() {
    runner
        .withPropertyValues(
            "enricher.delivery=at-least-once",
            "spring.kafka.producer.transaction-id-prefix=enricher-test-")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context.getBean(PipelineRunner.class).transactional()).isFalse();
            });
  }

  @Test
  void ledgerStoreCanBeTurnedOff() {
    runner
        .withPropertyValues("enricher.ledger.store=none", "enricher.ledger.lease-ttl=30s")
        .run(
            context -> {
              assertThat(context.getBean(ResultLedger.class)).isInstanceOf(NoopResultLedger.class);
              assertThat(context.getBean(LedgeredEnricher.class).settings().leaseTtl())
                  .isEqualTo(Duration.ofSeconds(30));
            });
  }

  @Test
  void postgresLedgerWithoutDataSourceFailsStartup() {
    runner
        .withPropertyValues("enricher.ledger.store=postgres")
        .run(
            context ->
                assertThat(context)
                    .hasFailed()
                    .getFailure()
                    .rootCause()
                    .hasMessageContaining("DataSource"));
  }

  @Test
  void busyBudgetSpansOneLeaseAndOwnerDefaultsToTheConfiguredName() {
    EnricherProperties.Ledger ledger =
        new EnricherProperties.Ledger(
            EnricherProperties.LedgerStore.MEMORY,
            Duration.ofSeconds(60),
            Duration.ofSeconds(2),
            Duration.ofMillis(200),
            Duration.ofMillis(500),
            false,
            "pod-a");

    assertThat(EnricherAutoConfiguration.busyBudget(ledger)).isEqualTo(Duration.ofSeconds(62));
    assertThat(EnricherAutoConfiguration.ledgerOwner(ledger)).isEqualTo("pod-a");
  }

  @Test
  void createsTopicsWhenAsked() {
    runner
        .withPropertyValues(
            "enricher.topics.create=true",
            "enricher.topics.partitions=6",
            "spring.kafka.admin.auto-create=false")
        .run(
            context -> {
              assertThat(context).hasSingleBean(KafkaAdmin.NewTopics.class);
              assertThat(
                      EnricherAutoConfiguration.topicsToCreate(
                          context.getBean(PipelineRegistry.class),
                          context.getBean(EnricherProperties.class).topics()))
                  .extracting(t -> t.name() + "/" + t.numPartitions() + "/" + t.replicationFactor())
                  .containsExactly("in/6/1", "out/6/1", "in.enricher-dlq/6/1");
            });
  }

  @Test
  void createsRetryTopicsToo() {
    runner
        .withPropertyValues(
            "enricher.topics.create=true",
            "enricher.pipelines.triage.retry-delays=10s,1m",
            "spring.kafka.admin.auto-create=false")
        .run(
            context ->
                assertThat(
                        EnricherAutoConfiguration.topicsToCreate(
                            context.getBean(PipelineRegistry.class),
                            context.getBean(EnricherProperties.class).topics()))
                    .extracting(t -> t.name())
                    .containsExactly(
                        "in",
                        "in.enricher-retry-1",
                        "in.enricher-retry-2",
                        "out",
                        "in.enricher-dlq"));
  }

  @Test
  void bindsRateLimitPricingSkipRulesTenantAndBudget() {
    runner
        .withPropertyValues(
            "enricher.rate-limit.requests-per-minute=600",
            "enricher.rate-limit.tokens-per-minute=200000",
            "enricher.rate-limit.max-wait=2s",
            "enricher.rate-limit.initial-concurrency=8",
            "enricher.rate-limit.max-concurrency=32",
            "enricher.pricing.[gpt-4o].input-per-million=2.50",
            "enricher.pricing.[gpt-4o].output-per-million=10",
            "enricher.pricing.[llama3.1-8b].input-per-million=0",
            "enricher.pipelines.triage.model=gpt-4o",
            "enricher.pipelines.triage.tenant=event.merchantId",
            "enricher.pipelines.triage.skip.small-refund=event.amount != null and event.amount < 5",
            "enricher.pipelines.triage.skip.pings=event.type == 'PING'",
            "enricher.pipelines.triage.emit-skipped=false",
            "enricher.pipelines.triage.budget.period=monthly",
            "enricher.pipelines.triage.budget.max-cost-usd=25",
            "enricher.pipelines.triage.budget.tenants.acme.max-tokens=1000",
            "enricher.pipelines.triage.budget.on-exceeded=downgrade",
            "enricher.pipelines.triage.budget.fallback-model=llama3.1-8b",
            "enricher.pipelines.triage.budget.after-downgrade=defer")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context.getBean(ProviderRateLimiter.class).limits())
                  .isEqualTo(
                      new ProviderRateLimiter.Limits(
                          600, 200_000, Duration.ofSeconds(2), 8, 1, 32, 0.5, Duration.ofSeconds(1)));
              PriceTable prices = context.getBean(PriceTable.class);
              assertThat(prices.costMicros("gpt-4o", new TokenUsage(1000, 100))).isEqualTo(3500);
              assertThat(prices.knows("llama3.1-8b")).isTrue();

              CostPolicy policy = context.getBean(CostPolicies.class).policy("triage");
              assertThat(policy.skipRules())
                  .extracting(SkipRule::name)
                  .containsExactly("small-refund", "pings");
              assertThat(policy.onExceeded()).isEqualTo(OverBudgetAction.DOWNGRADE);
              assertThat(policy.fallbackModel()).isEqualTo("llama3.1-8b");
              assertThat(policy.defers()).isTrue();
              TenantBudgets budgets = (TenantBudgets) policy.budgets();
              assertThat(budgets.period()).isEqualTo(BudgetPeriod.MONTHLY);
              assertThat(budgets.limitFor("someone")).isEqualTo(new BudgetLimit(-1, 25_000_000));
              // A tenant entry overrides what it sets and inherits the rest.
              assertThat(budgets.limitFor("acme")).isEqualTo(new BudgetLimit(1000, 25_000_000));

              PipelineRegistry.PipelineRuntime runtime =
                  context.getBean(PipelineRegistry.class).runtime("triage");
              assertThat(runtime.emitSkipped()).isFalse();
              assertThat(runtime.deferredTopic()).isEqualTo("in.enricher-deferred");
              assertThat(runtime.subscriptions()).containsExactly("in", "in.enricher-deferred");
            });
  }

  @Test
  void skipOnExceededDoesNotSubscribeToADeferredTopic() {
    runner
        .withPropertyValues(
            "enricher.pipelines.triage.budget.max-tokens=1000",
            "enricher.pipelines.triage.budget.on-exceeded=skip",
            "enricher.topics.create=true",
            "spring.kafka.admin.auto-create=false")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context.getBean(PipelineRegistry.class).runtime("triage").deferredTopic())
                  .isNull();
              assertThat(context.getBean(CostPolicies.class).policy("triage").defers()).isFalse();
            });
  }

  @Test
  void createsTheDeferredTopicWhenTheBudgetDefers() {
    runner
        .withPropertyValues(
            "enricher.pipelines.triage.budget.max-tokens=1000",
            "enricher.topics.create=true",
            "spring.kafka.admin.auto-create=false")
        .run(
            context ->
                assertThat(
                        EnricherAutoConfiguration.topicsToCreate(
                            context.getBean(PipelineRegistry.class),
                            context.getBean(EnricherProperties.class).topics()))
                    .extracting(t -> t.name())
                    .containsExactly("in", "in.enricher-deferred", "out", "in.enricher-dlq"));
  }

  @Test
  void downgradeWithoutFallbackModelFailsStartup() {
    runner
        .withPropertyValues(
            "enricher.pipelines.triage.budget.max-tokens=1000",
            "enricher.pipelines.triage.budget.on-exceeded=downgrade")
        .run(
            context ->
                assertThat(context)
                    .hasFailed()
                    .getFailure()
                    .hasStackTraceContaining("enricher.pipelines.triage")
                    .hasStackTraceContaining("fallback-model"));
  }

  @Test
  void invalidSkipExpressionFailsStartup() {
    runner
        .withPropertyValues("enricher.pipelines.triage.skip.broken=event.amount <")
        .run(
            context ->
                assertThat(context)
                    .hasFailed()
                    .getFailure()
                    .hasStackTraceContaining("enricher.pipelines.triage")
                    .hasStackTraceContaining("skip rule 'broken'"));
  }

  @Test
  void worstCallIncludesRateLimiterWaits() {
    runner.run(
        context -> {
          EnricherProperties properties = context.getBean(EnricherProperties.class);
          // 3 attempts x (30 s timeout + 5 s admission wait) + 2 x 5 s back-off = 115 s, under the
          // default 2 minute lease.
          assertThat(EnricherAutoConfiguration.worstCall(properties))
              .isEqualTo(Duration.ofSeconds(115));
        });
  }

  @Test
  void userProviderReplacesTheFake() {
    LlmProvider custom = new FakeLlmProvider(req -> "{}");
    runner
        .withBean("myProvider", LlmProvider.class, () -> custom)
        .run(context -> assertThat(context.getBean(LlmProvider.class)).isSameAs(custom));
  }

  @Test
  void unknownProviderFailsStartup() {
    runner
        .withPropertyValues("enricher.provider=openai")
        .run(
            context ->
                assertThat(context)
                    .hasFailed()
                    .getFailure()
                    .rootCause()
                    .hasMessageContaining("enricher.provider='openai'"));
  }

  @Test
  void badTemplateFailsStartupWithPipelineName() {
    runner
        .withPropertyValues("enricher.pipelines.triage.user-template={{evnt}}")
        .run(
            context ->
                assertThat(context)
                    .hasFailed()
                    .getFailure()
                    .rootCause()
                    .hasMessageContaining("{{evnt}}"));
  }

  @Test
  void pipelineWritingToItsInputFailsStartup() {
    runner
        .withPropertyValues("enricher.pipelines.triage.output-topic=in")
        .run(
            context ->
                assertThat(context)
                    .hasFailed()
                    .getFailure()
                    .hasStackTraceContaining("enricher.pipelines.triage")
                    .hasStackTraceContaining("own input topic"));
  }
}
