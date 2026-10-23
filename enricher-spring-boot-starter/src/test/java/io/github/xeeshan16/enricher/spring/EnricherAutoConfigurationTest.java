package io.github.xeeshan16.enricher.spring;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.xeeshan16.enricher.engine.Enricher;
import io.github.xeeshan16.enricher.engine.LedgeredEnricher;
import io.github.xeeshan16.enricher.kafka.OrderingMode;
import io.github.xeeshan16.enricher.ledger.InMemoryResultLedger;
import io.github.xeeshan16.enricher.ledger.NoopResultLedger;
import io.github.xeeshan16.enricher.ledger.ResultLedger;
import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.fake.FakeLlmProvider;
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
          PipelineRegistry.Runtime runtime = registry.runtime("triage");
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
                    assertThat(r.delegate()).isSameAs(context.getBean(LlmProvider.class));
                    assertThat(r.policy()).isEqualTo(ResilientLlmProvider.Policy.defaults());
                  });
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
              PipelineRegistry.Runtime runtime =
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
