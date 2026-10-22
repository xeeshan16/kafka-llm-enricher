package io.github.xeeshan16.enricher.spring;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.xeeshan16.enricher.engine.LedgeredEnricher;
import io.github.xeeshan16.enricher.ledger.InMemoryResultLedger;
import io.github.xeeshan16.enricher.ledger.NoopResultLedger;
import io.github.xeeshan16.enricher.ledger.ResultLedger;
import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.fake.FakeLlmProvider;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.util.backoff.BackOffExecution;

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
        });
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
  void atLeastOnceWithTransactionalTemplateFailsStartup() {
    runner
        .withPropertyValues(
            "enricher.delivery=at-least-once",
            "spring.kafka.producer.transaction-id-prefix=enricher-test-")
        .run(
            context ->
                assertThat(context)
                    .hasFailed()
                    .getFailure()
                    .rootCause()
                    .hasMessageContaining("at-least-once"));
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
  void busyBackOffSpansAboutOneLease() {
    EnricherProperties.Ledger ledger =
        new EnricherProperties.Ledger(
            EnricherProperties.LedgerStore.MEMORY,
            Duration.ofSeconds(60),
            Duration.ofSeconds(2),
            Duration.ofMillis(200),
            Duration.ofMillis(500),
            false,
            "pod-a");
    BackOffExecution execution = EnricherAutoConfiguration.busyBackOff(ledger).start();
    int attempts = 0;
    while (execution.nextBackOff() != BackOffExecution.STOP) {
      attempts++;
    }
    // 60 s lease / (0.5 s back-off + 2 s in-place wait) = 24 cycles, plus 3 for slack.
    assertThat(attempts).isEqualTo(27);
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
