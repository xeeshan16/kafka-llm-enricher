package io.github.xeeshan16.enricher.spring;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.fake.FakeLlmProvider;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
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
        });
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
              KafkaAdmin.NewTopics topics = context.getBean(KafkaAdmin.NewTopics.class);
              assertThat(topics.getNewTopics())
                  .extracting(t -> t.name() + "/" + t.numPartitions())
                  .containsExactly("in/6", "out/6", "in.enricher-dlq/6");
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
