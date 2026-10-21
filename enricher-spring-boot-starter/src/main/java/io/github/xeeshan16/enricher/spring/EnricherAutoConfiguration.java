package io.github.xeeshan16.enricher.spring;

import io.github.xeeshan16.enricher.engine.Enricher;
import io.github.xeeshan16.enricher.llm.EmbeddingProvider;
import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.fake.FakeEmbeddingProvider;
import io.github.xeeshan16.enricher.llm.fake.FakeLlmProvider;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.kafka.clients.admin.NewTopic;
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
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Wires enrichment pipelines into a Spring Boot application.
 *
 * <p>Every bean backs off when the application defines its own, so teams can swap the provider, the
 * sink or the retry back-off without forking. Kafka clients come from Spring Boot's {@code
 * spring.kafka.*} configuration; keys and values are read and written as strings.
 */
@AutoConfiguration(after = KafkaAutoConfiguration.class)
@ConditionalOnClass(KafkaTemplate.class)
@EnableConfigurationProperties(EnricherProperties.class)
public class EnricherAutoConfiguration {

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
      Enricher enricher,
      ConsumerFactory<?, ?> consumerFactory,
      KafkaTemplate<?, ?> kafkaTemplate,
      EnrichedRecordSink sink,
      BackOff enricherRetryBackOff) {
    return new PipelineRunner(
        registry,
        enricher,
        (ConsumerFactory<String, String>) consumerFactory,
        (KafkaTemplate<String, String>) kafkaTemplate,
        sink,
        enricherRetryBackOff,
        properties.autoStartup());
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
