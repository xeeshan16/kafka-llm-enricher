package io.github.xeeshan16.enricher.spring;

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
 * @param topics optional creation of the topics the pipelines use
 * @param pipelines pipelines by name
 */
@ConfigurationProperties(prefix = "enricher")
public record EnricherProperties(
    @DefaultValue("true") boolean autoStartup,
    @DefaultValue("fake") String provider,
    @DefaultValue Topics topics,
    Map<String, Pipeline> pipelines) {

  public EnricherProperties {
    topics = topics == null ? new Topics(false, 3, (short) 1) : topics;
    pipelines =
        pipelines == null
            ? Map.of()
            : Collections.unmodifiableMap(new LinkedHashMap<>(pipelines));
  }

  /**
   * Topic creation through Spring Kafka's {@code KafkaAdmin}. Off by default: production topics
   * are usually managed elsewhere.
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
