package io.github.xeeshan16.enricher.spring;

import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import io.github.xeeshan16.enricher.pipeline.PromptTemplate;
import io.github.xeeshan16.enricher.schema.OutputSchema;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The validated pipelines of this application, built once from {@link EnricherProperties}.
 *
 * <p>Every problem (missing topic, unreadable schema, unknown template placeholder) fails the
 * application at startup with the pipeline name in the message, instead of failing per record.
 */
public final class PipelineRegistry {

  private final List<PipelineDefinition> pipelines;
  private final Map<String, String> groupIds;
  private final Map<String, Integer> concurrency;

  PipelineRegistry(
      List<PipelineDefinition> pipelines,
      Map<String, String> groupIds,
      Map<String, Integer> concurrency) {
    this.pipelines = List.copyOf(pipelines);
    this.groupIds = Map.copyOf(groupIds);
    this.concurrency = Map.copyOf(concurrency);
  }

  public static PipelineRegistry from(EnricherProperties properties) {
    List<PipelineDefinition> definitions = new ArrayList<>();
    Map<String, String> groupIds = new HashMap<>();
    Map<String, Integer> concurrency = new HashMap<>();
    for (Map.Entry<String, EnricherProperties.Pipeline> entry : properties.pipelines().entrySet()) {
      String name = entry.getKey();
      EnricherProperties.Pipeline p = entry.getValue();
      try {
        definitions.add(toDefinition(name, p));
      } catch (RuntimeException e) {
        throw new IllegalStateException(
            "invalid pipeline 'enricher.pipelines." + name + "': " + e.getMessage(), e);
      }
      groupIds.put(
          name, p.groupId() == null || p.groupId().isBlank() ? "enricher-" + name : p.groupId());
      if (p.concurrency() < 1) {
        throw new IllegalStateException(
            "invalid pipeline 'enricher.pipelines." + name + "': concurrency must be at least 1");
      }
      concurrency.put(name, p.concurrency());
    }
    return new PipelineRegistry(definitions, groupIds, concurrency);
  }

  private static PipelineDefinition toDefinition(String name, EnricherProperties.Pipeline p) {
    if (p.outputSchema() == null) {
      throw new IllegalArgumentException("output-schema is required");
    }
    if (p.inputTopic() == null || p.inputTopic().isBlank()) {
      throw new IllegalArgumentException("input-topic is required");
    }
    String deadLetter =
        p.deadLetterTopic() == null || p.deadLetterTopic().isBlank()
            ? p.inputTopic() + ".enricher-dlq"
            : p.deadLetterTopic();
    return new PipelineDefinition(
        name,
        p.inputTopic(),
        p.outputTopic(),
        deadLetter,
        p.promptVersion(),
        p.model(),
        p.systemPrompt(),
        PromptTemplate.compile(p.userTemplate()),
        OutputSchema.parse(read(p)),
        p.maxOutputTokens());
  }

  private static String read(EnricherProperties.Pipeline p) {
    try {
      return p.outputSchema().getContentAsString(StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException("cannot read output-schema " + p.outputSchema(), e);
    }
  }

  public List<PipelineDefinition> pipelines() {
    return pipelines;
  }

  public Optional<PipelineDefinition> find(String name) {
    return pipelines.stream().filter(p -> p.name().equals(name)).findFirst();
  }

  /** Consumer group of a pipeline. */
  public String groupId(String pipeline) {
    return groupIds.get(pipeline);
  }

  /** Consumer threads of a pipeline. */
  public int concurrency(String pipeline) {
    return concurrency.getOrDefault(pipeline, 1);
  }
}
