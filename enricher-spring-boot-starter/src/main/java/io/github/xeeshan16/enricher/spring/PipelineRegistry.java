package io.github.xeeshan16.enricher.spring;

import io.github.xeeshan16.enricher.cost.OverBudgetAction;
import io.github.xeeshan16.enricher.kafka.EngineSettings;
import io.github.xeeshan16.enricher.kafka.FailureRecords;
import io.github.xeeshan16.enricher.kafka.RetryTopics;
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
 * <p>Every problem (missing topic, unreadable schema, unknown template placeholder, an invalid
 * engine setting) fails the application at startup with the pipeline name in the message, instead
 * of failing per record.
 */
public final class PipelineRegistry {

  /**
   * How one pipeline runs: consumer group, consumers, loop settings, retry stages, and whether it
   * writes skipped envelopes and consumes a deferred topic.
   *
   * @param deferredTopic the topic over-budget records are parked on, or {@code null} when the
   *     pipeline never defers
   */
  public record PipelineRuntime(
      String groupId,
      int concurrency,
      EngineSettings engine,
      RetryTopics retryTopics,
      boolean emitSkipped,
      String deferredTopic) {

    public PipelineRuntime(
        String groupId, int concurrency, EngineSettings engine, RetryTopics retryTopics) {
      this(groupId, concurrency, engine, retryTopics, true, null);
    }

    /** Input, retry and deferred topics: everything the pipeline's consumers subscribe to. */
    public List<String> subscriptions() {
      List<String> all = new ArrayList<>(retryTopics.subscriptions());
      if (deferredTopic != null) {
        all.add(deferredTopic);
      }
      return all;
    }
  }

  private final List<PipelineDefinition> pipelines;
  private final Map<String, PipelineRuntime> runtimes;

  PipelineRegistry(List<PipelineDefinition> pipelines, Map<String, PipelineRuntime> runtimes) {
    this.pipelines = List.copyOf(pipelines);
    this.runtimes = Map.copyOf(runtimes);
  }

  public static PipelineRegistry from(EnricherProperties properties) {
    List<PipelineDefinition> definitions = new ArrayList<>();
    Map<String, PipelineRuntime> runtimes = new HashMap<>();
    for (Map.Entry<String, EnricherProperties.Pipeline> entry : properties.pipelines().entrySet()) {
      String name = entry.getKey();
      EnricherProperties.Pipeline p = entry.getValue();
      try {
        PipelineDefinition definition = toDefinition(name, p);
        definitions.add(definition);
        runtimes.put(name, runtime(name, p, properties.engine()));
      } catch (RuntimeException e) {
        throw new IllegalStateException(
            "invalid pipeline 'enricher.pipelines." + name + "': " + e.getMessage(), e);
      }
    }
    return new PipelineRegistry(definitions, runtimes);
  }

  private static PipelineRuntime runtime(
      String name, EnricherProperties.Pipeline p, EnricherProperties.Engine engine) {
    if (p.concurrency() < 1) {
      throw new IllegalArgumentException("concurrency must be at least 1");
    }
    String groupId =
        p.groupId() == null || p.groupId().isBlank() ? "enricher-" + name : p.groupId();
    EngineSettings settings =
        new EngineSettings(
            p.ordering(),
            p.maxInFlight(),
            p.maxPendingPerPartition(),
            p.commitInterval(),
            engine.pollTimeout(),
            engine.shutdownTimeout(),
            engine.sendTimeout());
    RetryTopics retryTopics =
        new RetryTopics(p.inputTopic(), p.retryDelays() == null ? List.of() : p.retryDelays());
    validateBudget(p);
    String deferred =
        CostPolicies.defers(p) ? FailureRecords.deferredTopic(p.inputTopic()) : null;
    return new PipelineRuntime(
        groupId, p.concurrency(), settings, retryTopics, p.emitSkipped(), deferred);
  }

  private static void validateBudget(EnricherProperties.Pipeline p) {
    EnricherProperties.Budget b = p.budget();
    if (b == null) {
      return;
    }
    if (b.onExceeded() == OverBudgetAction.DOWNGRADE
        && (b.fallbackModel() == null || b.fallbackModel().isBlank())) {
      throw new IllegalArgumentException("budget.on-exceeded=downgrade needs budget.fallback-model");
    }
    if (b.afterDowngrade() == OverBudgetAction.DOWNGRADE) {
      throw new IllegalArgumentException("budget.after-downgrade must be defer or skip");
    }
    if (b.fallbackModel() != null && b.fallbackModel().equals(p.model())) {
      throw new IllegalArgumentException("budget.fallback-model must differ from model");
    }
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

  /** How a pipeline runs. */
  public PipelineRuntime runtime(String pipeline) {
    PipelineRuntime runtime = runtimes.get(pipeline);
    if (runtime == null) {
      throw new IllegalArgumentException("no pipeline " + pipeline);
    }
    return runtime;
  }

  /** Consumer group of a pipeline. */
  public String groupId(String pipeline) {
    return runtime(pipeline).groupId();
  }

  /** Consumers (poll loops) of a pipeline in this instance. */
  public int concurrency(String pipeline) {
    return runtime(pipeline).concurrency();
  }
}
