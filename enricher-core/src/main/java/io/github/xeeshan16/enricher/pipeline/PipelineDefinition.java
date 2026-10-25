package io.github.xeeshan16.enricher.pipeline;

import io.github.xeeshan16.enricher.privacy.PromptSafety;
import io.github.xeeshan16.enricher.schema.OutputSchema;
import java.util.Objects;

/**
 * One configured enrichment: where records come from, how they are prompted, what the answer must
 * look like and where results go.
 *
 * @param name unique pipeline name, used in headers, metrics and the consumer group
 * @param inputTopic topic to consume
 * @param outputTopic topic that receives enriched envelopes
 * @param deadLetterTopic topic that receives records that could not be enriched
 * @param promptVersion version label of the prompt and schema; part of the result identity, so bump
 *     it whenever the prompt or schema changes meaning
 * @param model model name passed to the provider
 * @param systemPrompt system instructions (may be empty)
 * @param userTemplate the user prompt template
 * @param outputSchema the schema the answer must satisfy
 * @param maxOutputTokens output token cap per call
 * @param safety PII masking, the injection guard and the handling of personal data in answers
 * @param repairAttempts how many repair prompts to send when an answer is not valid JSON or breaks
 *     the schema (0 to 2); each is a paid call
 */
public record PipelineDefinition(
    String name,
    String inputTopic,
    String outputTopic,
    String deadLetterTopic,
    String promptVersion,
    String model,
    String systemPrompt,
    PromptTemplate userTemplate,
    OutputSchema outputSchema,
    int maxOutputTokens,
    PromptSafety safety,
    int repairAttempts) {

  /** Highest supported {@code repairAttempts}. */
  public static final int MAX_REPAIR_ATTEMPTS = 2;

  /** A pipeline without masking, guard or repair. */
  public PipelineDefinition(
      String name,
      String inputTopic,
      String outputTopic,
      String deadLetterTopic,
      String promptVersion,
      String model,
      String systemPrompt,
      PromptTemplate userTemplate,
      OutputSchema outputSchema,
      int maxOutputTokens) {
    this(
        name,
        inputTopic,
        outputTopic,
        deadLetterTopic,
        promptVersion,
        model,
        systemPrompt,
        userTemplate,
        outputSchema,
        maxOutputTokens,
        PromptSafety.NONE,
        0);
  }

  public PipelineDefinition {
    requireText(name, "name");
    requireText(inputTopic, "inputTopic");
    requireText(outputTopic, "outputTopic");
    requireText(deadLetterTopic, "deadLetterTopic");
    requireText(promptVersion, "promptVersion");
    requireText(model, "model");
    systemPrompt = systemPrompt == null ? "" : systemPrompt;
    Objects.requireNonNull(userTemplate, "userTemplate");
    Objects.requireNonNull(outputSchema, "outputSchema");
    if (maxOutputTokens <= 0) {
      throw new IllegalArgumentException("maxOutputTokens must be positive: " + maxOutputTokens);
    }
    if (inputTopic.equals(outputTopic) || inputTopic.equals(deadLetterTopic)) {
      throw new IllegalArgumentException(
          "pipeline '" + name + "' must not write to its own input topic " + inputTopic);
    }
    Objects.requireNonNull(safety, "safety");
    if (repairAttempts < 0 || repairAttempts > MAX_REPAIR_ATTEMPTS) {
      throw new IllegalArgumentException(
          "repairAttempts must be between 0 and " + MAX_REPAIR_ATTEMPTS + ": " + repairAttempts);
    }
  }

  /**
   * The same pipeline asking a different model, for routing over-budget records to a cheaper one.
   * The model is part of the result-ledger key, so answers of the two models never mix.
   */
  public PipelineDefinition withModel(String otherModel) {
    return new PipelineDefinition(
        name,
        inputTopic,
        outputTopic,
        deadLetterTopic,
        promptVersion,
        otherModel,
        systemPrompt,
        userTemplate,
        outputSchema,
        maxOutputTokens,
        safety,
        repairAttempts);
  }

  /** The same pipeline with other safety settings. */
  public PipelineDefinition withSafety(PromptSafety otherSafety) {
    return new PipelineDefinition(
        name,
        inputTopic,
        outputTopic,
        deadLetterTopic,
        promptVersion,
        model,
        systemPrompt,
        userTemplate,
        outputSchema,
        maxOutputTokens,
        otherSafety,
        repairAttempts);
  }

  /** The same pipeline with another number of repair attempts. */
  public PipelineDefinition withRepairAttempts(int attempts) {
    return new PipelineDefinition(
        name,
        inputTopic,
        outputTopic,
        deadLetterTopic,
        promptVersion,
        model,
        systemPrompt,
        userTemplate,
        outputSchema,
        maxOutputTokens,
        safety,
        attempts);
  }

  private static void requireText(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
  }
}
