package io.github.xeeshan16.enricher.llm.observation;

import io.github.xeeshan16.enricher.llm.LlmProviderException;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import io.github.xeeshan16.enricher.llm.LlmThrottledException;
import io.github.xeeshan16.enricher.llm.LlmTimeoutException;
import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;

/**
 * Model-call observations named and tagged after the OpenTelemetry GenAI semantic conventions.
 *
 * <ul>
 *   <li>Name {@value #NAME}: a timer {@code gen_ai.client.operation} (seconds) and a span {@code
 *       chat <model>}.
 *   <li>Low-cardinality tags (metrics and spans): {@code gen_ai.operation.name} ({@code chat}),
 *       {@code gen_ai.provider.name}, {@code gen_ai.request.model} and {@code error.type} ({@code
 *       none} on success, so every timer has the same tag keys).
 *   <li>High-cardinality tags (spans only): {@code gen_ai.request.max_tokens}, {@code
 *       gen_ai.response.model}, {@code gen_ai.usage.input_tokens} and {@code
 *       gen_ai.usage.output_tokens}.
 * </ul>
 *
 * <p>Prompts and answers are never attached: they may hold personal data even after masking, and
 * span attributes end up in tracing backends with wider access than the Kafka topics.
 */
public class DefaultLlmObservationConvention implements LlmObservationConvention {

  /** Observation, timer and span-name prefix. */
  public static final String NAME = "gen_ai.client.operation";

  public static final DefaultLlmObservationConvention INSTANCE =
      new DefaultLlmObservationConvention();

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String getContextualName(LlmCallContext context) {
    return "chat " + context.request().model();
  }

  @Override
  public KeyValues getLowCardinalityKeyValues(LlmCallContext context) {
    return KeyValues.of(
        KeyValue.of("gen_ai.operation.name", "chat"),
        KeyValue.of("gen_ai.provider.name", context.provider()),
        KeyValue.of("gen_ai.request.model", context.request().model()),
        KeyValue.of("error.type", errorType(context.getError())));
  }

  @Override
  public KeyValues getHighCardinalityKeyValues(LlmCallContext context) {
    KeyValues values =
        KeyValues.of(
            KeyValue.of(
                "gen_ai.request.max_tokens",
                Integer.toString(context.request().maxOutputTokens())));
    LlmResponse response = context.getResponse();
    if (response == null) {
      return values;
    }
    return values.and(
        KeyValue.of("gen_ai.response.model", response.model()),
        KeyValue.of("gen_ai.usage.input_tokens", Long.toString(response.usage().inputTokens())),
        KeyValue.of("gen_ai.usage.output_tokens", Long.toString(response.usage().outputTokens())));
  }

  /**
   * A short, bounded classification of a failure: {@code timeout}, {@code throttled}, {@code
   * provider_retryable}, {@code provider_error}, the exception's simple class name, or {@code
   * none}.
   */
  protected String errorType(Throwable error) {
    if (error == null) {
      return "none";
    }
    if (error instanceof LlmTimeoutException) {
      return "timeout";
    }
    if (error instanceof LlmThrottledException) {
      return "throttled";
    }
    if (error instanceof LlmProviderException provider) {
      return provider.retryable() ? "provider_retryable" : "provider_error";
    }
    return error.getClass().getSimpleName();
  }
}
