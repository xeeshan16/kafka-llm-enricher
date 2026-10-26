package io.github.xeeshan16.enricher.llm.observation;

import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.LlmRequest;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Wraps every call of a provider in a Micrometer {@link Observation}: with a meter registry that is
 * a {@code gen_ai.client.operation} timer, with a tracer a {@code chat <model>} client span that is
 * a child of whatever observation is current on the calling thread (the Kafka record being
 * processed).
 *
 * <p>The engine places it closest to the provider, inside the rate limiter and the retry layer, so
 * one observation is one HTTP exchange: admission waits are not counted as model latency, and every
 * retried attempt and every 429 is visible on its own.
 */
public final class ObservedLlmProvider implements LlmProvider {

  private final LlmProvider delegate;
  private final ObservationRegistry registry;
  private final LlmObservationConvention convention;

  public ObservedLlmProvider(LlmProvider delegate, ObservationRegistry registry) {
    this(delegate, registry, null);
  }

  /**
   * @param convention custom names and tags, or {@code null} for {@link
   *     DefaultLlmObservationConvention}
   */
  public ObservedLlmProvider(
      LlmProvider delegate, ObservationRegistry registry, LlmObservationConvention convention) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.registry = Objects.requireNonNull(registry, "registry");
    this.convention = convention;
  }

  public LlmProvider delegate() {
    return delegate;
  }

  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  public LlmResponse complete(LlmRequest request) {
    LlmCallContext context = new LlmCallContext(delegate.name(), request);
    Supplier<LlmResponse> call =
        () -> {
          LlmResponse response = delegate.complete(request);
          context.setResponse(response);
          return response;
        };
    return Observation.createNotStarted(
            convention, DefaultLlmObservationConvention.INSTANCE, () -> context, registry)
        .observe(call);
  }
}
