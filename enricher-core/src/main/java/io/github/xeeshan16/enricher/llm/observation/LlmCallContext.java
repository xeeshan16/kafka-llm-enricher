package io.github.xeeshan16.enricher.llm.observation;

import io.github.xeeshan16.enricher.llm.LlmRequest;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import io.micrometer.observation.transport.RequestReplySenderContext;
import java.util.Objects;

/**
 * Observation context of one model call: a request-reply exchange with a remote service, so tracing
 * handlers record it as a {@code CLIENT} span. Nothing is propagated to the provider (the carrier
 * is unused); the call's trace context stays inside the enricher.
 */
public final class LlmCallContext extends RequestReplySenderContext<Object, LlmResponse> {

  private final String provider;
  private final LlmRequest request;

  public LlmCallContext(String provider, LlmRequest request) {
    super((carrier, key, value) -> {});
    this.provider = Objects.requireNonNull(provider, "provider");
    this.request = Objects.requireNonNull(request, "request");
    setRemoteServiceName(provider);
  }

  /** {@link io.github.xeeshan16.enricher.llm.LlmProvider#name()} of the called provider. */
  public String provider() {
    return provider;
  }

  public LlmRequest request() {
    return request;
  }
}
