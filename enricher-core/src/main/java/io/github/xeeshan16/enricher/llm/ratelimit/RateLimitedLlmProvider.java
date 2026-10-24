package io.github.xeeshan16.enricher.llm.ratelimit;

import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.LlmRequest;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import io.github.xeeshan16.enricher.llm.LlmThrottledException;
import io.github.xeeshan16.enricher.llm.TokenEstimator;
import java.util.Objects;

/**
 * Sends every call through a {@link ProviderRateLimiter}. Place it inside the retry decorator
 * ({@code ResilientLlmProvider(RateLimitedLlmProvider(provider))}) so each attempt, retries
 * included, is admitted on its own and each 429 feeds the concurrency limit.
 */
public final class RateLimitedLlmProvider implements LlmProvider {

  private final LlmProvider delegate;
  private final ProviderRateLimiter limiter;

  public RateLimitedLlmProvider(LlmProvider delegate, ProviderRateLimiter limiter) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.limiter = Objects.requireNonNull(limiter, "limiter");
  }

  public LlmProvider delegate() {
    return delegate;
  }

  public ProviderRateLimiter limiter() {
    return limiter;
  }

  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  public LlmResponse complete(LlmRequest request) {
    ProviderRateLimiter.Permit permit =
        limiter.acquire(TokenEstimator.estimate(request).totalTokens());
    LlmResponse response;
    try {
      response = delegate.complete(request);
    } catch (LlmThrottledException e) {
      permit.throttled(e.retryAfter().orElse(null));
      throw e;
    } catch (RuntimeException | Error e) {
      permit.failed();
      throw e;
    }
    permit.succeeded(response.usage().totalTokens());
    return response;
  }
}
