package io.github.xeeshan16.enricher.llm.resilience;

import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.LlmProviderException;
import io.github.xeeshan16.enricher.llm.LlmRequest;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import io.github.xeeshan16.enricher.llm.LlmTimeoutException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decorates an {@link LlmProvider} with a per-call timeout and bounded retries with full-jitter
 * back-off.
 *
 * <ul>
 *   <li>Each attempt runs on a virtual thread and is abandoned (interrupted) after {@link
 *       Policy#callTimeout()}; the caller sees a retryable {@link LlmTimeoutException}.
 *   <li>Retryable {@link LlmProviderException}s are retried up to {@link Policy#maxAttempts()} in
 *       total. The wait is the larger of the provider's {@code Retry-After} hint and a full-jitter
 *       draw, so a throttling provider is never hit sooner than it asked.
 *   <li>Non-retryable failures and any other exception propagate at once.
 * </ul>
 *
 * <p>Retries happen inside the caller's ledger lease, so the lease TTL must cover {@code
 * callTimeout * maxAttempts} plus the waits; {@link Policy#worstCase()} gives that bound. When
 * attempts are exhausted the last exception propagates, and the engine decides whether the record
 * goes to a retry topic or the dead-letter topic.
 */
public final class ResilientLlmProvider implements LlmProvider, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(ResilientLlmProvider.class);

  /**
   * @param callTimeout limit for one attempt; zero disables the timeout
   * @param maxAttempts attempts in total, at least 1
   * @param initialBackoff ceiling of the first jittered wait
   * @param maxBackoff ceiling of every jittered wait
   */
  public record Policy(
      Duration callTimeout, int maxAttempts, Duration initialBackoff, Duration maxBackoff) {
    public Policy {
      Objects.requireNonNull(callTimeout, "callTimeout");
      Objects.requireNonNull(initialBackoff, "initialBackoff");
      Objects.requireNonNull(maxBackoff, "maxBackoff");
      if (callTimeout.isNegative()) {
        throw new IllegalArgumentException("callTimeout must not be negative");
      }
      if (maxAttempts < 1) {
        throw new IllegalArgumentException("maxAttempts must be at least 1: " + maxAttempts);
      }
      if (initialBackoff.isNegative() || maxBackoff.compareTo(initialBackoff) < 0) {
        throw new IllegalArgumentException("need 0 <= initialBackoff <= maxBackoff");
      }
    }

    public static Policy defaults() {
      return new Policy(Duration.ofSeconds(30), 3, Duration.ofMillis(200), Duration.ofSeconds(5));
    }

    /** Upper bound of one {@link #complete} call when the provider sends no Retry-After hints. */
    public Duration worstCase() {
      return callTimeout.multipliedBy(maxAttempts).plus(maxBackoff.multipliedBy(maxAttempts - 1L));
    }
  }

  /** Pause between attempts; replaceable in tests. */
  @FunctionalInterface
  public interface Sleeper {
    void sleep(Duration duration) throws InterruptedException;
  }

  private final LlmProvider delegate;
  private final Policy policy;
  private final FullJitterBackoff backoff;
  private final Sleeper sleeper;
  private final ExecutorService callThreads = Executors.newVirtualThreadPerTaskExecutor();

  public ResilientLlmProvider(LlmProvider delegate, Policy policy) {
    this(
        delegate,
        policy,
        FullJitterBackoff.of(policy.initialBackoff(), policy.maxBackoff()),
        d -> Thread.sleep(d.toMillis()));
  }

  public ResilientLlmProvider(
      LlmProvider delegate, Policy policy, FullJitterBackoff backoff, Sleeper sleeper) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.policy = Objects.requireNonNull(policy, "policy");
    this.backoff = Objects.requireNonNull(backoff, "backoff");
    this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
  }

  public LlmProvider delegate() {
    return delegate;
  }

  public Policy policy() {
    return policy;
  }

  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  public LlmResponse complete(LlmRequest request) {
    for (int attempt = 1; ; attempt++) {
      try {
        return attempt(request);
      } catch (LlmProviderException e) {
        if (!e.retryable() || attempt >= policy.maxAttempts()) {
          throw e;
        }
        Duration wait = backoff.delay(attempt);
        if (e.retryAfter().isPresent() && e.retryAfter().get().compareTo(wait) > 0) {
          wait = e.retryAfter().get();
        }
        log.debug(
            "provider={} attempt {}/{} failed ({}); retrying in {} ms",
            delegate.name(),
            attempt,
            policy.maxAttempts(),
            e.getMessage(),
            wait.toMillis());
        pause(wait);
      }
    }
  }

  private LlmResponse attempt(LlmRequest request) {
    if (policy.callTimeout().isZero()) {
      return delegate.complete(request);
    }
    Future<LlmResponse> call = callThreads.submit(() -> delegate.complete(request));
    try {
      return call.get(policy.callTimeout().toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      call.cancel(true);
      throw new LlmTimeoutException(delegate.name(), policy.callTimeout());
    } catch (InterruptedException e) {
      call.cancel(true);
      Thread.currentThread().interrupt();
      throw new LlmProviderException("interrupted while waiting for " + delegate.name(), true, e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException runtime) {
        throw runtime;
      }
      if (cause instanceof Error error) {
        throw error;
      }
      throw new LlmProviderException(delegate.name() + " call failed", false, cause);
    }
  }

  private void pause(Duration wait) {
    if (wait.isZero()) {
      return;
    }
    try {
      sleeper.sleep(wait);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new LlmProviderException("interrupted during retry back-off", true, e);
    }
  }

  @Override
  public void close() {
    callThreads.shutdownNow();
  }
}
