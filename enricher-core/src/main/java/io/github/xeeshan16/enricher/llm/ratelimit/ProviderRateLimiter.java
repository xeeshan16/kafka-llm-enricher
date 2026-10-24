package io.github.xeeshan16.enricher.llm.ratelimit;

import io.github.xeeshan16.enricher.llm.LlmProviderException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * Keeps calls to one provider inside its quotas: a requests-per-minute bucket, a tokens-per-minute
 * bucket and an {@link AimdLimiter} on concurrency that backs off when the provider answers 429.
 *
 * <p>Token usage is not known before a call, so each call reserves an estimate (prompt tokens plus
 * the output cap) and {@link Permit#succeeded} settles the difference against the usage the provider
 * reported. Over-estimating is deliberate: it errs on the side of staying under the provider's TPM.
 *
 * <p>A call that would have to wait longer than {@link Limits#maxWait()} is refused with a retryable
 * {@link LlmProviderException} carrying the expected wait as its retry-after, so the caller's retry
 * policy (and, after that, the retry topics) take over instead of a worker sleeping indefinitely.
 *
 * <p>The limits are per process. With N replicas sharing one provider key, configure each with
 * roughly 1/N of the key's quota.
 */
public final class ProviderRateLimiter {

  /**
   * @param requestsPerMinute requests a minute; {@code 0} for no limit
   * @param tokensPerMinute input plus output tokens a minute; {@code 0} for no limit
   * @param maxWait longest a call may wait for the buckets and a concurrency slot together
   * @param initialConcurrency concurrent calls allowed at start
   * @param minConcurrency floor the AIMD limit never goes below
   * @param maxConcurrency ceiling the AIMD limit never goes above
   * @param decreaseFactor multiplier applied to the concurrency limit on a 429, in (0, 1)
   * @param decreaseCooldown minimum spacing between two decreases
   */
  public record Limits(
      int requestsPerMinute,
      long tokensPerMinute,
      Duration maxWait,
      int initialConcurrency,
      int minConcurrency,
      int maxConcurrency,
      double decreaseFactor,
      Duration decreaseCooldown) {

    public Limits {
      if (requestsPerMinute < 0 || tokensPerMinute < 0) {
        throw new IllegalArgumentException("rate limits must not be negative");
      }
      Objects.requireNonNull(maxWait, "maxWait");
      Objects.requireNonNull(decreaseCooldown, "decreaseCooldown");
      if (maxWait.isNegative() || decreaseCooldown.isNegative()) {
        throw new IllegalArgumentException("maxWait and decreaseCooldown must not be negative");
      }
    }

    /** No request or token limit, concurrency between 1 and 64 starting at 16. */
    public static Limits defaults() {
      return new Limits(0, 0, Duration.ofSeconds(5), 16, 1, 64, 0.5, Duration.ofSeconds(1));
    }
  }

  /** One admitted call. Report its outcome exactly once. */
  public interface Permit {
    /** The call succeeded and used {@code actualTokens} input plus output tokens. */
    void succeeded(long actualTokens);

    /** The provider throttled the call, optionally asking for a pause. */
    void throttled(Duration retryAfter);

    /** The call failed for another reason; its reservation is returned. */
    void failed();
  }

  /** Pause before a reserved call may start; replaceable in tests. */
  @FunctionalInterface
  public interface Sleeper {
    void sleep(Duration duration) throws InterruptedException;
  }

  /**
   * Counters for metrics.
   *
   * @param admitted calls let through
   * @param rejected calls refused because the wait would exceed {@code maxWait}
   * @param throttled calls the provider answered with a throttling error
   * @param decreases times the concurrency limit was cut
   * @param waitedNanos total time admitted calls waited for the buckets
   * @param concurrencyLimit the current AIMD limit
   * @param inFlight calls running now
   */
  public record Stats(
      long admitted,
      long rejected,
      long throttled,
      long decreases,
      long waitedNanos,
      int concurrencyLimit,
      int inFlight) {}

  private final Limits limits;
  private final TokenBucket requests;
  private final TokenBucket tokens;
  private final AimdLimiter concurrency;
  private final LongSupplier nanoTime;
  private final Sleeper sleeper;
  private final LongAdder admitted = new LongAdder();
  private final LongAdder rejected = new LongAdder();
  private final LongAdder throttled = new LongAdder();
  private final LongAdder decreases = new LongAdder();
  private final AtomicLong waitedNanos = new AtomicLong();

  public ProviderRateLimiter(Limits limits) {
    this(limits, System::nanoTime, Thread::sleep);
  }

  public ProviderRateLimiter(Limits limits, LongSupplier nanoTime, Sleeper sleeper) {
    this.limits = Objects.requireNonNull(limits, "limits");
    this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    this.requests =
        limits.requestsPerMinute() > 0
            ? TokenBucket.perMinute(limits.requestsPerMinute(), nanoTime)
            : null;
    this.tokens =
        limits.tokensPerMinute() > 0 ? TokenBucket.perMinute(limits.tokensPerMinute(), nanoTime) : null;
    this.concurrency =
        new AimdLimiter(
            limits.initialConcurrency(),
            limits.minConcurrency(),
            limits.maxConcurrency(),
            limits.decreaseFactor(),
            limits.decreaseCooldown().toNanos(),
            nanoTime);
  }

  public Limits limits() {
    return limits;
  }

  /**
   * Admits one call expected to use about {@code estimatedTokens} tokens, waiting for the buckets
   * and a concurrency slot as needed.
   *
   * @throws LlmProviderException (retryable, with a retry-after) when admission would take longer
   *     than {@link Limits#maxWait()}, or when interrupted
   */
  public Permit acquire(long estimatedTokens) {
    long maxWait = limits.maxWait().toNanos();
    long requestWait = requests == null ? 0 : requests.tryReserve(1, maxWait);
    if (requestWait < 0) {
      throw refused("requests-per-minute", requests, 1);
    }
    long tokenWait = tokens == null ? 0 : tokens.tryReserve(estimatedTokens, maxWait);
    if (tokenWait < 0) {
      refundBuckets(estimatedTokens, true, false);
      throw refused("tokens-per-minute", tokens, estimatedTokens);
    }
    long wait = Math.max(requestWait, tokenWait);
    long started = nanoTime.getAsLong();
    try {
      if (wait > 0) {
        sleeper.sleep(Duration.ofNanos(wait));
      }
      long left = maxWait - (nanoTime.getAsLong() - started);
      if (!concurrency.acquire(Math.max(0, left))) {
        refundBuckets(estimatedTokens, true, true);
        rejected.increment();
        throw new LlmProviderException(
            "local rate limit: no concurrency slot within "
                + limits.maxWait().toMillis()
                + " ms (limit "
                + concurrency.currentLimit()
                + ")",
            true,
            Duration.ofMillis(Math.max(1, limits.maxWait().toMillis() / 2)),
            null);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      refundBuckets(estimatedTokens, true, true);
      throw new LlmProviderException("interrupted while waiting for the rate limiter", true, e);
    }
    waitedNanos.addAndGet(nanoTime.getAsLong() - started);
    admitted.increment();
    return new AdmittedCall(estimatedTokens);
  }

  public Stats stats() {
    return new Stats(
        admitted.sum(),
        rejected.sum(),
        throttled.sum(),
        decreases.sum(),
        waitedNanos.get(),
        concurrency.currentLimit(),
        concurrency.inFlight());
  }

  private LlmProviderException refused(String what, TokenBucket bucket, long permits) {
    rejected.increment();
    double missing = permits - bucket.available();
    long minutesNanos = Duration.ofMinutes(1).toNanos();
    long hint = (long) Math.ceil(Math.max(0, missing) * minutesNanos / bucket.capacity());
    return new LlmProviderException(
        "local rate limit: "
            + what
            + " would need a wait longer than "
            + limits.maxWait().toMillis()
            + " ms",
        true,
        Duration.ofNanos(Math.max(Duration.ofMillis(1).toNanos(), hint)),
        null);
  }

  private void refundBuckets(long estimatedTokens, boolean request, boolean token) {
    if (request && requests != null) {
      requests.refund(1);
    }
    if (token && tokens != null) {
      tokens.refund(estimatedTokens);
    }
  }

  private final class AdmittedCall implements Permit {
    private final long estimatedTokens;
    private boolean done;

    AdmittedCall(long estimatedTokens) {
      this.estimatedTokens = estimatedTokens;
    }

    @Override
    public void succeeded(long actualTokens) {
      if (finish()) {
        if (tokens != null) {
          long delta = actualTokens - estimatedTokens;
          if (delta > 0) {
            tokens.charge(delta);
          } else {
            tokens.refund(-delta);
          }
        }
        concurrency.onSuccess();
      }
    }

    @Override
    public void throttled(Duration retryAfter) {
      if (finish()) {
        throttled.increment();
        if (concurrency.onThrottle()) {
          decreases.increment();
        }
        if (retryAfter != null && retryAfter.isPositive()) {
          long until = nanoTime.getAsLong() + retryAfter.toNanos();
          if (requests != null) {
            requests.blockUntil(until);
          }
          if (tokens != null) {
            tokens.blockUntil(until);
          }
        }
      }
    }

    @Override
    public void failed() {
      if (finish()) {
        refundBuckets(estimatedTokens, false, true);
      }
    }

    private boolean finish() {
      synchronized (this) {
        if (done) {
          return false;
        }
        done = true;
      }
      concurrency.release();
      return true;
    }
  }
}
