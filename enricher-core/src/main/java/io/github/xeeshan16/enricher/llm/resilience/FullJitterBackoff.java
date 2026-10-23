package io.github.xeeshan16.enricher.llm.resilience;

import java.time.Duration;
import java.util.Objects;
import java.util.function.LongUnaryOperator;

/**
 * Exponential back-off with full jitter: the wait before retry {@code n} (1-based) is drawn
 * uniformly from {@code [0, min(max, initial * 2^(n-1))]}.
 *
 * <p>Full jitter spreads the retries of many workers that failed at the same moment (a provider 503
 * or 429 burst) across the whole window instead of having them return together. See the AWS
 * Architecture Blog post "Exponential Backoff And Jitter" for the comparison with equal and
 * decorrelated jitter.
 */
public final class FullJitterBackoff {

  private final Duration initial;
  private final Duration max;
  private final LongUnaryOperator random;

  /**
   * @param initial ceiling of the first wait
   * @param max ceiling of every wait
   * @param random returns a uniform value in {@code [0, bound]} for a given inclusive bound
   */
  public FullJitterBackoff(Duration initial, Duration max, LongUnaryOperator random) {
    this.initial = Objects.requireNonNull(initial, "initial");
    this.max = Objects.requireNonNull(max, "max");
    this.random = Objects.requireNonNull(random, "random");
    if (initial.isNegative() || max.isNegative() || max.compareTo(initial) < 0) {
      throw new IllegalArgumentException(
          "need 0 <= initial <= max, got initial=" + initial + " max=" + max);
    }
  }

  /** Uses {@link java.util.concurrent.ThreadLocalRandom}. */
  public static FullJitterBackoff of(Duration initial, Duration max) {
    return new FullJitterBackoff(
        initial,
        max,
        bound ->
            bound <= 0 ? 0 : java.util.concurrent.ThreadLocalRandom.current().nextLong(bound + 1));
  }

  /** The ceiling of the wait before retry {@code retry} (1-based), before jitter. */
  public Duration ceiling(int retry) {
    if (retry < 1) {
      throw new IllegalArgumentException("retry is 1-based: " + retry);
    }
    long initialMillis = initial.toMillis();
    long maxMillis = max.toMillis();
    if (initialMillis == 0) {
      return Duration.ZERO;
    }
    int shift = retry - 1;
    if (shift >= 62 || initialMillis > (maxMillis >> shift)) {
      return max;
    }
    return Duration.ofMillis(initialMillis << shift);
  }

  /** A jittered wait before retry {@code retry} (1-based). */
  public Duration delay(int retry) {
    long bound = ceiling(retry).toMillis();
    long value = random.applyAsLong(bound);
    return Duration.ofMillis(Math.max(0, Math.min(bound, value)));
  }
}
