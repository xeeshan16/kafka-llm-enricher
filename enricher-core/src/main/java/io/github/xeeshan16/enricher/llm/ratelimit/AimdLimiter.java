package io.github.xeeshan16.enricher.llm.ratelimit;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * A concurrency limit that adapts to the provider: additive increase, multiplicative decrease
 * (AIMD), the scheme TCP uses for congestion control.
 *
 * <ul>
 *   <li>Every successful call raises the limit by {@code 1 / limit}, so it grows by about one per
 *       round of {@code limit} successes, up to {@code max}.
 *   <li>A throttled call (HTTP 429) multiplies it by {@code decreaseFactor}, down to {@code min}.
 *       Decreases are spaced by {@code cooldown}, so a burst of 429s from calls that were already
 *       in flight counts as one signal instead of collapsing the limit to the floor.
 * </ul>
 *
 * <p>Callers that find the limit reached wait for a slot. Thread-safe; waits on a {@link
 * ReentrantLock} condition so virtual threads are not pinned.
 */
public final class AimdLimiter {

  private final int min;
  private final int max;
  private final double decreaseFactor;
  private final long cooldownNanos;
  private final LongSupplier nanoTime;
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition slotFreed = lock.newCondition();
  private double limit;
  private int inFlight;
  private long lastDecrease;
  private boolean decreasedOnce;

  public AimdLimiter(
      int initial,
      int min,
      int max,
      double decreaseFactor,
      long cooldownNanos,
      LongSupplier nanoTime) {
    if (min < 1 || max < min || initial < min || initial > max) {
      throw new IllegalArgumentException(
          "need 1 <= min <= initial <= max, got min="
              + min
              + " initial="
              + initial
              + " max="
              + max);
    }
    if (!(decreaseFactor > 0 && decreaseFactor < 1)) {
      throw new IllegalArgumentException("decreaseFactor must be in (0, 1): " + decreaseFactor);
    }
    if (cooldownNanos < 0) {
      throw new IllegalArgumentException("cooldown must not be negative");
    }
    this.min = min;
    this.max = max;
    this.decreaseFactor = decreaseFactor;
    this.cooldownNanos = cooldownNanos;
    this.nanoTime = nanoTime;
    this.limit = initial;
  }

  /**
   * Takes a slot, waiting up to {@code timeoutNanos} for one.
   *
   * @return {@code false} when no slot freed up in time
   */
  public boolean acquire(long timeoutNanos) throws InterruptedException {
    long remaining = timeoutNanos;
    lock.lock();
    try {
      while (inFlight >= currentLimit()) {
        if (remaining <= 0) {
          return false;
        }
        remaining = slotFreed.awaitNanos(remaining);
      }
      inFlight++;
      return true;
    } finally {
      lock.unlock();
    }
  }

  /** Gives a slot back. */
  public void release() {
    lock.lock();
    try {
      if (inFlight > 0) {
        inFlight--;
      }
      slotFreed.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /** A call succeeded: grow the limit additively. */
  public void onSuccess() {
    lock.lock();
    try {
      limit = Math.min(max, limit + 1.0 / limit);
      slotFreed.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /**
   * A call was throttled: shrink the limit multiplicatively, unless the last decrease was less than
   * one cooldown ago.
   *
   * @return whether the limit was decreased
   */
  public boolean onThrottle() {
    lock.lock();
    try {
      long now = nanoTime.getAsLong();
      if (decreasedOnce && now - lastDecrease < cooldownNanos) {
        return false;
      }
      limit = Math.max(min, limit * decreaseFactor);
      lastDecrease = now;
      decreasedOnce = true;
      return true;
    } finally {
      lock.unlock();
    }
  }

  /** The whole number of calls currently allowed at once. */
  public int currentLimit() {
    lock.lock();
    try {
      return Math.max(min, (int) Math.floor(limit + 1e-9));
    } finally {
      lock.unlock();
    }
  }

  /** Calls holding a slot right now. */
  public int inFlight() {
    lock.lock();
    try {
      return inFlight;
    } finally {
      lock.unlock();
    }
  }
}
