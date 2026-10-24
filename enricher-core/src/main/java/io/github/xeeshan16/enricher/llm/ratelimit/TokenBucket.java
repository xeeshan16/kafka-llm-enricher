package io.github.xeeshan16.enricher.llm.ratelimit;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * A token bucket that hands out reservations: a caller takes its permits at once and is told how
 * long to wait before using them. The balance may go negative (debt), which is how waiting callers
 * queue up behind each other without a lock being held while they sleep.
 *
 * <p>A request for more permits than the bucket holds is capped at the capacity, so an oversized
 * request still goes through once the bucket is full instead of never.
 *
 * <p>Thread-safe. Uses a {@link ReentrantLock} rather than {@code synchronized} so virtual threads
 * waiting for it are not pinned to their carrier.
 */
public final class TokenBucket {

  private final long capacity;
  private final double permitsPerNano;
  private final LongSupplier nanoTime;
  private final ReentrantLock lock = new ReentrantLock();
  private double available;
  private long lastRefill;
  private long blockedUntil;

  /**
   * @param capacity permits the bucket holds when full, and the burst it allows
   * @param permitsPerMinute refill rate
   * @param nanoTime monotonic clock, normally {@code System::nanoTime}
   */
  public TokenBucket(long capacity, double permitsPerMinute, LongSupplier nanoTime) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("capacity must be positive: " + capacity);
    }
    if (!(permitsPerMinute > 0)) {
      throw new IllegalArgumentException("permitsPerMinute must be positive: " + permitsPerMinute);
    }
    this.capacity = capacity;
    this.permitsPerNano = permitsPerMinute / TimeUnit.MINUTES.toNanos(1);
    this.nanoTime = nanoTime;
    this.available = capacity;
    this.lastRefill = nanoTime.getAsLong();
    this.blockedUntil = lastRefill;
  }

  /** A bucket that refills {@code perMinute} permits a minute and bursts up to the same amount. */
  public static TokenBucket perMinute(long perMinute, LongSupplier nanoTime) {
    return new TokenBucket(perMinute, perMinute, nanoTime);
  }

  public long capacity() {
    return capacity;
  }

  /**
   * Reserves {@code permits} if they become usable within {@code maxWaitNanos}.
   *
   * @return nanoseconds the caller must wait before using the permits, or {@code -1} when that
   *     would exceed {@code maxWaitNanos}; nothing is reserved in that case
   */
  public long tryReserve(long permits, long maxWaitNanos) {
    if (permits < 0) {
      throw new IllegalArgumentException("permits must not be negative: " + permits);
    }
    long wanted = Math.min(permits, capacity);
    lock.lock();
    try {
      long now = refill();
      long wait = Math.max(waitFor(wanted), blockedUntil - now);
      if (wait > maxWaitNanos) {
        return -1;
      }
      available -= wanted;
      return Math.max(0, wait);
    } finally {
      lock.unlock();
    }
  }

  /** Reserves {@code permits} however long that takes, for usage that already happened. */
  public void charge(long permits) {
    if (permits <= 0) {
      return;
    }
    lock.lock();
    try {
      refill();
      available -= Math.min(permits, capacity);
    } finally {
      lock.unlock();
    }
  }

  /** Gives back permits that were reserved but not used. */
  public void refund(long permits) {
    if (permits <= 0) {
      return;
    }
    lock.lock();
    try {
      refill();
      available = Math.min(capacity, available + Math.min(permits, capacity));
    } finally {
      lock.unlock();
    }
  }

  /** Lets no reservation become usable before {@code nanoTime}, for a provider's Retry-After. */
  public void blockUntil(long untilNanos) {
    lock.lock();
    try {
      if (untilNanos - blockedUntil > 0) {
        blockedUntil = untilNanos;
      }
    } finally {
      lock.unlock();
    }
  }

  /** Permits available right now; negative while callers are queued. */
  public double available() {
    lock.lock();
    try {
      refill();
      return available;
    } finally {
      lock.unlock();
    }
  }

  private long refill() {
    long now = nanoTime.getAsLong();
    long elapsed = now - lastRefill;
    if (elapsed > 0) {
      available = Math.min(capacity, available + elapsed * permitsPerNano);
      lastRefill = now;
    }
    return now;
  }

  private long waitFor(long wanted) {
    double missing = wanted - available;
    if (missing <= 0) {
      return 0;
    }
    return (long) Math.ceil(missing / permitsPerNano);
  }
}
