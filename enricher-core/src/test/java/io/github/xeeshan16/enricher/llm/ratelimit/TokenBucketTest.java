package io.github.xeeshan16.enricher.llm.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class TokenBucketTest {

  private static final long SECOND = TimeUnit.SECONDS.toNanos(1);

  private final AtomicLong now = new AtomicLong(1_000 * SECOND);

  @Test
  void burstUpToCapacityThenWaitsForRefill() {
    TokenBucket bucket = TokenBucket.perMinute(60, now::get); // one per second

    for (int i = 0; i < 60; i++) {
      assertThat(bucket.tryReserve(1, 0)).isZero();
    }
    assertThat(bucket.tryReserve(1, 0)).isEqualTo(-1);
    assertThat(bucket.tryReserve(1, 5 * SECOND)).isEqualTo(SECOND);
    // The next caller queues behind the first one.
    assertThat(bucket.tryReserve(1, 5 * SECOND)).isEqualTo(2 * SECOND);
  }

  @Test
  void refusedReservationTakesNothing() {
    TokenBucket bucket = TokenBucket.perMinute(60, now::get);
    bucket.tryReserve(60, 0);

    assertThat(bucket.tryReserve(10, SECOND)).isEqualTo(-1);
    assertThat(bucket.tryReserve(1, SECOND)).isEqualTo(SECOND);
  }

  @Test
  void refillsWithTimeAndNeverAboveCapacity() {
    TokenBucket bucket = TokenBucket.perMinute(60, now::get);
    bucket.tryReserve(60, 0);

    now.addAndGet(30 * SECOND);
    assertThat(bucket.available()).isEqualTo(30.0, org.assertj.core.data.Offset.offset(1e-6));
    now.addAndGet(3600 * SECOND);
    assertThat(bucket.available()).isEqualTo(60.0);
  }

  @Test
  void oversizedRequestIsCappedAtCapacity() {
    TokenBucket bucket = TokenBucket.perMinute(1000, now::get);

    assertThat(bucket.tryReserve(5000, 0)).isZero();
    assertThat(bucket.available()).isEqualTo(0.0);
  }

  @Test
  void chargeCreatesDebtAndRefundRepaysIt() {
    TokenBucket bucket = TokenBucket.perMinute(600, now::get); // ten per second
    bucket.charge(600);
    bucket.charge(100);
    assertThat(bucket.available()).isEqualTo(-100.0);
    assertThat(bucket.tryReserve(10, 60 * SECOND)).isEqualTo(11 * SECOND);

    bucket.refund(1000); // one refund returns at most one bucketful
    assertThat(bucket.available()).isEqualTo(490.0);
    bucket.refund(1000);
    assertThat(bucket.available()).isEqualTo(600.0);
  }

  @Test
  void blockUntilHoldsEveryReservationBack() {
    TokenBucket bucket = TokenBucket.perMinute(60, now::get);
    bucket.blockUntil(now.get() + 3 * SECOND);

    assertThat(bucket.tryReserve(1, 10 * SECOND)).isEqualTo(3 * SECOND);
    assertThat(bucket.tryReserve(1, 2 * SECOND)).isEqualTo(-1);
    now.addAndGet(3 * SECOND);
    assertThat(bucket.tryReserve(1, 0)).isZero();
  }
}
