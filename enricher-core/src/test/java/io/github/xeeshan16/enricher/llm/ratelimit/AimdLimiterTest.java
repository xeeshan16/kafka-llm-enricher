package io.github.xeeshan16.enricher.llm.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class AimdLimiterTest {

  private final AtomicLong now = new AtomicLong();

  private AimdLimiter limiter(int initial, int min, int max) {
    return new AimdLimiter(initial, min, max, 0.5, TimeUnit.SECONDS.toNanos(1), now::get);
  }

  @Test
  void growsByAboutOnePerRoundOfSuccesses() {
    AimdLimiter limiter = limiter(4, 1, 64);

    for (int i = 0; i < 4; i++) {
      limiter.onSuccess();
    }
    assertThat(limiter.currentLimit()).isEqualTo(4); // 4 + 4 * (about 1/4), just under 5

    for (int i = 0; i < 10; i++) {
      limiter.onSuccess();
    }
    assertThat(limiter.currentLimit()).isBetween(6, 7);
  }

  @Test
  void neverGrowsPastMax() {
    AimdLimiter limiter = limiter(2, 1, 3);
    for (int i = 0; i < 1000; i++) {
      limiter.onSuccess();
    }
    assertThat(limiter.currentLimit()).isEqualTo(3);
  }

  @Test
  void halvesOnThrottleOncePerCooldown() {
    AimdLimiter limiter = limiter(32, 2, 64);

    assertThat(limiter.onThrottle()).isTrue();
    assertThat(limiter.currentLimit()).isEqualTo(16);
    // The rest of the burst of 429s from calls that were already in flight.
    assertThat(limiter.onThrottle()).isFalse();
    assertThat(limiter.onThrottle()).isFalse();
    assertThat(limiter.currentLimit()).isEqualTo(16);

    now.addAndGet(TimeUnit.SECONDS.toNanos(1));
    assertThat(limiter.onThrottle()).isTrue();
    assertThat(limiter.currentLimit()).isEqualTo(8);
    for (int i = 0; i < 5; i++) {
      now.addAndGet(TimeUnit.SECONDS.toNanos(1));
      limiter.onThrottle();
    }
    assertThat(limiter.currentLimit()).isEqualTo(2);
  }

  @Test
  void acquireWaitsForAReleasedSlot() throws Exception {
    AimdLimiter limiter = limiter(1, 1, 1);
    assertThat(limiter.acquire(0)).isTrue();
    assertThat(limiter.acquire(TimeUnit.MILLISECONDS.toNanos(20))).isFalse();

    CountDownLatch waiting = new CountDownLatch(1);
    AtomicBoolean got = new AtomicBoolean();
    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  waiting.countDown();
                  try {
                    got.set(limiter.acquire(TimeUnit.SECONDS.toNanos(5)));
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                });
    waiting.await();
    Thread.sleep(50);
    limiter.release();
    t.join(5000);
    assertThat(got).isTrue();
    assertThat(limiter.inFlight()).isOne();
  }

  @Test
  void rejectsInconsistentBounds() {
    assertThatThrownBy(() -> limiter(0, 1, 4)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> limiter(5, 1, 4)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new AimdLimiter(2, 1, 4, 1.0, 0, now::get))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
