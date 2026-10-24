package io.github.xeeshan16.enricher.llm.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.LlmProviderException;
import io.github.xeeshan16.enricher.llm.LlmRequest;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import io.github.xeeshan16.enricher.llm.LlmThrottledException;
import io.github.xeeshan16.enricher.llm.TokenUsage;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ProviderRateLimiterTest {

  private final AtomicLong now = new AtomicLong(TimeUnit.HOURS.toNanos(1));
  private final List<Duration> sleeps = new ArrayList<>();

  private ProviderRateLimiter limiter(int rpm, long tpm, Duration maxWait, int concurrency) {
    return new ProviderRateLimiter(
        new ProviderRateLimiter.Limits(
            rpm, tpm, maxWait, concurrency, 1, Math.max(concurrency, 1), 0.5, Duration.ZERO),
        now::get,
        d -> {
          sleeps.add(d);
          now.addAndGet(d.toNanos());
        });
  }

  @Test
  void requestsPerMinuteSpreadsCallsOutAndRefusesLongWaits() {
    ProviderRateLimiter limiter = limiter(60, 0, Duration.ofSeconds(2), 64);
    for (int i = 0; i < 60; i++) {
      limiter.acquire(10).succeeded(10);
    }
    assertThat(sleeps).isEmpty();

    limiter.acquire(10).succeeded(10);
    assertThat(sleeps).containsExactly(Duration.ofSeconds(1));

    ProviderRateLimiter strict = limiter(60, 0, Duration.ofMillis(500), 64);
    for (int i = 0; i < 60; i++) {
      strict.acquire(1).succeeded(1);
    }
    assertThatThrownBy(() -> strict.acquire(1))
        .isInstanceOfSatisfying(
            LlmProviderException.class,
            e -> {
              assertThat(e.retryable()).isTrue();
              assertThat(e.retryAfter()).isPresent();
              assertThat(e.getMessage()).contains("requests-per-minute");
            });
    assertThat(strict.stats().rejected()).isOne();
  }

  @Test
  void tokensPerMinuteSettlesEstimatesAgainstRealUsage() {
    ProviderRateLimiter limiter = limiter(0, 1000, Duration.ZERO, 64);
    ProviderRateLimiter.Permit a = limiter.acquire(400);
    ProviderRateLimiter.Permit b = limiter.acquire(400);
    assertThatThrownBy(() -> limiter.acquire(400)).isInstanceOf(LlmProviderException.class);

    a.succeeded(100);
    b.succeeded(100);
    // 1000 - 2 * 100 used: a 700-token estimate fits again without waiting.
    limiter.acquire(700).succeeded(700);
    assertThat(sleeps).isEmpty();
  }

  @Test
  void failedCallsReturnTheirTokens() {
    ProviderRateLimiter limiter = limiter(0, 1000, Duration.ZERO, 64);
    limiter.acquire(900).failed();
    limiter.acquire(900).succeeded(900);
    assertThat(limiter.stats().admitted()).isEqualTo(2);
  }

  @Test
  void throttlingHalvesConcurrencyAndHonoursRetryAfter() {
    ProviderRateLimiter limiter = limiter(600, 0, Duration.ofSeconds(10), 8);
    limiter.acquire(1).throttled(Duration.ofSeconds(2));

    ProviderRateLimiter.Stats stats = limiter.stats();
    assertThat(stats.throttled()).isOne();
    assertThat(stats.decreases()).isOne();
    assertThat(stats.concurrencyLimit()).isEqualTo(4);
    assertThat(stats.inFlight()).isZero();

    limiter.acquire(1).succeeded(1);
    assertThat(sleeps).containsExactly(Duration.ofSeconds(2));
  }

  @Test
  void permitOutcomeCountsOnce() {
    ProviderRateLimiter limiter = limiter(0, 0, Duration.ZERO, 1);
    ProviderRateLimiter.Permit permit = limiter.acquire(1);
    permit.succeeded(1);
    permit.failed();
    permit.throttled(null);

    assertThat(limiter.stats().throttled()).isZero();
    assertThat(limiter.stats().inFlight()).isZero();
    limiter.acquire(1).succeeded(1); // the single slot is free again
  }

  @Test
  void rateLimitedProviderFeedsThrottlesBackAndConvergesBelowTheProviderCapacity()
      throws Exception {
    int providerCapacity = 4;
    Semaphore capacity = new Semaphore(providerCapacity);
    AtomicInteger served = new AtomicInteger();
    LlmProvider provider =
        new LlmProvider() {
          @Override
          public String name() {
            return "capacity-4";
          }

          @Override
          public LlmResponse complete(LlmRequest request) {
            if (!capacity.tryAcquire()) {
              throw new LlmThrottledException("429 too many requests", null);
            }
            try {
              Thread.sleep(2);
              served.incrementAndGet();
              return new LlmResponse("{}", request.model(), new TokenUsage(10, 5), Duration.ZERO);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new LlmProviderException("interrupted", true, e);
            } finally {
              capacity.release();
            }
          }
        };
    ProviderRateLimiter limiter =
        new ProviderRateLimiter(
            new ProviderRateLimiter.Limits(
                0, 0, Duration.ofSeconds(30), 16, 1, 16, 0.5, Duration.ZERO));
    RateLimitedLlmProvider limited = new RateLimitedLlmProvider(provider, limiter);
    LlmRequest request = new LlmRequest("m", "", "hello", null, 16);

    int calls = 300;
    AtomicInteger throttledSeen = new AtomicInteger();
    try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < calls; i++) {
        futures.add(
            pool.submit(
                () -> {
                  while (true) {
                    try {
                      limited.complete(request);
                      return;
                    } catch (LlmThrottledException e) {
                      throttledSeen.incrementAndGet();
                    }
                  }
                }));
      }
      for (Future<?> f : futures) {
        f.get(60, TimeUnit.SECONDS);
      }
    }

    ProviderRateLimiter.Stats stats = limiter.stats();
    assertThat(served).hasValue(calls);
    assertThat(stats.throttled()).isEqualTo(throttledSeen.get()).isPositive();
    assertThat(stats.decreases()).isPositive();
    assertThat(stats.concurrencyLimit()).isLessThan(16);
    assertThat(stats.inFlight()).isZero();
  }
}
