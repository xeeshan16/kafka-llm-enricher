package io.github.xeeshan16.enricher.llm.resilience;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.LlmProviderException;
import io.github.xeeshan16.enricher.llm.LlmRequest;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import io.github.xeeshan16.enricher.llm.LlmTimeoutException;
import io.github.xeeshan16.enricher.llm.TokenUsage;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ResilientLlmProviderTest {

  private static final LlmRequest REQUEST = new LlmRequest("m", "", "hello", null, 16);
  private static final LlmResponse OK =
      new LlmResponse("{}", "m", new TokenUsage(1, 1), Duration.ZERO);

  private final List<Duration> sleeps = new ArrayList<>();

  /** Scripted provider: each call takes the next step; a step is a response or an exception. */
  private static final class Scripted implements LlmProvider {
    final Deque<Object> steps = new ArrayDeque<>();
    final AtomicInteger calls = new AtomicInteger();

    Scripted then(Object step) {
      steps.add(step);
      return this;
    }

    @Override
    public String name() {
      return "scripted";
    }

    @Override
    public LlmResponse complete(LlmRequest request) {
      calls.incrementAndGet();
      Object step = steps.isEmpty() ? OK : steps.poll();
      if (step instanceof RuntimeException e) {
        throw e;
      }
      return (LlmResponse) step;
    }
  }

  private ResilientLlmProvider resilient(LlmProvider delegate, ResilientLlmProvider.Policy policy) {
    return new ResilientLlmProvider(
        delegate,
        policy,
        new FullJitterBackoff(policy.initialBackoff(), policy.maxBackoff(), bound -> bound),
        sleeps::add);
  }

  private static ResilientLlmProvider.Policy policy(Duration timeout, int attempts) {
    return new ResilientLlmProvider.Policy(
        timeout, attempts, Duration.ofMillis(100), Duration.ofMillis(250));
  }

  @Test
  void retriesRetryableFailuresWithGrowingJitteredWaits() {
    Scripted provider =
        new Scripted()
            .then(new LlmProviderException("503", true))
            .then(new LlmProviderException("503", true))
            .then(OK);

    LlmResponse response = resilient(provider, policy(Duration.ZERO, 3)).complete(REQUEST);

    assertThat(response).isSameAs(OK);
    assertThat(provider.calls).hasValue(3);
    assertThat(sleeps).containsExactly(Duration.ofMillis(100), Duration.ofMillis(200));
  }

  @Test
  void honoursRetryAfterWhenItIsLongerThanTheJitteredWait() {
    Scripted provider =
        new Scripted()
            .then(new LlmProviderException("429", true, Duration.ofSeconds(3), null))
            .then(new LlmProviderException("429", true, Duration.ofMillis(1), null))
            .then(OK);

    resilient(provider, policy(Duration.ZERO, 3)).complete(REQUEST);

    assertThat(sleeps).containsExactly(Duration.ofSeconds(3), Duration.ofMillis(200));
  }

  @Test
  void givesUpAfterMaxAttemptsWithTheLastFailure() {
    LlmProviderException last = new LlmProviderException("still 503", true);
    Scripted provider =
        new Scripted().then(new LlmProviderException("503", true)).then(last).then(OK);

    assertThatThrownBy(() -> resilient(provider, policy(Duration.ZERO, 2)).complete(REQUEST))
        .isSameAs(last);
    assertThat(provider.calls).hasValue(2);
    assertThat(sleeps).hasSize(1);
  }

  @Test
  void doesNotRetryNonRetryableFailuresOrBugs() {
    Scripted badRequest = new Scripted().then(new LlmProviderException("400", false));
    Scripted bug = new Scripted().then(new IllegalStateException("bug"));

    assertThatThrownBy(() -> resilient(badRequest, policy(Duration.ZERO, 5)).complete(REQUEST))
        .hasMessage("400");
    assertThatThrownBy(() -> resilient(bug, policy(Duration.ofSeconds(5), 5)).complete(REQUEST))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("bug");
    assertThat(badRequest.calls).hasValue(1);
    assertThat(bug.calls).hasValue(1);
    assertThat(sleeps).isEmpty();
  }

  @Test
  void slowCallTimesOutIsInterruptedAndRetried() throws Exception {
    CountDownLatch never = new CountDownLatch(1);
    AtomicBoolean interrupted = new AtomicBoolean();
    AtomicInteger calls = new AtomicInteger();
    LlmProvider slowOnce =
        new LlmProvider() {
          @Override
          public String name() {
            return "slow";
          }

          @Override
          public LlmResponse complete(LlmRequest request) {
            if (calls.incrementAndGet() == 1) {
              try {
                never.await(30, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
              }
              throw new LlmProviderException("interrupted", true);
            }
            return OK;
          }
        };

    try (ResilientLlmProvider provider = resilient(slowOnce, policy(Duration.ofMillis(200), 2))) {
      long start = System.nanoTime();
      assertThat(provider.complete(REQUEST)).isSameAs(OK);
      assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(10));
    }
    assertThat(calls).hasValue(2);
    // The abandoned attempt is cancelled with an interrupt, so a well-behaved client stops.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!interrupted.get() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertThat(interrupted).isTrue();
  }

  @Test
  void timeoutOnTheLastAttemptIsARetryableTimeoutException() {
    LlmProvider hangs =
        new LlmProvider() {
          @Override
          public String name() {
            return "hangs";
          }

          @Override
          public LlmResponse complete(LlmRequest request) {
            try {
              Thread.sleep(10_000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return OK;
          }
        };

    try (ResilientLlmProvider provider = resilient(hangs, policy(Duration.ofMillis(100), 1))) {
      assertThatThrownBy(() -> provider.complete(REQUEST))
          .isInstanceOfSatisfying(
              LlmTimeoutException.class, e -> assertThat(e.retryable()).isTrue())
          .hasMessageContaining("hangs call timed out after 100 ms");
    }
  }

  @Test
  void worstCaseCoversEveryAttemptAndWait() {
    ResilientLlmProvider.Policy policy =
        new ResilientLlmProvider.Policy(
            Duration.ofSeconds(30), 3, Duration.ofMillis(200), Duration.ofSeconds(5));

    assertThat(policy.worstCase()).isEqualTo(Duration.ofSeconds(100));
    assertThat(ResilientLlmProvider.Policy.defaults()).isEqualTo(policy);
    assertThatThrownBy(
            () -> new ResilientLlmProvider.Policy(Duration.ZERO, 0, Duration.ZERO, Duration.ZERO))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
