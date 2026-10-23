package io.github.xeeshan16.enricher.llm.resilience;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class FullJitterBackoffTest {

  @Test
  void ceilingDoublesUntilTheCap() {
    FullJitterBackoff backoff =
        new FullJitterBackoff(Duration.ofMillis(200), Duration.ofSeconds(5), bound -> bound);

    assertThat(List.of(1, 2, 3, 4, 5, 6, 40, 100))
        .extracting(n -> backoff.ceiling(n).toMillis())
        .containsExactly(200L, 400L, 800L, 1600L, 3200L, 5000L, 5000L, 5000L);
  }

  @Test
  void delayIsDrawnBetweenZeroAndTheCeiling() {
    List<Long> bounds = new ArrayList<>();
    FullJitterBackoff backoff =
        new FullJitterBackoff(
            Duration.ofMillis(100),
            Duration.ofSeconds(1),
            bound -> {
              bounds.add(bound);
              return bound / 2;
            });

    assertThat(backoff.delay(1)).isEqualTo(Duration.ofMillis(50));
    assertThat(backoff.delay(3)).isEqualTo(Duration.ofMillis(200));
    assertThat(bounds).containsExactly(100L, 400L);
  }

  @Test
  void randomValuesOutsideTheWindowAreClamped() {
    FullJitterBackoff high =
        new FullJitterBackoff(Duration.ofMillis(100), Duration.ofSeconds(1), bound -> bound * 10);
    FullJitterBackoff low =
        new FullJitterBackoff(Duration.ofMillis(100), Duration.ofSeconds(1), bound -> -5);

    assertThat(high.delay(1)).isEqualTo(Duration.ofMillis(100));
    assertThat(low.delay(1)).isZero();
  }

  @Test
  void defaultRandomStaysInsideTheWindow() {
    FullJitterBackoff backoff = FullJitterBackoff.of(Duration.ofMillis(10), Duration.ofMillis(80));
    for (int i = 0; i < 1_000; i++) {
      assertThat(backoff.delay(1 + i % 6).toMillis()).isBetween(0L, 80L);
    }
    assertThat(FullJitterBackoff.of(Duration.ZERO, Duration.ZERO).delay(3)).isZero();
  }

  @Test
  void rejectsInvalidSettings() {
    assertThatThrownBy(() -> FullJitterBackoff.of(Duration.ofSeconds(2), Duration.ofSeconds(1)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> FullJitterBackoff.of(Duration.ZERO, Duration.ZERO).ceiling(0))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
