package io.github.xeeshan16.enricher.llm.fake;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.xeeshan16.enricher.llm.LlmRequest;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class FakeLlmProviderLatencyTest {

  private static LlmRequest request(String prompt) {
    return new LlmRequest("fake-1", "", prompt, null, 16);
  }

  @Test
  void latencyIsDeterministicAndWithinBounds() {
    FakeLlmProvider fake =
        new FakeLlmProvider(r -> "{}", Duration.ofMillis(20), Duration.ofMillis(30));
    for (int i = 0; i < 50; i++) {
      Duration d = fake.latencyOf(request("event " + i));
      assertThat(d).isBetween(Duration.ofMillis(20), Duration.ofMillis(50));
      assertThat(fake.latencyOf(request("event " + i))).isEqualTo(d);
    }
  }

  @Test
  void sleepsForTheSimulatedLatencyAndReportsIt() {
    FakeLlmProvider fake = new FakeLlmProvider(r -> "{}", Duration.ofMillis(40), Duration.ZERO);
    long start = System.nanoTime();
    LlmResponse response = fake.complete(request("x"));
    assertThat(Duration.ofNanos(System.nanoTime() - start))
        .isGreaterThanOrEqualTo(Duration.ofMillis(40));
    assertThat(response.latency()).isEqualTo(Duration.ofMillis(40));
  }

  @Test
  void defaultFakeIsInstantAndRejectsNegativeLatency() {
    assertThat(new FakeLlmProvider().latencyOf(request("x"))).isZero();
    assertThatThrownBy(() -> new FakeLlmProvider(r -> "{}", Duration.ofMillis(-1), Duration.ZERO))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
