package io.github.xeeshan16.enricher.llm.observation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.LlmProviderException;
import io.github.xeeshan16.enricher.llm.LlmRequest;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import io.github.xeeshan16.enricher.llm.LlmThrottledException;
import io.github.xeeshan16.enricher.llm.LlmTimeoutException;
import io.github.xeeshan16.enricher.llm.TokenUsage;
import io.github.xeeshan16.enricher.llm.fake.FakeLlmProvider;
import io.github.xeeshan16.enricher.llm.resilience.FullJitterBackoff;
import io.github.xeeshan16.enricher.llm.resilience.ResilientLlmProvider;
import io.micrometer.common.KeyValue;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ObservedLlmProviderTest {

  private static final LlmRequest REQUEST =
      new LlmRequest("fake-1", "system", "classify {\"id\":1}", null, 64);

  private final List<Observation.Context> stopped = new CopyOnWriteArrayList<>();
  private final ObservationRegistry registry = ObservationRegistry.create();

  ObservedLlmProviderTest() {
    registry
        .observationConfig()
        .observationHandler(
            new ObservationHandler<Observation.Context>() {
              @Override
              public boolean supportsContext(Observation.Context context) {
                return true;
              }

              @Override
              public void onStop(Observation.Context context) {
                stopped.add(context);
              }
            });
  }

  private static String low(Observation.Context context, String key) {
    KeyValue value = context.getLowCardinalityKeyValue(key);
    return value == null ? null : value.getValue();
  }

  private static String high(Observation.Context context, String key) {
    KeyValue value = context.getHighCardinalityKeyValue(key);
    return value == null ? null : value.getValue();
  }

  private List<Observation.Context> modelCalls() {
    return stopped.stream()
        .filter(c -> DefaultLlmObservationConvention.NAME.equals(c.getName()))
        .toList();
  }

  @Test
  void recordsOneClientObservationPerCallWithGenAiTags() {
    LlmResponse response =
        new ObservedLlmProvider(new FakeLlmProvider(), registry).complete(REQUEST);

    assertThat(modelCalls()).hasSize(1);
    Observation.Context call = modelCalls().get(0);
    assertThat(call).isInstanceOf(LlmCallContext.class);
    assertThat(call.getContextualName()).isEqualTo("chat fake-1");
    assertThat(low(call, "gen_ai.operation.name")).isEqualTo("chat");
    assertThat(low(call, "gen_ai.provider.name")).isEqualTo("fake");
    assertThat(low(call, "gen_ai.request.model")).isEqualTo("fake-1");
    assertThat(low(call, "error.type")).isEqualTo("none");
    assertThat(high(call, "gen_ai.request.max_tokens")).isEqualTo("64");
    assertThat(high(call, "gen_ai.response.model")).isEqualTo("fake-1");
    assertThat(high(call, "gen_ai.usage.input_tokens"))
        .isEqualTo(Long.toString(response.usage().inputTokens()));
    assertThat(high(call, "gen_ai.usage.output_tokens"))
        .isEqualTo(Long.toString(response.usage().outputTokens()));
    assertThat(call.getError()).isNull();
    // Prompts never become span attributes.
    assertThat(call.getAllKeyValues().stream().map(KeyValue::getValue))
        .noneMatch(v -> v.contains("classify"));
  }

  @Test
  void classifiesFailuresIntoABoundedErrorType() {
    assertThat(errorTypeOf(new LlmTimeoutException("p", Duration.ofSeconds(1))))
        .isEqualTo("timeout");
    assertThat(errorTypeOf(new LlmThrottledException("429", Duration.ofSeconds(1))))
        .isEqualTo("throttled");
    assertThat(errorTypeOf(new LlmProviderException("503", true))).isEqualTo("provider_retryable");
    assertThat(errorTypeOf(new LlmProviderException("401", false))).isEqualTo("provider_error");
    assertThat(errorTypeOf(new IllegalStateException("bug"))).isEqualTo("IllegalStateException");
  }

  private String errorTypeOf(RuntimeException failure) {
    stopped.clear();
    LlmProvider failing = failingWith(failure);
    assertThatThrownBy(() -> new ObservedLlmProvider(failing, registry).complete(REQUEST))
        .isSameAs(failure);
    Observation.Context call = modelCalls().get(0);
    assertThat(call.getError()).isSameAs(failure);
    assertThat(high(call, "gen_ai.usage.input_tokens")).isNull();
    return low(call, "error.type");
  }

  @Test
  void attemptsOnTheRetryThreadsAreChildrenOfTheCallersObservation() {
    AtomicInteger calls = new AtomicInteger();
    LlmProvider flaky =
        new LlmProvider() {
          @Override
          public String name() {
            return "flaky";
          }

          @Override
          public LlmResponse complete(LlmRequest request) {
            if (calls.incrementAndGet() == 1) {
              throw new LlmProviderException("503", true);
            }
            return new LlmResponse("{}", "fake-1", new TokenUsage(3, 1), Duration.ZERO);
          }
        };
    ResilientLlmProvider resilient =
        new ResilientLlmProvider(
            new ObservedLlmProvider(flaky, registry),
            new ResilientLlmProvider.Policy(Duration.ofSeconds(5), 3, Duration.ZERO, Duration.ZERO),
            FullJitterBackoff.of(Duration.ZERO, Duration.ZERO),
            d -> {},
            registry);

    Observation parent = Observation.start("enricher.record.process", registry);
    try (Observation.Scope ignored = parent.openScope()) {
      resilient.complete(REQUEST);
    } finally {
      parent.stop();
    }

    List<Observation.Context> attempts = modelCalls();
    assertThat(attempts).hasSize(2);
    assertThat(attempts)
        .allSatisfy(
            a -> {
              assertThat(a.getParentObservation()).isNotNull();
              assertThat(a.getParentObservation().getContextView()).isSameAs(parent.getContext());
            });
    assertThat(low(attempts.get(0), "error.type")).isEqualTo("provider_retryable");
    assertThat(low(attempts.get(1), "error.type")).isEqualTo("none");
    resilient.close();
  }

  @Test
  void aCustomConventionReplacesNamesAndTags() {
    LlmObservationConvention custom =
        new DefaultLlmObservationConvention() {
          @Override
          public io.micrometer.common.KeyValues getLowCardinalityKeyValues(LlmCallContext c) {
            return super.getLowCardinalityKeyValues(c).and("team", "payments");
          }
        };
    new ObservedLlmProvider(new FakeLlmProvider(), registry, custom).complete(REQUEST);
    assertThat(low(modelCalls().get(0), "team")).isEqualTo("payments");
  }

  private static LlmProvider failingWith(RuntimeException failure) {
    return new LlmProvider() {
      @Override
      public String name() {
        return "failing";
      }

      @Override
      public LlmResponse complete(LlmRequest request) {
        throw failure;
      }
    };
  }
}
