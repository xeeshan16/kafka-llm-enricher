package io.github.xeeshan16.enricher.spring;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.xeeshan16.enricher.cost.UsageAccounting;
import io.github.xeeshan16.enricher.engine.Enricher;
import io.github.xeeshan16.enricher.engine.LedgeredEnricher;
import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.TokenUsage;
import io.github.xeeshan16.enricher.llm.fake.FakeLlmProvider;
import io.github.xeeshan16.enricher.llm.observation.ObservedLlmProvider;
import io.github.xeeshan16.enricher.llm.ratelimit.RateLimitedLlmProvider;
import io.github.xeeshan16.enricher.llm.resilience.ResilientLlmProvider;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class EnricherObservabilityAutoConfigurationTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(
              AutoConfigurations.of(
                  KafkaAutoConfiguration.class,
                  EnricherAutoConfiguration.class,
                  EnricherMetricsAutoConfiguration.class,
                  EnricherHealthAutoConfiguration.class))
          .withPropertyValues(
              "enricher.auto-startup=false",
              "enricher.pipelines.triage.input-topic=in",
              "enricher.pipelines.triage.output-topic=out",
              "enricher.pipelines.triage.user-template=Classify {{event}}",
              "enricher.pipelines.triage.output-schema=classpath:schemas/test.json",
              "enricher.pricing.fake-1.input-per-million=2",
              "enricher.pricing.fake-1.output-per-million=8");

  @Test
  void withoutRegistriesNothingIsObservedOrMetered() {
    runner.run(
        context -> {
          assertThat(context).hasNotFailed();
          assertThat(context).doesNotHaveBean(MicrometerUsageMeter.class);
          assertThat(context).doesNotHaveBean(EnricherMeterBinder.class);
          ResilientLlmProvider resilient =
              (ResilientLlmProvider) context.getBean(Enricher.class).provider();
          assertThat(((RateLimitedLlmProvider) resilient.delegate()).delegate())
              .isSameAs(context.getBean(LlmProvider.class));
          // The health indicator only needs Actuator on the classpath.
          assertThat(context.getBean(PipelinesHealthIndicator.class).health().getStatus())
              .isEqualTo(Status.UP);
        });
  }

  @Test
  void anObservationRegistryObservesEveryProviderAttempt() {
    runner
        .withBean(ObservationRegistry.class, ObservationRegistry::create)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              ResilientLlmProvider resilient =
                  (ResilientLlmProvider) context.getBean(Enricher.class).provider();
              LlmProvider inner = ((RateLimitedLlmProvider) resilient.delegate()).delegate();
              assertThat(inner)
                  .isInstanceOfSatisfying(
                      ObservedLlmProvider.class,
                      o -> assertThat(o.delegate()).isSameAs(context.getBean(LlmProvider.class)));
            });
  }

  @Test
  void aMeterRegistryAddsSpendEngineAndLimiterMeters() {
    runner
        .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
        .withPropertyValues("enricher.metrics.max-tenants=2")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              MeterRegistry registry = context.getBean(MeterRegistry.class);
              context.getBean(EnricherMeterBinder.class).bindTo(registry);

              // Both meters receive spend events.
              LedgeredEnricher enricher = context.getBean(LedgeredEnricher.class);
              assertThat(enricher.meter()).isNotSameAs(context.getBean(UsageAccounting.class));
              for (String tenant : new String[] {"acme", "globex", "initech", "umbrella"}) {
                enricher
                    .meter()
                    .providerCall("triage", tenant, "fake-1", new TokenUsage(1000, 500));
              }
              enricher.meter().ledgerHit("triage", "acme", "fake-1", new TokenUsage(1000, 500));
              enricher.meter().repairAttempt("triage", "acme", "fake-1", true);
              enricher.meter().piiMasked("triage", "PAN", 2);

              assertThat(context.getBean(UsageAccounting.class).total("triage").calls())
                  .isEqualTo(4);
              assertThat(
                      registry
                          .get("enricher.provider.calls")
                          .tags("pipeline", "triage", "tenant", "acme")
                          .counter()
                          .count())
                  .isEqualTo(1);
              // Tenants beyond max-tenants share one series.
              assertThat(
                      registry
                          .get("enricher.provider.calls")
                          .tags("tenant", TenantTags.OTHER)
                          .counter()
                          .count())
                  .isEqualTo(2);
              // 1000 x 2 + 500 x 8 USD per million tokens = 0.006 USD per call.
              assertThat(registry.get("enricher.cost").tags("tenant", "acme").counter().count())
                  .isCloseTo(0.006, org.assertj.core.data.Offset.offset(1e-9));
              assertThat(
                      registry.get("enricher.cost.saved").tags("tenant", "acme").counter().count())
                  .isCloseTo(0.006, org.assertj.core.data.Offset.offset(1e-9));
              assertThat(
                      registry
                          .get("gen_ai.client.token.usage")
                          .tags("gen_ai.token.type", "input", "gen_ai.provider.name", "fake")
                          .summary()
                          .totalAmount())
                  .isEqualTo(4000);
              assertThat(registry.get("enricher.repairs").tag("result", "fixed").counter().count())
                  .isEqualTo(1);
              assertThat(registry.get("enricher.pii.masked").tag("type", "PAN").counter().count())
                  .isEqualTo(2);

              // Engine and limiter meters exist from the start, at zero.
              assertThat(
                      registry
                          .get("enricher.records.polled")
                          .tag("pipeline", "triage")
                          .functionCounter()
                          .count())
                  .isZero();
              assertThat(
                      registry
                          .get("enricher.partitions.rebalanced")
                          .tags("pipeline", "triage", "event", "lost")
                          .functionCounter()
                          .count())
                  .isZero();
              assertThat(
                      registry
                          .get("enricher.ratelimit.concurrency.limit")
                          .tag("provider", "fake")
                          .gauge()
                          .value())
                  .isEqualTo(16);
            });
  }

  @Test
  void fakeLatencyIsConfigurable() {
    runner
        .withPropertyValues(
            "enricher.providers.fake.latency=25ms", "enricher.providers.fake.latency-jitter=10ms")
        .run(
            context -> {
              FakeLlmProvider fake = (FakeLlmProvider) context.getBean(LlmProvider.class);
              assertThat(
                      fake.latencyOf(
                          new io.github.xeeshan16.enricher.llm.LlmRequest("m", "", "x", null, 8)))
                  .isBetween(Duration.ofMillis(25), Duration.ofMillis(35));
            });
  }
}
