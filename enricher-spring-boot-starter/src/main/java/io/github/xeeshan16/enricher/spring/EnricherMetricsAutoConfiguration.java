package io.github.xeeshan16.enricher.spring;

import io.github.xeeshan16.enricher.cost.PriceTable;
import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.ratelimit.ProviderRateLimiter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Micrometer meters for the engine, the rate limiter and spend, when the application has a {@link
 * MeterRegistry} (for example through {@code spring-boot-starter-actuator}). Model-call and record
 * timers and spans come from observations, see {@link EnricherAutoConfiguration}.
 */
@AutoConfiguration(
    after = EnricherAutoConfiguration.class,
    afterName = {
      "org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration",
      "org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration",
      "org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleMetricsExportAutoConfiguration"
    })
@ConditionalOnClass(MeterRegistry.class)
@ConditionalOnBean({MeterRegistry.class, PipelineRunner.class})
public class EnricherMetricsAutoConfiguration {

  /** Spend events as meters; it receives them next to the in-memory accounting. */
  @Bean
  @ConditionalOnMissingBean
  public MicrometerUsageMeter enricherMicrometerUsageMeter(
      MeterRegistry registry,
      PriceTable prices,
      LlmProvider provider,
      EnricherProperties properties) {
    return new MicrometerUsageMeter(
        registry,
        prices,
        provider.name(),
        properties.metrics().tenantTag(),
        properties.metrics().maxTenants());
  }

  /** Engine and rate-limiter meters; Spring Boot binds every {@code MeterBinder} bean. */
  @Bean
  @ConditionalOnMissingBean
  public EnricherMeterBinder enricherMeterBinder(
      PipelineRunner runner, ProviderRateLimiter limiter, LlmProvider provider) {
    return new EnricherMeterBinder(runner, limiter, provider.name());
  }
}
