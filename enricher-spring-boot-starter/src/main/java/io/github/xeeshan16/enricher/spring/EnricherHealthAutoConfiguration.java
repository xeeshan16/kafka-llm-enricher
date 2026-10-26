package io.github.xeeshan16.enricher.spring;

import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/** The {@code enricher} health component, when Spring Boot Actuator is on the classpath. */
@AutoConfiguration(after = EnricherAutoConfiguration.class)
@ConditionalOnClass(HealthIndicator.class)
@ConditionalOnBean(PipelineRunner.class)
public class EnricherHealthAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(name = "enricherHealthIndicator")
  public PipelinesHealthIndicator enricherHealthIndicator(PipelineRunner runner) {
    return new PipelinesHealthIndicator(runner);
  }
}
