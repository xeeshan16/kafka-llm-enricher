package io.github.xeeshan16.enricher.app.demo;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Runs the {@link DisputeGenerator} when {@code enricher.demo.generator.enabled=true}. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "enricher.demo.generator", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(DemoGeneratorProperties.class)
public class DemoGeneratorConfiguration {

  @Bean
  DisputeGenerator disputeGenerator(
      DemoGeneratorProperties properties,
      KafkaProperties kafka,
      ObjectProvider<SslBundles> sslBundles) {
    return new DisputeGenerator(
        properties, kafka.buildProducerProperties(sslBundles.getIfAvailable()));
  }
}
