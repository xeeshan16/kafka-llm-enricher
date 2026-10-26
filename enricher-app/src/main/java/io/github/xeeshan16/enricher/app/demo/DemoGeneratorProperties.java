package io.github.xeeshan16.enricher.app.demo;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * {@code enricher.demo.generator.*}: the synthetic dispute stream of the compose demo.
 *
 * @param enabled publish synthetic disputes while the application runs
 * @param topic where to publish them
 * @param eventsPerSecond publishing rate
 * @param maxEvents stop after this many events; 0 for no limit
 * @param replayRatio share of events that repeat an earlier event exactly
 * @param seed same seed, same stream
 */
@ConfigurationProperties(prefix = "enricher.demo.generator")
public record DemoGeneratorProperties(
    @DefaultValue("false") boolean enabled,
    @DefaultValue("payments.disputes") String topic,
    @DefaultValue("5") double eventsPerSecond,
    @DefaultValue("0") long maxEvents,
    @DefaultValue("0.1") double replayRatio,
    @DefaultValue("42") long seed) {

  public DemoGeneratorProperties {
    if (eventsPerSecond <= 0) {
      throw new IllegalArgumentException("enricher.demo.generator.events-per-second must be > 0");
    }
  }
}
