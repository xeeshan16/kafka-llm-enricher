package io.github.xeeshan16.enricher.spring;

import io.github.xeeshan16.enricher.kafka.ConsumerLoop;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * {@code DOWN} when a consumer loop has died. A dead loop does not restart by itself and its
 * partitions stay unconsumed until the group rebalances them, so the health group used for liveness
 * should include this indicator ({@code
 * management.endpoint.health.group.liveness.include=livenessState,enricher}).
 */
public final class PipelinesHealthIndicator implements HealthIndicator {

  private final PipelineRunner runner;

  public PipelinesHealthIndicator(PipelineRunner runner) {
    this.runner = Objects.requireNonNull(runner, "runner");
  }

  @Override
  public Health health() {
    if (!runner.isRunning()) {
      return Health.up().withDetail("state", "not started").build();
    }
    Map<String, Object> loops = new LinkedHashMap<>();
    boolean failed = false;
    for (ConsumerLoop loop : runner.loops()) {
      Map<String, Object> detail = new LinkedHashMap<>();
      detail.put("partitions", loop.assignedPartitions().size());
      detail.put("inFlight", loop.stats().snapshot().inFlight());
      Throwable failure = loop.failure();
      if (failure != null) {
        failed = true;
        detail.put("failure", failure.getClass().getSimpleName());
      }
      loops.put(loop.name(), detail);
    }
    return (failed ? Health.down() : Health.up())
        .withDetail("delivery", runner.transactional() ? "exactly-once" : "at-least-once")
        .withDetail("loops", loops)
        .build();
  }
}
