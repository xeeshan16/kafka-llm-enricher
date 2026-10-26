package io.github.xeeshan16.enricher.spring;

import io.github.xeeshan16.enricher.kafka.EngineStats;
import io.github.xeeshan16.enricher.llm.ratelimit.ProviderRateLimiter;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;

/**
 * Engine and rate-limiter meters, read from their counters at scrape time.
 *
 * <ul>
 *   <li>Per pipeline: {@code enricher.records.polled}, {@code .processed}, {@code .discarded},
 *       {@code .committed}, {@code enricher.outputs.written}, {@code enricher.commits}, {@code
 *       enricher.commit.failures}, {@code enricher.partitions.rewound}, {@code
 *       enricher.partitions.pauses}, {@code enricher.partitions.rebalanced} (by {@code event}:
 *       assigned, revoked, lost); gauges {@code enricher.records.in_flight} and {@code
 *       enricher.partitions.paused}.
 *   <li>Per provider: {@code enricher.ratelimit.admitted}, {@code .rejected} (would have waited
 *       longer than {@code max-wait}), {@code .throttled} (429 and 529 answers), {@code
 *       .decreases}, {@code enricher.ratelimit.wait} (seconds spent waiting for quota); gauges
 *       {@code enricher.ratelimit.concurrency.limit} (the AIMD limit) and {@code
 *       enricher.ratelimit.in_flight}.
 * </ul>
 */
public final class EnricherMeterBinder implements MeterBinder {

  private final PipelineRunner runner;
  private final ProviderRateLimiter limiter;
  private final String provider;

  public EnricherMeterBinder(PipelineRunner runner, ProviderRateLimiter limiter, String provider) {
    this.runner = Objects.requireNonNull(runner, "runner");
    this.limiter = Objects.requireNonNull(limiter, "limiter");
    this.provider = Objects.requireNonNull(provider, "provider");
  }

  @Override
  public void bindTo(MeterRegistry registry) {
    for (String pipeline : runner.pipelineNames()) {
      engineCounter(
          registry,
          pipeline,
          "enricher.records.polled",
          "Records returned by poll()",
          s -> s.polled());
      engineCounter(
          registry,
          pipeline,
          "enricher.records.processed",
          "Records whose processing finished",
          s -> s.processed());
      engineCounter(
          registry,
          pipeline,
          "enricher.records.discarded",
          "Finished records dropped because their partition was revoked or rewound",
          s -> s.discarded());
      engineCounter(
          registry,
          pipeline,
          "enricher.records.committed",
          "Input records whose offsets were committed",
          s -> s.committedInputs());
      engineCounter(
          registry,
          pipeline,
          "enricher.outputs.written",
          "Output, retry, deferred and dead-letter records written",
          s -> s.writtenOutputs());
      engineCounter(registry, pipeline, "enricher.commits", "Successful commits", s -> s.commits());
      engineCounter(
          registry,
          pipeline,
          "enricher.commit.failures",
          "Failed commits",
          s -> s.commitFailures());
      engineCounter(
          registry,
          pipeline,
          "enricher.partitions.rewound",
          "Partitions sought back to their last committed offset",
          s -> s.rewinds());
      engineCounter(
          registry,
          pipeline,
          "enricher.partitions.pauses",
          "Partitions paused for backpressure",
          s -> s.pauses());
      rebalance(registry, pipeline, "assigned", s -> s.partitionsAssigned());
      rebalance(registry, pipeline, "revoked", s -> s.partitionsRevoked());
      rebalance(registry, pipeline, "lost", s -> s.partitionsLost());
      Gauge.builder("enricher.records.in_flight", runner, r -> r.stats(pipeline).inFlight())
          .description("Records being processed now")
          .tag("pipeline", pipeline)
          .register(registry);
      Gauge.builder("enricher.partitions.paused", runner, r -> r.stats(pipeline).pausedPartitions())
          .description("Partitions paused for backpressure now")
          .tag("pipeline", pipeline)
          .register(registry);
    }
    limiterCounter(registry, "enricher.ratelimit.admitted", "Calls admitted", s -> s.admitted());
    limiterCounter(
        registry,
        "enricher.ratelimit.rejected",
        "Calls refused because quota would free up later than max-wait",
        s -> s.rejected());
    limiterCounter(
        registry,
        "enricher.ratelimit.throttled",
        "Throttling answers (429, 529)",
        s -> s.throttled());
    limiterCounter(
        registry,
        "enricher.ratelimit.decreases",
        "Cuts of the adaptive concurrency limit",
        s -> s.decreases());
    FunctionCounter.builder(
            "enricher.ratelimit.wait",
            limiter,
            l -> l.stats().waitedNanos() / (double) TimeUnit.SECONDS.toNanos(1))
        .description("Time admitted calls waited for request and token quota")
        .baseUnit("seconds")
        .tag("provider", provider)
        .register(registry);
    Gauge.builder(
            "enricher.ratelimit.concurrency.limit", limiter, l -> l.stats().concurrencyLimit())
        .description("Current adaptive (AIMD) concurrency limit")
        .tag("provider", provider)
        .register(registry);
    Gauge.builder("enricher.ratelimit.in_flight", limiter, l -> l.stats().inFlight())
        .description("Provider calls running now")
        .tag("provider", provider)
        .register(registry);
  }

  private void engineCounter(
      MeterRegistry registry,
      String pipeline,
      String name,
      String description,
      ToDoubleFunction<EngineStats.Snapshot> value) {
    FunctionCounter.builder(name, runner, r -> value.applyAsDouble(r.stats(pipeline)))
        .description(description)
        .tag("pipeline", pipeline)
        .register(registry);
  }

  private void rebalance(
      MeterRegistry registry,
      String pipeline,
      String event,
      ToDoubleFunction<EngineStats.Snapshot> value) {
    FunctionCounter.builder(
            "enricher.partitions.rebalanced", runner, r -> value.applyAsDouble(r.stats(pipeline)))
        .description("Partitions assigned, revoked or lost by rebalances")
        .tags("pipeline", pipeline, "event", event)
        .register(registry);
  }

  private void limiterCounter(
      MeterRegistry registry,
      String name,
      String description,
      ToDoubleFunction<ProviderRateLimiter.Stats> value) {
    FunctionCounter.builder(name, limiter, l -> value.applyAsDouble(l.stats()))
        .description(description)
        .tag("provider", provider)
        .register(registry);
  }
}
