package io.github.xeeshan16.enricher.kafka;

import io.github.xeeshan16.enricher.cost.CostPolicy;
import io.github.xeeshan16.enricher.engine.EnrichmentOutcome;
import io.github.xeeshan16.enricher.engine.FailureReason;
import io.github.xeeshan16.enricher.engine.LedgeredEnricher;
import io.github.xeeshan16.enricher.ledger.LedgerBusyException;
import io.github.xeeshan16.enricher.llm.LlmProviderException;
import io.github.xeeshan16.enricher.pipeline.InputRecord;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enriches one record of a pipeline and decides where the result goes.
 *
 * <table>
 *   <caption>Routing</caption>
 *   <tr><th>Outcome</th><th>Written to</th></tr>
 *   <tr><td>usable answer (provider or ledger)</td><td>output topic</td></tr>
 *   <tr><td>answer not JSON, or violates the schema</td><td>dead-letter topic, at once</td></tr>
 *   <tr><td>non-retryable provider error (400, 401)</td><td>dead-letter topic, at once</td></tr>
 *   <tr><td>retryable provider error after the provider's in-place attempts</td>
 *       <td>next retry topic, or dead-letter topic as {@code RETRIES_EXHAUSTED}</td></tr>
 *   <tr><td>key leased by another worker for longer than {@code busyBudget}</td>
 *       <td>next retry topic, or dead-letter topic as {@code LEDGER_BUSY}</td></tr>
 *   <tr><td>any other exception (a poison record, a bug)</td>
 *       <td>dead-letter topic as {@code PROCESSING_ERROR}</td></tr>
 *   <tr><td>a skip rule matched, or over budget with action {@code skip}</td>
 *       <td>skipped envelope on the output topic, or nothing when {@code emitSkipped} is off</td></tr>
 *   <tr><td>over budget with action {@code defer}</td>
 *       <td>{@code <input>.enricher-deferred}, due when the budget window resets</td></tr>
 *   <tr><td>tombstone (null value)</td><td>nothing</td></tr>
 * </table>
 *
 * <p>Enriched, skipped and deferred records carry an {@code enricher-tenant} header.
 *
 * <p>Waiting for another worker's lease happens on this worker thread, which costs nothing in the
 * poll loop, and is bounded by {@code busyBudget} (about one lease lifetime): by then the other
 * worker has stored its result (a ledger hit) or its lease has lapsed (this worker claims it).
 */
public final class EnrichmentProcessor implements RecordProcessor {

  private static final Logger log = LoggerFactory.getLogger(EnrichmentProcessor.class);

  /** Pause between checks of a busy key; replaceable in tests. */
  @FunctionalInterface
  public interface Sleeper {
    void sleep(Duration duration) throws InterruptedException;
  }

  private final PipelineDefinition pipeline;
  private final LedgeredEnricher enricher;
  private final RetryTopics retryTopics;
  private final OutputRecordFactory outputs;
  private final FailureRecords failures;
  private final Duration busyRetryInterval;
  private final Duration busyBudget;
  private final Clock clock;
  private final Sleeper sleeper;
  private final CostPolicy costPolicy;
  private final boolean emitSkipped;

  public EnrichmentProcessor(
      PipelineDefinition pipeline,
      LedgeredEnricher enricher,
      RetryTopics retryTopics,
      OutputRecordFactory outputs,
      Duration busyRetryInterval,
      Duration busyBudget,
      Clock clock,
      Sleeper sleeper) {
    this(
        pipeline,
        enricher,
        retryTopics,
        outputs,
        busyRetryInterval,
        busyBudget,
        clock,
        sleeper,
        CostPolicy.none(),
        true);
  }

  /**
   * @param costPolicy skip rules, tenant and budgets of this pipeline
   * @param emitSkipped write a skipped envelope for skipped records (one output per input), or
   *     nothing
   */
  public EnrichmentProcessor(
      PipelineDefinition pipeline,
      LedgeredEnricher enricher,
      RetryTopics retryTopics,
      OutputRecordFactory outputs,
      Duration busyRetryInterval,
      Duration busyBudget,
      Clock clock,
      Sleeper sleeper,
      CostPolicy costPolicy,
      boolean emitSkipped) {
    this.costPolicy = Objects.requireNonNull(costPolicy, "costPolicy");
    this.emitSkipped = emitSkipped;
    this.pipeline = Objects.requireNonNull(pipeline, "pipeline");
    this.enricher = Objects.requireNonNull(enricher, "enricher");
    this.retryTopics = Objects.requireNonNull(retryTopics, "retryTopics");
    this.outputs = Objects.requireNonNull(outputs, "outputs");
    this.busyRetryInterval = Objects.requireNonNull(busyRetryInterval, "busyRetryInterval");
    this.busyBudget = Objects.requireNonNull(busyBudget, "busyBudget");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    this.failures = new FailureRecords(pipeline, retryTopics, clock);
  }

  public EnrichmentProcessor(
      PipelineDefinition pipeline,
      LedgeredEnricher enricher,
      RetryTopics retryTopics,
      OutputRecordFactory outputs,
      Duration busyRetryInterval,
      Duration busyBudget) {
    this(
        pipeline,
        enricher,
        retryTopics,
        outputs,
        busyRetryInterval,
        busyBudget,
        Clock.systemUTC(),
        d -> Thread.sleep(d.toMillis()));
  }

  public PipelineDefinition pipeline() {
    return pipeline;
  }

  public CostPolicy costPolicy() {
    return costPolicy;
  }

  @Override
  public List<ProducerRecord<String, String>> process(ConsumerRecord<String, String> record) {
    if (record.value() == null) {
      return List.of();
    }
    int stage = Math.max(0, retryTopics.stageOf(record.topic()));
    InputRecord input = FailureRecords.originalInput(record);
    long busyDeadline = clock.millis() + busyBudget.toMillis();
    while (true) {
      try {
        LedgeredEnricher.Result result = enricher.process(pipeline, input, costPolicy);
        return switch (result.outcome()) {
          case EnrichmentOutcome.Enriched enriched ->
              List.of(
                  withTenant(
                      outputs.enriched(pipeline, input, enriched, result.source()),
                      result.tenant()));
          case EnrichmentOutcome.Skipped skipped ->
              emitSkipped
                  ? List.of(withTenant(outputs.skipped(pipeline, input, skipped), result.tenant()))
                  : List.of();
          case EnrichmentOutcome.Deferred deferred -> {
            log.info(
                "pipeline={} deferring {}-{}@{} until {}: {}",
                pipeline.name(),
                input.topic(),
                input.partition(),
                input.offset(),
                deferred.until(),
                deferred.detail());
            yield List.of(
                failures.deferred(
                    record,
                    stage,
                    deferred.tenant(),
                    deferred.until().toEpochMilli(),
                    deferred.detail()));
          }
          case EnrichmentOutcome.Failed failed -> {
            log.warn(
                "pipeline={} cannot enrich {}-{}@{}: {} {}",
                pipeline.name(),
                input.topic(),
                input.partition(),
                input.offset(),
                failed.reason(),
                failed.detail());
            yield List.of(failures.deadLetter(record, stage, failed.reason(), failed.detail()));
          }
        };
      } catch (LedgerBusyException e) {
        if (clock.millis() < busyDeadline) {
          pause();
          continue;
        }
        return List.of(
            failures.retryOrDeadLetter(
                record,
                stage,
                FailureReason.LEDGER_BUSY,
                FailureReason.LEDGER_BUSY,
                e.getMessage()));
      } catch (LlmProviderException e) {
        if (e.retryable()) {
          log.info(
              "pipeline={} retryable failure for {}-{}@{} at stage {}: {}",
              pipeline.name(),
              input.topic(),
              input.partition(),
              input.offset(),
              stage,
              e.getMessage());
          return List.of(
              failures.retryOrDeadLetter(
                  record,
                  stage,
                  FailureReason.PROVIDER_ERROR,
                  FailureReason.RETRIES_EXHAUSTED,
                  e.getMessage()));
        }
        log.warn(
            "pipeline={} non-retryable provider failure for {}-{}@{}: {}",
            pipeline.name(),
            input.topic(),
            input.partition(),
            input.offset(),
            e.getMessage());
        return List.of(
            failures.deadLetter(record, stage, FailureReason.PROVIDER_ERROR, e.getMessage()));
      }
    }
  }

  @Override
  public List<ProducerRecord<String, String>> onFailure(
      ConsumerRecord<String, String> record, Throwable error) {
    log.warn(
        "pipeline={} dead-lettering {}-{}@{} after an unexpected error",
        pipeline.name(),
        record.topic(),
        record.partition(),
        record.offset(),
        error);
    int stage = Math.max(0, retryTopics.stageOf(record.topic()));
    return List.of(
        failures.deadLetter(record, stage, FailureReason.PROCESSING_ERROR, error.toString()));
  }

  @Override
  public long notBeforeMillis(ConsumerRecord<String, String> record) {
    String value = EnricherHeaders.read(record.headers(), EnricherHeaders.RETRY_NOT_BEFORE);
    if (value == null) {
      return 0;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  private static ProducerRecord<String, String> withTenant(
      ProducerRecord<String, String> record, String tenant) {
    if (record.headers().lastHeader(EnricherHeaders.TENANT) == null) {
      record.headers().add(EnricherHeaders.TENANT, EnricherHeaders.utf8(tenant));
    }
    return record;
  }

  private void pause() {
    try {
      sleeper.sleep(busyRetryInterval);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while waiting for a ledger lease", e);
    }
  }
}
