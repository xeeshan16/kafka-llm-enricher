package io.github.xeeshan16.enricher.kafka.observation;

import io.github.xeeshan16.enricher.kafka.EnricherHeaders;
import io.github.xeeshan16.enricher.kafka.RecordProcessor;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Observes a {@link RecordProcessor}: one {@code enricher.record.process} observation per input
 * record, continuing the producer's trace from the record headers, with one {@code
 * enricher.record.publish} child per output record that writes the trace context into the output's
 * headers. Model calls made while the record is processed are observed as children of the process
 * observation, so a trace reads: upstream producer, process, {@code chat <model>} (one per attempt
 * and repair), send.
 *
 * <p>Failures of the delegate are handed to its {@link RecordProcessor#onFailure} inside the same
 * observation, so the dead-letter record is traced too. If that throws as well, the exception goes
 * to the engine as before, which calls {@link #onFailure} (the delegate's) again.
 */
public final class ObservedRecordProcessor implements RecordProcessor {

  private final RecordProcessor delegate;
  private final String pipeline;
  private final String deadLetterTopic;
  private final ObservationRegistry registry;

  public ObservedRecordProcessor(
      RecordProcessor delegate,
      String pipeline,
      String deadLetterTopic,
      ObservationRegistry registry) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.pipeline = Objects.requireNonNull(pipeline, "pipeline");
    this.deadLetterTopic = Objects.requireNonNull(deadLetterTopic, "deadLetterTopic");
    this.registry = Objects.requireNonNull(registry, "registry");
  }

  public RecordProcessor delegate() {
    return delegate;
  }

  @Override
  public List<ProducerRecord<String, String>> process(ConsumerRecord<String, String> record) {
    RecordProcessContext context = new RecordProcessContext(pipeline, record);
    Observation observation =
        Observation.createNotStarted(
                null, RecordObservationConventions.Process.INSTANCE, () -> context, registry)
            .start();
    try (Observation.Scope ignored = observation.openScope()) {
      List<ProducerRecord<String, String>> outputs;
      try {
        outputs = Objects.requireNonNull(delegate.process(record), "process returned null");
      } catch (RuntimeException e) {
        observation.error(e);
        outputs = Objects.requireNonNull(delegate.onFailure(record, e), "onFailure returned null");
      }
      context.outcome(
          outputs.isEmpty()
              ? RecordRoutes.DROPPED
              : RecordRoutes.of(outputs.get(0), deadLetterTopic));
      for (ProducerRecord<String, String> output : outputs) {
        if (context.tenant() == null) {
          context.tenant(EnricherHeaders.read(output.headers(), EnricherHeaders.TENANT));
        }
        publish(output);
      }
      return outputs;
    } catch (RuntimeException e) {
      observation.error(e);
      throw e;
    } finally {
      observation.stop();
    }
  }

  private void publish(ProducerRecord<String, String> output) {
    RecordPublishContext context =
        new RecordPublishContext(pipeline, RecordRoutes.of(output, deadLetterTopic), output);
    Observation.createNotStarted(
            null, RecordObservationConventions.Publish.INSTANCE, () -> context, registry)
        .start()
        .stop();
  }

  @Override
  public List<ProducerRecord<String, String>> onFailure(
      ConsumerRecord<String, String> record, Throwable error) {
    return delegate.onFailure(record, error);
  }

  @Override
  public long notBeforeMillis(ConsumerRecord<String, String> record) {
    return delegate.notBeforeMillis(record);
  }
}
