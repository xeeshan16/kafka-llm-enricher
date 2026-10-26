package io.github.xeeshan16.enricher.kafka.observation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.xeeshan16.enricher.kafka.EnricherHeaders;
import io.github.xeeshan16.enricher.kafka.RecordProcessor;
import io.micrometer.common.KeyValue;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.transport.ReceiverContext;
import io.micrometer.observation.transport.SenderContext;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;

class ObservedRecordProcessorTest {

  private static final String DLQ = "in.enricher-dlq";

  private final List<Observation.Context> stopped = new CopyOnWriteArrayList<>();
  private final List<String> extracted = new CopyOnWriteArrayList<>();
  private final ObservationRegistry registry = ObservationRegistry.create();

  ObservedRecordProcessorTest() {
    // Stands in for a tracing handler: reads the parent from the input, writes a context into
    // outputs.
    registry
        .observationConfig()
        .observationHandler(
            new ObservationHandler<Observation.Context>() {
              @Override
              public boolean supportsContext(Observation.Context context) {
                return true;
              }

              @Override
              @SuppressWarnings("unchecked")
              public void onStart(Observation.Context context) {
                if (context instanceof ReceiverContext<?> receiver) {
                  ReceiverContext<Object> r = (ReceiverContext<Object>) receiver;
                  extracted.add(String.valueOf(r.getGetter().get(r.getCarrier(), "traceparent")));
                }
                if (context instanceof SenderContext<?> sender) {
                  SenderContext<Object> s = (SenderContext<Object>) sender;
                  s.getSetter().set(s.getCarrier(), "traceparent", "child-of-process");
                }
              }

              @Override
              public void onStop(Observation.Context context) {
                stopped.add(context);
              }
            });
  }

  private static ConsumerRecord<String, String> input(String traceparent) {
    ConsumerRecord<String, String> record = new ConsumerRecord<>("in", 2, 41L, "D-1", "{}");
    if (traceparent != null) {
      record.headers().add("traceparent", traceparent.getBytes(StandardCharsets.UTF_8));
    }
    return record;
  }

  private static ProducerRecord<String, String> output(String topic, String header, String value) {
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, "D-1", "{}");
    if (header != null) {
      record.headers().add(header, value.getBytes(StandardCharsets.UTF_8));
    }
    return record;
  }

  private static RecordProcessor processor(
      Function<ConsumerRecord<String, String>, List<ProducerRecord<String, String>>> process) {
    return new RecordProcessor() {
      @Override
      public List<ProducerRecord<String, String>> process(ConsumerRecord<String, String> record) {
        return process.apply(record);
      }

      @Override
      public List<ProducerRecord<String, String>> onFailure(
          ConsumerRecord<String, String> record, Throwable error) {
        return List.of(output(DLQ, EnricherHeaders.FAILURE_REASON, "PROCESSING_ERROR"));
      }

      @Override
      public long notBeforeMillis(ConsumerRecord<String, String> record) {
        return 1234L;
      }
    };
  }

  private ObservedRecordProcessor observed(RecordProcessor delegate) {
    return new ObservedRecordProcessor(delegate, "triage", DLQ, registry);
  }

  private List<Observation.Context> named(String name) {
    return stopped.stream().filter(c -> name.equals(c.getName())).toList();
  }

  private static String low(Observation.Context context, String key) {
    KeyValue value = context.getLowCardinalityKeyValue(key);
    return value == null ? null : value.getValue();
  }

  private static String header(ProducerRecord<String, String> record, String key) {
    Header header = record.headers().lastHeader(key);
    return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
  }

  @Test
  void continuesTheInputTraceAndPropagatesItToEveryOutput() {
    ProducerRecord<String, String> enriched = output("out", EnricherHeaders.TENANT, "acme");
    List<ProducerRecord<String, String>> outputs =
        observed(processor(r -> List.of(enriched))).process(input("00-upstream-01"));

    assertThat(outputs).containsExactly(enriched);
    assertThat(extracted).containsExactly("00-upstream-01");
    assertThat(header(enriched, "traceparent")).isEqualTo("child-of-process");

    Observation.Context process = named(RecordObservationConventions.PROCESS).get(0);
    assertThat(process.getContextualName()).isEqualTo("process in");
    assertThat(low(process, "messaging.system")).isEqualTo("kafka");
    assertThat(low(process, "messaging.destination.name")).isEqualTo("in");
    assertThat(low(process, "pipeline")).isEqualTo("triage");
    assertThat(low(process, "outcome")).isEqualTo(RecordRoutes.ENRICHED);
    assertThat(low(process, "error.type")).isEqualTo("none");
    assertThat(process.getHighCardinalityKeyValue("messaging.kafka.offset").getValue())
        .isEqualTo("41");
    assertThat(process.getHighCardinalityKeyValue("enricher.tenant").getValue()).isEqualTo("acme");

    Observation.Context publish = named(RecordObservationConventions.PUBLISH).get(0);
    assertThat(publish.getContextualName()).isEqualTo("send out");
    assertThat(publish.getParentObservation().getContextView()).isSameAs(process);
    assertThat(low(publish, "outcome")).isEqualTo(RecordRoutes.ENRICHED);
  }

  @Test
  void replacesATraceContextCopiedFromTheInputIntoARetryRecord() {
    ProducerRecord<String, String> retry =
        output("in.enricher-retry-1", EnricherHeaders.FAILURE_REASON, "PROVIDER_ERROR");
    retry.headers().add("traceparent", "stale".getBytes(StandardCharsets.UTF_8));

    observed(processor(r -> List.of(retry))).process(input(null));

    assertThat(retry.headers().headers("traceparent")).hasSize(1);
    assertThat(header(retry, "traceparent")).isEqualTo("child-of-process");
    assertThat(low(named(RecordObservationConventions.PROCESS).get(0), "outcome"))
        .isEqualTo(RecordRoutes.RETRY);
  }

  @Test
  void classifiesEveryRoute() {
    assertThat(RecordRoutes.of(output("out", null, null), DLQ)).isEqualTo(RecordRoutes.ENRICHED);
    assertThat(RecordRoutes.of(output("out", EnricherHeaders.SKIP_REASON, "rule:tiny"), DLQ))
        .isEqualTo(RecordRoutes.SKIPPED);
    assertThat(
            RecordRoutes.of(
                output("in.enricher-deferred", EnricherHeaders.FAILURE_REASON, "BUDGET_EXCEEDED"),
                DLQ))
        .isEqualTo(RecordRoutes.DEFERRED);
    assertThat(RecordRoutes.of(output(DLQ, EnricherHeaders.FAILURE_REASON, "X"), DLQ))
        .isEqualTo(RecordRoutes.DEAD_LETTER);

    observed(processor(r -> List.of())).process(input(null));
    assertThat(low(named(RecordObservationConventions.PROCESS).get(0), "outcome"))
        .isEqualTo(RecordRoutes.DROPPED);
  }

  @Test
  void aFailingRecordIsDeadLetteredInsideItsObservation() {
    IllegalStateException poison = new IllegalStateException("poison");
    List<ProducerRecord<String, String>> outputs =
        observed(
                processor(
                    r -> {
                      throw poison;
                    }))
            .process(input(null));

    assertThat(outputs).singleElement().satisfies(o -> assertThat(o.topic()).isEqualTo(DLQ));
    assertThat(header(outputs.get(0), "traceparent")).isEqualTo("child-of-process");
    Observation.Context process = named(RecordObservationConventions.PROCESS).get(0);
    assertThat(process.getError()).isSameAs(poison);
    assertThat(low(process, "outcome")).isEqualTo(RecordRoutes.DEAD_LETTER);
    assertThat(low(process, "error.type")).isEqualTo("IllegalStateException");
  }

  @Test
  void aFailingFailureHandlerReachesTheEngine() {
    RecordProcessor broken =
        new RecordProcessor() {
          @Override
          public List<ProducerRecord<String, String>> process(
              ConsumerRecord<String, String> record) {
            throw new IllegalStateException("first");
          }

          @Override
          public List<ProducerRecord<String, String>> onFailure(
              ConsumerRecord<String, String> record, Throwable error) {
            throw new IllegalStateException("second");
          }
        };
    assertThatThrownBy(() -> observed(broken).process(input(null))).hasMessage("second");
    assertThat(named(RecordObservationConventions.PROCESS)).hasSize(1);
  }

  @Test
  void delegatesSchedulingHints() {
    assertThat(observed(processor(r -> List.of())).notBeforeMillis(input(null))).isEqualTo(1234L);
  }
}
