package io.github.xeeshan16.enricher.kafka.observation;

import io.micrometer.observation.transport.SenderContext;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Observation context of one output record (enriched, skipped, retry, deferred or dead-letter).
 * Tracing handlers write the current trace context into its headers, replacing any copied from the
 * input, so consumers of the output topic continue the trace.
 */
public final class RecordPublishContext extends SenderContext<ProducerRecord<String, String>> {

  private final String pipeline;
  private final String route;

  public RecordPublishContext(
      String pipeline, String route, ProducerRecord<String, String> record) {
    super(
        (carrier, key, value) -> {
          if (carrier != null) {
            carrier.headers().remove(key);
            carrier.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
          }
        });
    this.pipeline = Objects.requireNonNull(pipeline, "pipeline");
    this.route = Objects.requireNonNull(route, "route");
    setCarrier(Objects.requireNonNull(record, "record"));
    setRemoteServiceName("kafka");
  }

  public String pipeline() {
    return pipeline;
  }

  /** One of the {@link RecordRoutes} values. */
  public String route() {
    return route;
  }
}
