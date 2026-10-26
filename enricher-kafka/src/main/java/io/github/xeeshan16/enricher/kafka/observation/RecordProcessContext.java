package io.github.xeeshan16.enricher.kafka.observation;

import io.micrometer.observation.transport.ReceiverContext;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

/**
 * Observation context of one input record being processed. Tracing handlers read the producer's
 * trace context (for example W3C {@code traceparent}) from the record headers and start a {@code
 * CONSUMER} span under it.
 */
public final class RecordProcessContext extends ReceiverContext<ConsumerRecord<String, String>> {

  private final String pipeline;
  private final ConsumerRecord<String, String> record;
  private volatile String outcome = RecordRoutes.NONE;
  private volatile String tenant;

  public RecordProcessContext(String pipeline, ConsumerRecord<String, String> record) {
    super(RecordProcessContext::header);
    this.pipeline = Objects.requireNonNull(pipeline, "pipeline");
    this.record = Objects.requireNonNull(record, "record");
    setCarrier(record);
    setRemoteServiceName("kafka");
  }

  static String header(ConsumerRecord<String, String> carrier, String key) {
    if (carrier == null) {
      return null;
    }
    Header header = carrier.headers().lastHeader(key);
    return header == null || header.value() == null
        ? null
        : new String(header.value(), StandardCharsets.UTF_8);
  }

  public String pipeline() {
    return pipeline;
  }

  public ConsumerRecord<String, String> record() {
    return record;
  }

  /** Where the record went: one of the {@link RecordRoutes} values. */
  public String outcome() {
    return outcome;
  }

  void outcome(String outcome) {
    this.outcome = outcome;
  }

  /** Tenant the record was accounted to, when an output carried one; else {@code null}. */
  public String tenant() {
    return tenant;
  }

  void tenant(String tenant) {
    this.tenant = tenant;
  }
}
