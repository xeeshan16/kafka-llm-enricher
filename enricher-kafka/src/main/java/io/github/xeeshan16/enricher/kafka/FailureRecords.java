package io.github.xeeshan16.enricher.kafka;

import io.github.xeeshan16.enricher.engine.FailureReason;
import io.github.xeeshan16.enricher.pipeline.InputRecord;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * Builds retry and dead-letter records. Both carry the original key and value unchanged, the
 * original input's coordinates (kept from the first stage, so a record on a second retry topic
 * still points at the input record), and the failure reason, detail and attempt count.
 */
public final class FailureRecords {

  /**
   * Suffix of the topic that parks over-budget records: {@code <input-topic>.enricher-deferred}.
   */
  public static final String DEFERRED_SUFFIX = ".enricher-deferred";

  private static final int MAX_DETAIL = 1000;
  private static final List<String> REPLACED =
      List.of(
          EnricherHeaders.PIPELINE,
          EnricherHeaders.FAILURE_REASON,
          EnricherHeaders.FAILURE_DETAIL,
          EnricherHeaders.ATTEMPTS,
          EnricherHeaders.RETRY_NOT_BEFORE,
          EnricherHeaders.TENANT);

  private final PipelineDefinition pipeline;
  private final RetryTopics retryTopics;
  private final Clock clock;

  public FailureRecords(PipelineDefinition pipeline, RetryTopics retryTopics, Clock clock) {
    this.pipeline = Objects.requireNonNull(pipeline, "pipeline");
    this.retryTopics = Objects.requireNonNull(retryTopics, "retryTopics");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  /** The record as the pipeline first saw it: coordinates from the headers when it was retried. */
  public static InputRecord originalInput(ConsumerRecord<String, String> record) {
    String topic = EnricherHeaders.read(record.headers(), EnricherHeaders.ORIGINAL_TOPIC);
    String partition = EnricherHeaders.read(record.headers(), EnricherHeaders.ORIGINAL_PARTITION);
    String offset = EnricherHeaders.read(record.headers(), EnricherHeaders.ORIGINAL_OFFSET);
    if (topic != null && partition != null && offset != null) {
      try {
        return new InputRecord(
            topic,
            Integer.parseInt(partition),
            Long.parseLong(offset),
            record.key(),
            record.value());
      } catch (NumberFormatException e) {
        // Malformed headers written by something else: fall back to this record's coordinates.
      }
    }
    return new InputRecord(
        record.topic(), record.partition(), record.offset(), record.key(), record.value());
  }

  /** The deferred topic of a pipeline consuming {@code inputTopic}. */
  public static String deferredTopic(String inputTopic) {
    return inputTopic + DEFERRED_SUFFIX;
  }

  /**
   * A record parked on the deferred topic until {@code untilMillis}, when its tenant's budget
   * resets. The pipeline consumes the deferred topic and holds the record until then without
   * blocking anything else.
   */
  public ProducerRecord<String, String> deferred(
      ConsumerRecord<String, String> record,
      int stage,
      String tenant,
      long untilMillis,
      String detail) {
    RecordHeaders headers = headers(record, stage, FailureReason.BUDGET_EXCEEDED, detail);
    headers.add(EnricherHeaders.RETRY_NOT_BEFORE, EnricherHeaders.utf8(Long.toString(untilMillis)));
    headers.add(EnricherHeaders.TENANT, EnricherHeaders.utf8(tenant));
    return new ProducerRecord<>(
        deferredTopic(pipeline.inputTopic()), null, record.key(), record.value(), headers);
  }

  /** Delivery stages tried, including this one, for a record consumed at {@code stage}. */
  static int attempts(int stage) {
    return stage + 1;
  }

  /** A dead-letter record for {@code record}, consumed at retry stage {@code stage}. */
  public ProducerRecord<String, String> deadLetter(
      ConsumerRecord<String, String> record, int stage, FailureReason reason, String detail) {
    RecordHeaders headers = headers(record, stage, reason, detail);
    return new ProducerRecord<>(
        pipeline.deadLetterTopic(), null, record.key(), record.value(), headers);
  }

  /**
   * The next retry stage's record when there is one, otherwise a dead-letter record with {@code
   * exhaustedReason}.
   */
  public ProducerRecord<String, String> retryOrDeadLetter(
      ConsumerRecord<String, String> record,
      int stage,
      FailureReason reason,
      FailureReason exhaustedReason,
      String detail) {
    int next = stage + 1;
    if (next > retryTopics.stages()) {
      return deadLetter(
          record,
          stage,
          exhaustedReason,
          detail + " (gave up after " + attempts(stage) + " delivery attempt(s))");
    }
    RecordHeaders headers = headers(record, stage, reason, detail);
    long notBefore = clock.millis() + retryTopics.delay(next).toMillis();
    headers.add(EnricherHeaders.RETRY_NOT_BEFORE, EnricherHeaders.utf8(Long.toString(notBefore)));
    return new ProducerRecord<>(
        retryTopics.topic(next), null, record.key(), record.value(), headers);
  }

  private RecordHeaders headers(
      ConsumerRecord<String, String> record, int stage, FailureReason reason, String detail) {
    RecordHeaders headers = new RecordHeaders();
    for (Header header : record.headers()) {
      if (!REPLACED.contains(header.key())) {
        headers.add(header);
      }
    }
    if (headers.lastHeader(EnricherHeaders.ORIGINAL_TOPIC) == null) {
      headers.add(EnricherHeaders.ORIGINAL_TOPIC, EnricherHeaders.utf8(record.topic()));
      headers.add(
          EnricherHeaders.ORIGINAL_PARTITION,
          EnricherHeaders.utf8(Integer.toString(record.partition())));
      headers.add(
          EnricherHeaders.ORIGINAL_OFFSET, EnricherHeaders.utf8(Long.toString(record.offset())));
    }
    headers.add(EnricherHeaders.PIPELINE, EnricherHeaders.utf8(pipeline.name()));
    headers.add(EnricherHeaders.FAILURE_REASON, EnricherHeaders.utf8(reason.name()));
    String text = String.valueOf(detail);
    headers.add(
        EnricherHeaders.FAILURE_DETAIL,
        EnricherHeaders.utf8(text.length() <= MAX_DETAIL ? text : text.substring(0, MAX_DETAIL)));
    headers.add(EnricherHeaders.ATTEMPTS, EnricherHeaders.utf8(Integer.toString(attempts(stage))));
    return headers;
  }
}
