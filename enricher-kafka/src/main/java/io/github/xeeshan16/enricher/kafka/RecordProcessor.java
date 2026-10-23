package io.github.xeeshan16.enricher.kafka;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * What a {@link ConsumerLoop} does with one record. Called on a worker (virtual) thread, possibly
 * concurrently for records in different lanes, so implementations must be thread-safe. Blocking (a
 * model call, a database statement) is expected and does not hold up the poll loop.
 *
 * <p>The returned records are written when the input's offset is committed: in the same Kafka
 * transaction for exactly-once delivery, or before the offset commit for at-least-once. They are
 * written in the input's offset order, so per-partition output order follows input order.
 */
public interface RecordProcessor {

  /**
   * Processes one record.
   *
   * @return records to write for it (an enriched envelope, a retry or dead-letter record), or an
   *     empty list to write nothing
   */
  List<ProducerRecord<String, String>> process(ConsumerRecord<String, String> record);

  /**
   * Called when {@link #process} threw. Must not throw; typically returns a dead-letter record so
   * one poison record cannot block its partition. If it throws anyway, the loop retries the record
   * after a second.
   */
  List<ProducerRecord<String, String>> onFailure(
      ConsumerRecord<String, String> record, Throwable error);

  /**
   * Epoch milliseconds before which the record must not start, for example the due time of a record
   * on a retry topic. The loop holds the record (and its lane) without blocking polls.
   */
  default long notBeforeMillis(ConsumerRecord<String, String> record) {
    return 0;
  }
}
