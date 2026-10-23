package io.github.xeeshan16.enricher.kafka;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Buffered records of one assigned partition, in offset order, with the output of those that
 * finished. Only the finished prefix may be committed: offset {@code n} is committed only when
 * every buffered record before it has finished.
 *
 * <p>Identity matters: when a partition is revoked, lost or rewound, its state object is dropped
 * and a new one is created for later records, so results of work started against the old object are
 * recognised as stale and discarded. Not thread-safe; owned by the poll thread.
 */
final class PartitionState {

  private static final class Slot {
    final long offset;
    List<ProducerRecord<String, String>> outputs;
    boolean done;

    Slot(long offset) {
      this.offset = offset;
    }
  }

  private final TopicPartition partition;
  private final ArrayDeque<Slot> slots = new ArrayDeque<>();
  private final Map<Long, Slot> byOffset = new HashMap<>();

  PartitionState(TopicPartition partition) {
    this.partition = partition;
  }

  TopicPartition partition() {
    return partition;
  }

  /** Buffers a polled record. Returns {@code false} for an offset already buffered. */
  boolean add(ConsumerRecord<String, String> record) {
    if (byOffset.containsKey(record.offset())
        || (!slots.isEmpty() && record.offset() < slots.peekLast().offset)) {
      return false;
    }
    Slot slot = new Slot(record.offset());
    slots.addLast(slot);
    byOffset.put(record.offset(), slot);
    return true;
  }

  /** Records the output of a finished record. */
  void complete(long offset, List<ProducerRecord<String, String>> outputs) {
    Slot slot = byOffset.get(offset);
    if (slot != null) {
      slot.outputs = List.copyOf(outputs);
      slot.done = true;
    }
  }

  /** Buffered records, finished or not, that are not committed yet. */
  int pending() {
    return slots.size();
  }

  /** Offset of the first uncommitted buffered record, or -1 when nothing is buffered. */
  long firstOffset() {
    return slots.isEmpty() ? -1 : slots.peekFirst().offset;
  }

  /**
   * Removes the finished prefix, appends its outputs to {@code sink} in offset order and returns
   * the offset to commit (last removed + 1), or -1 when the first record is still running.
   */
  long drainDone(List<ProducerRecord<String, String>> sink) {
    long next = -1;
    while (!slots.isEmpty() && slots.peekFirst().done) {
      Slot slot = slots.pollFirst();
      byOffset.remove(slot.offset);
      sink.addAll(slot.outputs);
      next = slot.offset + 1;
    }
    return next;
  }
}
