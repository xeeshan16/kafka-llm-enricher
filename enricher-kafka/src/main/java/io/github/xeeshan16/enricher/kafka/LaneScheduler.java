package io.github.xeeshan16.enricher.kafka;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Decides which buffered task may run next. Tasks belong to lanes; a lane runs at most one task at
 * a time, in the order tasks were added, and a task does not start before its due time. Lanes are
 * independent, so a slow or delayed task holds up only its own lane.
 *
 * <p>Not thread-safe: the consumer loop owns it and calls it from the poll thread only.
 *
 * @param <T> task type
 */
final class LaneScheduler<T> {

  private record Entry<T>(T task, long dueMillis) {}

  private record Delayed(Object lane, long dueMillis) {}

  private final Map<Object, ArrayDeque<Entry<T>>> lanes = new HashMap<>();
  private final Set<Object> busy = new HashSet<>();
  private final ArrayDeque<Object> ready = new ArrayDeque<>();
  private final PriorityQueue<Delayed> delayed =
      new PriorityQueue<>(Comparator.comparingLong(Delayed::dueMillis));
  private int queued;

  /** Appends a task to its lane. */
  void add(Object lane, T task, long dueMillis, long nowMillis) {
    ArrayDeque<Entry<T>> queue = lanes.computeIfAbsent(lane, l -> new ArrayDeque<>());
    queue.addLast(new Entry<>(task, dueMillis));
    queued++;
    if (queue.size() == 1 && !busy.contains(lane)) {
      arm(lane, dueMillis, nowMillis);
    }
  }

  /**
   * Puts a task back at the head of its lane, for a task that must run again before anything behind
   * it. Call before {@link #done} for the same lane.
   */
  void addFirst(Object lane, T task, long dueMillis) {
    lanes.computeIfAbsent(lane, l -> new ArrayDeque<>()).addFirst(new Entry<>(task, dueMillis));
    queued++;
  }

  /** The next task that may start now, or {@code null}. Marks its lane busy. */
  T next(long nowMillis) {
    while (!delayed.isEmpty() && delayed.peek().dueMillis() <= nowMillis) {
      ready.addLast(delayed.poll().lane());
    }
    while (!ready.isEmpty()) {
      Object lane = ready.pollFirst();
      ArrayDeque<Entry<T>> queue = lanes.get(lane);
      if (queue == null || queue.isEmpty() || busy.contains(lane)) {
        continue; // stale: the lane was removed or is already running
      }
      Entry<T> head = queue.peekFirst();
      if (head.dueMillis() > nowMillis) {
        delayed.add(new Delayed(lane, head.dueMillis()));
        continue;
      }
      queue.pollFirst();
      queued--;
      if (queue.isEmpty()) {
        lanes.remove(lane);
      }
      busy.add(lane);
      return head.task();
    }
    return null;
  }

  /** The running task of {@code lane} finished; its next task becomes eligible. */
  void done(Object lane, long nowMillis) {
    if (!busy.remove(lane)) {
      return;
    }
    ArrayDeque<Entry<T>> queue = lanes.get(lane);
    if (queue != null && !queue.isEmpty()) {
      arm(lane, queue.peekFirst().dueMillis(), nowMillis);
    }
  }

  /** Forgets every lane matching {@code which}, queued tasks included. */
  void removeLanes(Predicate<Object> which) {
    Iterator<Map.Entry<Object, ArrayDeque<Entry<T>>>> it = lanes.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Object, ArrayDeque<Entry<T>>> entry = it.next();
      if (which.test(entry.getKey())) {
        queued -= entry.getValue().size();
        it.remove();
      }
    }
    busy.removeIf(which);
    // Stale entries left in ready and delayed are skipped by next().
  }

  /** Due time of the earliest delayed lane head, or {@link Long#MAX_VALUE}. */
  long nextDueMillis() {
    Delayed first = delayed.peek();
    return first == null ? Long.MAX_VALUE : first.dueMillis();
  }

  /** Tasks waiting to start. */
  int queued() {
    return queued;
  }

  /** Lanes with a running task. */
  int running() {
    return busy.size();
  }

  private void arm(Object lane, long dueMillis, long nowMillis) {
    if (dueMillis <= nowMillis) {
      ready.addLast(lane);
    } else {
      delayed.add(new Delayed(lane, dueMillis));
    }
  }
}
