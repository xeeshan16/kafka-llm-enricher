package io.github.xeeshan16.enricher.kafka;

import java.time.Duration;
import java.util.Objects;

/**
 * Tuning of one {@link ConsumerLoop}.
 *
 * @param ordering which records must run one after another
 * @param maxInFlight records processed concurrently by one loop (one consumer)
 * @param maxPendingPerPartition buffered, uncommitted records per partition before the partition is
 *     paused; it is resumed when the backlog falls to half
 * @param commitInterval how often finished records are committed (one Kafka transaction, or one
 *     offset commit, per interval)
 * @param pollTimeout longest wait in {@code poll()} while nothing is running
 * @param shutdownTimeout how long a stopping loop waits for running records before it commits what
 *     finished and closes
 * @param sendTimeout how long an at-least-once commit waits for the broker to acknowledge output
 */
public record EngineSettings(
    OrderingMode ordering,
    int maxInFlight,
    int maxPendingPerPartition,
    Duration commitInterval,
    Duration pollTimeout,
    Duration shutdownTimeout,
    Duration sendTimeout) {

  public EngineSettings {
    Objects.requireNonNull(ordering, "ordering");
    if (maxInFlight < 1) {
      throw new IllegalArgumentException("maxInFlight must be at least 1: " + maxInFlight);
    }
    if (maxPendingPerPartition < 1) {
      throw new IllegalArgumentException(
          "maxPendingPerPartition must be at least 1: " + maxPendingPerPartition);
    }
    requirePositive(commitInterval, "commitInterval");
    requirePositive(pollTimeout, "pollTimeout");
    Objects.requireNonNull(shutdownTimeout, "shutdownTimeout");
    if (shutdownTimeout.isNegative()) {
      throw new IllegalArgumentException("shutdownTimeout must not be negative");
    }
    requirePositive(sendTimeout, "sendTimeout");
  }

  public static EngineSettings defaults() {
    return new EngineSettings(
        OrderingMode.KEY,
        16,
        500,
        Duration.ofMillis(100),
        Duration.ofMillis(100),
        Duration.ofSeconds(10),
        Duration.ofSeconds(30));
  }

  public EngineSettings withOrdering(OrderingMode value) {
    return new EngineSettings(
        value,
        maxInFlight,
        maxPendingPerPartition,
        commitInterval,
        pollTimeout,
        shutdownTimeout,
        sendTimeout);
  }

  public EngineSettings withMaxInFlight(int value) {
    return new EngineSettings(
        ordering,
        value,
        maxPendingPerPartition,
        commitInterval,
        pollTimeout,
        shutdownTimeout,
        sendTimeout);
  }

  public EngineSettings withMaxPendingPerPartition(int value) {
    return new EngineSettings(
        ordering, maxInFlight, value, commitInterval, pollTimeout, shutdownTimeout, sendTimeout);
  }

  public EngineSettings withCommitInterval(Duration value) {
    return new EngineSettings(
        ordering,
        maxInFlight,
        maxPendingPerPartition,
        value,
        pollTimeout,
        shutdownTimeout,
        sendTimeout);
  }

  public EngineSettings withShutdownTimeout(Duration value) {
    return new EngineSettings(
        ordering,
        maxInFlight,
        maxPendingPerPartition,
        commitInterval,
        pollTimeout,
        value,
        sendTimeout);
  }

  private static void requirePositive(Duration d, String name) {
    Objects.requireNonNull(d, name);
    if (d.isNegative() || d.isZero()) {
      throw new IllegalArgumentException(name + " must be positive");
    }
  }
}
