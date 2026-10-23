package io.github.xeeshan16.enricher.kafka;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Counters of one {@link ConsumerLoop}, safe to read from any thread. Day-6 metrics export these
 * through Micrometer; tests use them to assert behaviour such as "no rebalance happened".
 */
public final class EngineStats {

  final LongAdder polled = new LongAdder();
  final LongAdder processed = new LongAdder();
  final LongAdder discarded = new LongAdder();
  final LongAdder committedInputs = new LongAdder();
  final LongAdder writtenOutputs = new LongAdder();
  final LongAdder commits = new LongAdder();
  final LongAdder commitFailures = new LongAdder();
  final LongAdder rewinds = new LongAdder();
  final LongAdder partitionsAssigned = new LongAdder();
  final LongAdder partitionsRevoked = new LongAdder();
  final LongAdder partitionsLost = new LongAdder();
  final LongAdder pauses = new LongAdder();
  final LongAdder resumes = new LongAdder();
  final AtomicInteger inFlight = new AtomicInteger();
  final AtomicInteger maxInFlightSeen = new AtomicInteger();
  final AtomicInteger pausedPartitions = new AtomicInteger();

  void started() {
    int now = inFlight.incrementAndGet();
    maxInFlightSeen.accumulateAndGet(now, Math::max);
  }

  void finished() {
    inFlight.decrementAndGet();
  }

  /** A point-in-time copy. */
  public Snapshot snapshot() {
    return new Snapshot(
        polled.sum(),
        processed.sum(),
        discarded.sum(),
        committedInputs.sum(),
        writtenOutputs.sum(),
        commits.sum(),
        commitFailures.sum(),
        rewinds.sum(),
        partitionsAssigned.sum(),
        partitionsRevoked.sum(),
        partitionsLost.sum(),
        pauses.sum(),
        resumes.sum(),
        inFlight.get(),
        maxInFlightSeen.get(),
        pausedPartitions.get());
  }

  /**
   * @param polled records returned by {@code poll()}
   * @param processed records whose processing finished and was kept
   * @param discarded finished records dropped because their partition was revoked or rewound
   * @param committedInputs input records whose offsets were committed
   * @param writtenOutputs output, retry and dead-letter records written with those commits
   * @param commits successful commits (transactions, or offset commits)
   * @param commitFailures failed commits; each one rewinds the affected partitions
   * @param rewinds partitions sought back to their last committed offset
   * @param partitionsAssigned partitions assigned by rebalances
   * @param partitionsRevoked partitions revoked by rebalances
   * @param partitionsLost partitions lost without a chance to commit
   * @param pauses partitions paused for backpressure
   * @param resumes partitions resumed
   * @param inFlight records being processed right now
   * @param maxInFlightSeen highest {@code inFlight} observed
   * @param pausedPartitions partitions paused right now
   */
  public record Snapshot(
      long polled,
      long processed,
      long discarded,
      long committedInputs,
      long writtenOutputs,
      long commits,
      long commitFailures,
      long rewinds,
      long partitionsAssigned,
      long partitionsRevoked,
      long partitionsLost,
      long pauses,
      long resumes,
      int inFlight,
      int maxInFlightSeen,
      int pausedPartitions) {

    public static final Snapshot EMPTY =
        new Snapshot(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    /** Sum of two snapshots; {@code maxInFlightSeen} takes the larger value. */
    public Snapshot plus(Snapshot o) {
      return new Snapshot(
          polled + o.polled,
          processed + o.processed,
          discarded + o.discarded,
          committedInputs + o.committedInputs,
          writtenOutputs + o.writtenOutputs,
          commits + o.commits,
          commitFailures + o.commitFailures,
          rewinds + o.rewinds,
          partitionsAssigned + o.partitionsAssigned,
          partitionsRevoked + o.partitionsRevoked,
          partitionsLost + o.partitionsLost,
          pauses + o.pauses,
          resumes + o.resumes,
          inFlight + o.inFlight,
          Math.max(maxInFlightSeen, o.maxInFlightSeen),
          pausedPartitions + o.pausedPartitions);
    }
  }
}
