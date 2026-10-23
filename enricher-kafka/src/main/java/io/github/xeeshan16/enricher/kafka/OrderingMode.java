package io.github.xeeshan16.enricher.kafka;

/**
 * Which records of one partition must be processed one after another. Records in different lanes
 * run in parallel; records in the same lane run in offset order. Output for a partition is always
 * written, and its offsets committed, in offset order, whatever the mode.
 */
public enum OrderingMode {
  /**
   * One lane per record key within a partition (the default). Records without a key share one lane
   * per partition. Keeps per-entity order (one card, one account) while unrelated keys run in
   * parallel.
   */
  KEY,
  /** One lane per partition: the classic sequential consumer. */
  PARTITION,
  /** One lane per record: maximum parallelism, no processing order. */
  UNORDERED
}
