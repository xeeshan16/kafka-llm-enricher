package io.github.xeeshan16.enricher.kafka.observation;

import io.github.xeeshan16.enricher.kafka.EnricherHeaders;
import io.github.xeeshan16.enricher.kafka.FailureRecords;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Bounded names for where an output record goes, used as the {@code outcome} tag. */
public final class RecordRoutes {

  /** An enriched envelope on the output topic. */
  public static final String ENRICHED = "enriched";

  /** A skipped envelope on the output topic. */
  public static final String SKIPPED = "skipped";

  /** Parked on the deferred topic until a budget window resets. */
  public static final String DEFERRED = "deferred";

  /** Sent to a retry topic. */
  public static final String RETRY = "retry";

  /** Sent to the dead-letter topic. */
  public static final String DEAD_LETTER = "dead_letter";

  /** Nothing written: a tombstone, or a skip with skipped envelopes turned off. */
  public static final String DROPPED = "dropped";

  /** Not known yet. */
  public static final String NONE = "none";

  private RecordRoutes() {}

  static String of(ProducerRecord<String, String> output, String deadLetterTopic) {
    if (output.topic().equals(deadLetterTopic)) {
      return DEAD_LETTER;
    }
    if (output.topic().endsWith(FailureRecords.DEFERRED_SUFFIX)) {
      return DEFERRED;
    }
    if (EnricherHeaders.read(output.headers(), EnricherHeaders.FAILURE_REASON) != null) {
      return RETRY;
    }
    if (EnricherHeaders.read(output.headers(), EnricherHeaders.SKIP_REASON) != null) {
      return SKIPPED;
    }
    return ENRICHED;
  }
}
