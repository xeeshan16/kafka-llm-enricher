package io.github.xeeshan16.enricher.kafka;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/** Kafka header names written by the enricher. Values are UTF-8 strings. */
public final class EnricherHeaders {

  /** Pipeline name, on enriched, retry and dead-lettered records. */
  public static final String PIPELINE = "enricher-pipeline";

  /** Prompt version, on enriched records. */
  public static final String PROMPT_VERSION = "enricher-prompt-version";

  /** {@code sha256:<hex>} of the canonical input, on enriched records. */
  public static final String INPUT_HASH = "enricher-input-hash";

  /**
   * {@code provider} when this delivery paid for a model call, {@code ledger} when it reused a
   * stored result. On enriched records.
   */
  public static final String RESULT_SOURCE = "enricher-result-source";

  /**
   * {@link io.github.xeeshan16.enricher.engine.FailureReason} name, on retry and dead-lettered
   * records.
   */
  public static final String FAILURE_REASON = "enricher-failure-reason";

  /** Human-readable failure detail, on retry and dead-lettered records. */
  public static final String FAILURE_DETAIL = "enricher-failure-detail";

  /**
   * Delivery stages tried so far (1 for the input topic, 2 after the first retry topic, ...), on
   * retry and dead-lettered records.
   */
  public static final String ATTEMPTS = "enricher-attempts";

  /** Epoch milliseconds before which a retry record must not be processed. */
  public static final String RETRY_NOT_BEFORE = "enricher-retry-not-before";

  /** Topic of the original input record, on retry and dead-lettered records. */
  public static final String ORIGINAL_TOPIC = "enricher-original-topic";

  /** Partition of the original input record, on retry and dead-lettered records. */
  public static final String ORIGINAL_PARTITION = "enricher-original-partition";

  /** Offset of the original input record, on retry and dead-lettered records. */
  public static final String ORIGINAL_OFFSET = "enricher-original-offset";

  private EnricherHeaders() {}

  /** The last value of header {@code name} as a string, or {@code null}. */
  public static String read(Headers headers, String name) {
    Header header = headers.lastHeader(name);
    return header == null || header.value() == null
        ? null
        : new String(header.value(), StandardCharsets.UTF_8);
  }

  static byte[] utf8(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
