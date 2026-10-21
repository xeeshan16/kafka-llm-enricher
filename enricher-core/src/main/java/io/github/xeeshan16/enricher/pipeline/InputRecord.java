package io.github.xeeshan16.enricher.pipeline;

import java.util.Objects;

/**
 * A consumed record, independent of the Kafka client types.
 *
 * @param topic source topic
 * @param partition source partition
 * @param offset source offset
 * @param key record key, may be {@code null}
 * @param value record value as text; JSON values are parsed, anything else is treated as a string
 */
public record InputRecord(String topic, int partition, long offset, String key, String value) {

  public InputRecord {
    Objects.requireNonNull(topic, "topic");
    Objects.requireNonNull(value, "value");
  }
}
