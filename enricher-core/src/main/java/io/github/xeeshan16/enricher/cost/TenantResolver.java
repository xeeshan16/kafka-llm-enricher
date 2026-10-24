package io.github.xeeshan16.enricher.cost;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.xeeshan16.enricher.engine.PreparedRecord;

/**
 * Decides which tenant a record's spend is charged to, for budgets and cost accounting. Must be
 * cheap, deterministic and thread-safe. Never returns {@code null} or a blank id.
 */
@FunctionalInterface
public interface TenantResolver {

  /** Tenant used when a pipeline has no resolver or a record carries no tenant. */
  String DEFAULT_TENANT = "default";

  String tenantOf(PreparedRecord record);

  /** Every record belongs to {@code tenant}. */
  static TenantResolver fixed(String tenant) {
    return record -> tenant;
  }

  /**
   * The text at a JSON pointer in the event, for example {@code /merchantId}; {@link
   * #DEFAULT_TENANT} when it is missing, null or blank.
   */
  static TenantResolver jsonPointer(String pointer) {
    com.fasterxml.jackson.core.JsonPointer compiled =
        com.fasterxml.jackson.core.JsonPointer.compile(pointer);
    return record -> {
      JsonNode node = record.input().at(compiled);
      String text = node.isMissingNode() || node.isNull() ? null : node.asText();
      return text == null || text.isBlank() ? DEFAULT_TENANT : text;
    };
  }

  /** The record key; {@link #DEFAULT_TENANT} for records without one. */
  static TenantResolver recordKey() {
    return record -> {
      String key = record.record().key();
      return key == null || key.isBlank() ? DEFAULT_TENANT : key;
    };
  }
}
