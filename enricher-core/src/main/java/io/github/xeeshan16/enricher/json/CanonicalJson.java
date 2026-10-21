package io.github.xeeshan16.enricher.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * Canonical JSON: object keys sorted at every level, no insignificant whitespace.
 *
 * <p>Two events that differ only in key order or formatting get the same canonical form and so the
 * same {@link #sha256 hash}, which is what lets the result ledger recognise a re-delivered or
 * re-published event. Numbers keep Jackson's textual form, so {@code 1} and {@code 1.0} stay
 * different.
 */
public final class CanonicalJson {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private CanonicalJson() {}

  /** Returns a deep copy of {@code node} with object keys sorted recursively. */
  public static JsonNode canonicalize(JsonNode node) {
    if (node.isObject()) {
      List<Map.Entry<String, JsonNode>> entries = new ArrayList<>();
      node.fields().forEachRemaining(entries::add);
      entries.sort(Map.Entry.comparingByKey());
      ObjectNode sorted = MAPPER.createObjectNode();
      for (Map.Entry<String, JsonNode> e : entries) {
        sorted.set(e.getKey(), canonicalize(e.getValue()));
      }
      return sorted;
    }
    if (node.isArray()) {
      ArrayNode array = MAPPER.createArrayNode();
      for (JsonNode child : node) {
        array.add(canonicalize(child));
      }
      return array;
    }
    return node.deepCopy();
  }

  /** Canonical compact string form. */
  public static String write(JsonNode node) {
    try {
      return MAPPER.writeValueAsString(canonicalize(node));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("cannot serialise JSON", e);
    }
  }

  /** {@code sha256:<hex>} of the canonical form. */
  public static String sha256(JsonNode node) {
    return "sha256:" + sha256Hex(write(node));
  }

  /** Lowercase hex SHA-256 of the UTF-8 bytes of {@code text}. */
  public static String sha256Hex(String text) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return HexFormat.of().formatHex(digest.digest(text.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required by every Java runtime", e);
    }
  }
}
