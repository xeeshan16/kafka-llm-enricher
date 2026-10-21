package io.github.xeeshan16.enricher.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import java.util.List;

/**
 * The JSON Schema (draft 2020-12) every enrichment must satisfy.
 *
 * <p>The root must describe an object, because the enrichment is embedded as an object in the
 * output envelope. Instances are immutable and thread-safe.
 */
public final class OutputSchema {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final JsonSchemaFactory FACTORY =
      JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);

  private final JsonNode node;
  private final JsonSchema schema;

  private OutputSchema(JsonNode node) {
    if (!"object".equals(node.path("type").asText())) {
      throw new IllegalArgumentException("output schema root must have \"type\": \"object\"");
    }
    this.node = node.deepCopy();
    this.schema = FACTORY.getSchema(this.node);
  }

  public static OutputSchema of(JsonNode node) {
    return new OutputSchema(node);
  }

  public static OutputSchema parse(String json) {
    try {
      return new OutputSchema(MAPPER.readTree(json));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("output schema is not valid JSON: " + e.getMessage(), e);
    }
  }

  /** The schema document (a defensive copy). */
  public JsonNode node() {
    return node.deepCopy();
  }

  /** Validation errors for {@code instance}, sorted; empty when it conforms. */
  public List<String> validate(JsonNode instance) {
    return schema.validate(instance).stream().map(ValidationMessage::getMessage).sorted().toList();
  }
}
