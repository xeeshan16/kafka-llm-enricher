package io.github.xeeshan16.enricher.eval;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * One labelled dispute of the eval dataset ({@code eval/disputes-v1.jsonl}).
 *
 * @param id dispute id, also the Kafka key and the {@code disputeId} field of the event
 * @param event the input event as published to the input topic
 * @param category expected {@code category}
 * @param urgency expected {@code urgency}
 * @param refund expected {@code customer_requests_refund}
 * @param tags slices: {@code explicit} or {@code implicit} wording, {@code pan}, {@code email},
 *     {@code phone}, {@code negated-refund}, {@code injection}
 * @param pii personal data written into the event, which must never reach a prompt
 */
public record EvalCase(
    String id,
    ObjectNode event,
    String category,
    String urgency,
    boolean refund,
    List<String> tags,
    List<String> pii) {

  public static final String DATASET = "eval/disputes-v1.jsonl";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public EvalCase {
    Objects.requireNonNull(id, "id");
    Objects.requireNonNull(event, "event");
    tags = List.copyOf(tags);
    pii = List.copyOf(pii);
  }

  public boolean has(String tag) {
    return tags.contains(tag);
  }

  /** The event as JSON text, fields in dataset order. */
  public String json() {
    return event.toString();
  }

  /** The same event with its fields in reverse order: a different byte string, the same input. */
  public String reorderedJson() {
    List<Map.Entry<String, JsonNode>> fields = new ArrayList<>();
    Iterator<Map.Entry<String, JsonNode>> it = event.fields();
    while (it.hasNext()) {
      fields.add(it.next());
    }
    Collections.reverse(fields);
    ObjectNode reordered = MAPPER.createObjectNode();
    for (Map.Entry<String, JsonNode> field : fields) {
      reordered.set(field.getKey(), field.getValue());
    }
    return reordered.toString();
  }

  /** Reads the dataset from the class path; {@code limit <= 0} reads every case. */
  public static List<EvalCase> load(String resource, int limit) {
    InputStream in = EvalCase.class.getClassLoader().getResourceAsStream(resource);
    if (in == null) {
      throw new IllegalStateException("dataset not on the class path: " + resource);
    }
    List<EvalCase> cases = new ArrayList<>();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null && (limit <= 0 || cases.size() < limit)) {
        if (line.isBlank()) {
          continue;
        }
        JsonNode row = MAPPER.readTree(line);
        JsonNode labels = row.path("labels");
        cases.add(
            new EvalCase(
                row.path("id").asText(),
                (ObjectNode) row.path("event"),
                labels.path("category").asText(),
                labels.path("urgency").asText(),
                labels.path("customer_requests_refund").asBoolean(),
                strings(row.path("tags")),
                strings(row.path("pii"))));
      }
    } catch (IOException e) {
      throw new UncheckedIOException("cannot read " + resource, e);
    }
    return cases;
  }

  private static List<String> strings(JsonNode array) {
    List<String> out = new ArrayList<>();
    array.forEach(n -> out.add(n.asText()));
    return out;
  }
}
