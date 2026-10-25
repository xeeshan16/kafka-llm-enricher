package io.github.xeeshan16.enricher.privacy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Replaces personal data in an event with placeholders before the event is put into a prompt.
 *
 * <p>Two mechanisms, applied to a copy of the event:
 *
 * <ul>
 *   <li><b>Field masking</b>: the values at the configured paths (for example {@code
 *       customer.name}) are replaced as a whole with {@code [REDACTED]}, whatever they contain.
 *   <li><b>Detectors</b>: every string value is scanned by the {@link PiiDetector}s in order, and
 *       each match becomes {@code [<LABEL>_<n>]}. Numbering is per record and stable: the same
 *       value gets the same placeholder everywhere in the record, so the model can still tell that
 *       two fields hold the same card or email address. Numbers stored as JSON numbers are scanned
 *       too, since card numbers often arrive as such.
 * </ul>
 *
 * <p>Object keys are never changed. Placeholders are deterministic, so the masked prompt (and with
 * it the fake provider's answer) is reproducible. The original values never leave the process
 * through the prompt; they stay in the output envelope's {@code input}, which goes back to Kafka
 * under the same access controls as the input topic.
 *
 * <p>Instances are immutable and thread-safe.
 */
public final class PiiMasker {

  /** Placeholder written for masked fields. */
  public static final String REDACTED = "[REDACTED]";

  /** Masks nothing. */
  public static final PiiMasker NONE = new PiiMasker(List.of(), Set.of());

  private final List<PiiDetector> detectors;
  private final Set<String> fieldPaths;

  /**
   * @param detectors detectors run on every string, in order; earlier detectors win on overlaps
   * @param fieldPaths dot-separated paths of fields masked as a whole, for example {@code
   *     customer.name}; arrays on the way are entered element by element
   */
  public PiiMasker(List<PiiDetector> detectors, Set<String> fieldPaths) {
    this.detectors = List.copyOf(detectors);
    this.fieldPaths = Set.copyOf(fieldPaths);
    for (String path : this.fieldPaths) {
      if (path.isBlank() || path.startsWith(".") || path.endsWith(".")) {
        throw new IllegalArgumentException("invalid mask field path '" + path + "'");
      }
    }
  }

  /** Every built-in detector, no field paths. */
  public static PiiMasker defaults() {
    return new PiiMasker(PiiDetectors.all(), Set.of());
  }

  public List<PiiDetector> detectors() {
    return detectors;
  }

  public Set<String> fieldPaths() {
    return fieldPaths;
  }

  /** Whether this masker can change anything. */
  public boolean enabled() {
    return !detectors.isEmpty() || !fieldPaths.isEmpty();
  }

  /**
   * The result of masking one value.
   *
   * @param value the masked copy (the input itself when nothing was masked)
   * @param counts number of distinct values masked, by label ({@code REDACTED} for field masks)
   */
  public record Masked(JsonNode value, Map<String, Integer> counts) {
    public Masked {
      counts = Map.copyOf(counts);
    }

    /** Total distinct values masked. */
    public int total() {
      return counts.values().stream().mapToInt(Integer::intValue).sum();
    }
  }

  /** Masks a JSON value (object, array, string or scalar). The input is not modified. */
  public Masked mask(JsonNode value) {
    if (!enabled()) {
      return new Masked(value, Map.of());
    }
    State state = new State();
    JsonNode copy = value.deepCopy();
    JsonNode result = walk(copy, "", state);
    return new Masked(result, state.counts());
  }

  /** Masks one piece of free text with the detectors (field paths do not apply). */
  public String maskText(String text) {
    if (detectors.isEmpty()) {
      return text;
    }
    return maskString(text, new State());
  }

  private JsonNode walk(JsonNode node, String path, State state) {
    if (!path.isEmpty() && fieldPaths.contains(path) && !node.isNull()) {
      state.redacted++;
      return JsonNodeFactory.instance.textNode(REDACTED);
    }
    if (node.isObject()) {
      ObjectNode object = (ObjectNode) node;
      List<String> names = new ArrayList<>();
      for (Iterator<String> it = object.fieldNames(); it.hasNext(); ) {
        names.add(it.next());
      }
      for (String name : names) {
        String child = path.isEmpty() ? name : path + "." + name;
        object.set(name, walk(object.get(name), child, state));
      }
      return object;
    }
    if (node.isArray()) {
      ArrayNode array = (ArrayNode) node;
      for (int i = 0; i < array.size(); i++) {
        array.set(i, walk(array.get(i), path, state));
      }
      return array;
    }
    if (node.isTextual()) {
      String masked = maskString(node.textValue(), state);
      return masked.equals(node.textValue()) ? node : JsonNodeFactory.instance.textNode(masked);
    }
    if (node.isIntegralNumber() && !detectors.isEmpty()) {
      String text = node.asText();
      String masked = maskString(text, state);
      return masked.equals(text) ? node : JsonNodeFactory.instance.textNode(masked);
    }
    return node;
  }

  private String maskString(String text, State state) {
    String current = text;
    for (PiiDetector detector : detectors) {
      List<PiiDetector.Span> spans = new ArrayList<>(detector.find(current));
      if (spans.isEmpty()) {
        continue;
      }
      spans.sort(Comparator.comparingInt(PiiDetector.Span::start));
      StringBuilder out = new StringBuilder(current.length());
      int at = 0;
      for (PiiDetector.Span span : spans) {
        if (span.start() < at || span.end() > current.length()) {
          continue; // overlapping or out-of-range span from a custom detector
        }
        out.append(current, at, span.start());
        out.append(
            state.placeholder(detector.label(), current.substring(span.start(), span.end())));
        at = span.end();
      }
      out.append(current, at, current.length());
      current = out.toString();
    }
    return current;
  }

  /** Per-record numbering of placeholders. */
  private static final class State {
    final Map<String, Map<String, String>> byLabel = new LinkedHashMap<>();
    int redacted;

    String placeholder(String label, String original) {
      Map<String, String> values = byLabel.computeIfAbsent(label, k -> new HashMap<>());
      String key = normalise(original);
      return values.computeIfAbsent(key, k -> "[" + label + "_" + (values.size() + 1) + "]");
    }

    Map<String, Integer> counts() {
      Map<String, Integer> counts = new TreeMap<>();
      byLabel.forEach((label, values) -> counts.put(label, values.size()));
      if (redacted > 0) {
        counts.put("REDACTED", redacted);
      }
      return counts;
    }

    /** Same card or phone written with different separators counts as one value. */
    private static String normalise(String s) {
      return s.replaceAll("[\\s().-]", "").toLowerCase(java.util.Locale.ROOT);
    }
  }
}
