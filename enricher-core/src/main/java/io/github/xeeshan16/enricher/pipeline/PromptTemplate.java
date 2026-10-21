package io.github.xeeshan16.enricher.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A user-prompt template with {@code {{placeholders}}}.
 *
 * <p>Supported placeholders:
 *
 * <ul>
 *   <li>{@code {{event}}}: the whole event, as compact JSON between {@code <event>} and {@code
 *       </event>} markers on their own lines.
 *   <li>{@code {{event.a.b}}}: one field of the event, written as a JSON literal (strings keep
 *       their quotes); a missing field renders as {@code null}.
 *   <li>{@code {{key}}} and {@code {{topic}}}: the record key and source topic as JSON strings.
 * </ul>
 *
 * <p>Event content is untrusted. Every substituted value is JSON-encoded and {@code <} is written
 * as {@code <}, so event text cannot close the {@code <event>} block or pose as new
 * instructions outside a JSON string. Unknown placeholders are rejected when the template is
 * compiled, so typos fail at startup rather than per record.
 */
public final class PromptTemplate {

  private static final Pattern PLACEHOLDER =
      Pattern.compile("\\{\\{\\s*([A-Za-z_][A-Za-z0-9_\\-]*(?:\\.[A-Za-z0-9_\\-]+)*)\\s*}}");
  private static final Set<String> ROOTS = Set.of("event", "key", "topic");
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String source;

  private PromptTemplate(String source) {
    this.source = source;
  }

  /** Compiles and checks {@code source}. */
  public static PromptTemplate compile(String source) {
    if (source == null || source.isBlank()) {
      throw new IllegalArgumentException("prompt template must not be blank");
    }
    List<String> unknown = new ArrayList<>();
    Matcher matcher = PLACEHOLDER.matcher(source);
    while (matcher.find()) {
      String root = matcher.group(1).split("\\.", -1)[0];
      if (!ROOTS.contains(root) || (!root.equals("event") && matcher.group(1).contains("."))) {
        unknown.add(matcher.group(0));
      }
    }
    if (!unknown.isEmpty()) {
      throw new IllegalArgumentException(
          "unknown placeholders " + unknown + "; use {{event}}, {{event.<path>}}, {{key}}, {{topic}}");
    }
    return new PromptTemplate(source);
  }

  public String source() {
    return source;
  }

  /** Renders the template for one record. */
  public String render(JsonNode event, String key, String topic) {
    Matcher matcher = PLACEHOLDER.matcher(source);
    return matcher.replaceAll(
        match -> Matcher.quoteReplacement(resolve(match.group(1), event, key, topic)));
  }

  private static String resolve(String path, JsonNode event, String key, String topic) {
    return switch (path) {
      case "event" -> "<event>\n" + json(event) + "\n</event>";
      case "key" -> key == null ? "null" : json(MAPPER.getNodeFactory().textNode(key));
      case "topic" -> topic == null ? "null" : json(MAPPER.getNodeFactory().textNode(topic));
      default -> {
        JsonNode node = event;
        String[] parts = path.split("\\.", -1);
        for (int i = 1; i < parts.length; i++) {
          node = node.path(parts[i]);
        }
        yield node.isMissingNode() ? "null" : json(node);
      }
    };
  }

  /** Compact JSON with {@code <} escaped so the text can never contain a closing marker. */
  static String json(JsonNode node) {
    try {
      return MAPPER.writeValueAsString(node).replace("<", "\\u003c");
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("cannot serialise event", e);
    }
  }
}
