package io.github.xeeshan16.enricher.llm.fake;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.xeeshan16.enricher.llm.LlmRequest;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Produces a schema-shaped JSON answer from the event in the prompt using keyword rules.
 *
 * <p>It is a stand-in for a model, not a model: enum fields take the value whose words occur most
 * often in the event (falling back to {@code other} when the enum has it), booleans are true when
 * the last word of the property name occurs, numbers take the first number in range and strings
 * take the first sentence of the event's longest text field. That is enough for realistic demos
 * and for measuring pipeline mechanics without network access or keys.
 *
 * <p>The event is read from between the {@code <event>} and {@code </event>} markers that the
 * prompt renderer writes; without markers the whole user prompt is used.
 */
public final class SchemaAwareResponder implements Function<LlmRequest, String> {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final JsonNodeFactory NODES = JsonNodeFactory.instance;
  private static final Pattern NUMBER = Pattern.compile("-?\\d+(?:\\.\\d+)?");
  private static final Pattern WORD_SPLIT = Pattern.compile("[^a-z0-9]+");
  private static final Pattern CAMEL = Pattern.compile("([a-z])([A-Z])");
  private static final int DEFAULT_STRING_LENGTH = 160;

  @Override
  public String apply(LlmRequest request) {
    String eventText = eventBlock(request.userPrompt());
    Event event = new Event(eventText.toLowerCase(Locale.ROOT), longestText(eventText));
    JsonNode schema = request.responseSchema();
    JsonNode answer;
    if (schema == null || schema.isMissingNode() || schema.isNull()) {
      ObjectNode node = NODES.objectNode();
      node.put("summary", firstSentence(event.longestText(), DEFAULT_STRING_LENGTH));
      answer = node;
    } else {
      answer = generate(schema, "", event);
    }
    try {
      return MAPPER.writeValueAsString(answer);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("cannot serialise fake answer", e);
    }
  }

  private record Event(String lowerText, String longestText) {}

  private JsonNode generate(JsonNode schema, String propertyName, Event event) {
    JsonNode enumValues = schema.get("enum");
    if (enumValues != null && enumValues.isArray() && !enumValues.isEmpty()) {
      return pickEnum(enumValues, event);
    }
    switch (primaryType(schema)) {
      case "object" -> {
        ObjectNode node = NODES.objectNode();
        Iterator<Map.Entry<String, JsonNode>> fields = schema.path("properties").fields();
        while (fields.hasNext()) {
          Map.Entry<String, JsonNode> field = fields.next();
          node.set(field.getKey(), generate(field.getValue(), field.getKey(), event));
        }
        return node;
      }
      case "array" -> {
        ArrayNode array = NODES.arrayNode();
        int minItems = schema.path("minItems").asInt(0);
        for (int i = 0; i < minItems; i++) {
          array.add(generate(schema.path("items"), propertyName, event));
        }
        return array;
      }
      case "boolean" -> {
        List<String> words = words(propertyName);
        boolean value = !words.isEmpty() && event.lowerText().contains(words.getLast());
        return NODES.booleanNode(value);
      }
      case "integer", "number" -> {
        return number(schema, primaryType(schema).equals("integer"), event);
      }
      case "null" -> {
        return NODES.nullNode();
      }
      default -> {
        int max = schema.path("maxLength").asInt(DEFAULT_STRING_LENGTH);
        int min = schema.path("minLength").asInt(0);
        String text = firstSentence(event.longestText(), max);
        while (text.length() < min) {
          text = text + ".";
        }
        return NODES.textNode(text);
      }
    }
  }

  private static String primaryType(JsonNode schema) {
    JsonNode type = schema.get("type");
    if (type == null) {
      return schema.has("properties") ? "object" : "string";
    }
    if (type.isArray()) {
      for (JsonNode t : type) {
        if (!"null".equals(t.asText())) {
          return t.asText();
        }
      }
      return "null";
    }
    return type.asText();
  }

  private static JsonNode pickEnum(JsonNode enumValues, Event event) {
    JsonNode best = null;
    int bestScore = 0;
    JsonNode other = null;
    for (JsonNode candidate : enumValues) {
      String value = candidate.asText();
      if ("other".equalsIgnoreCase(value)) {
        other = candidate;
      }
      int score = 0;
      for (String word : words(value)) {
        if (word.length() >= 3 && event.lowerText().contains(word)) {
          score++;
        }
      }
      if (score > bestScore) {
        best = candidate;
        bestScore = score;
      }
    }
    if (best != null) {
      return best.deepCopy();
    }
    if (other != null) {
      return other.deepCopy();
    }
    int index = Math.floorMod(event.lowerText().hashCode(), enumValues.size());
    return enumValues.get(index).deepCopy();
  }

  private static JsonNode number(JsonNode schema, boolean integer, Event event) {
    double min = schema.path("minimum").asDouble(Double.NEGATIVE_INFINITY);
    double max = schema.path("maximum").asDouble(Double.POSITIVE_INFINITY);
    Matcher matcher = NUMBER.matcher(event.lowerText());
    while (matcher.find()) {
      double value = Double.parseDouble(matcher.group());
      if (value >= min && value <= max && (!integer || value == Math.rint(value))) {
        return integer ? NODES.numberNode((long) value) : NODES.numberNode(value);
      }
    }
    double fallback = Double.isInfinite(min) ? 0 : min;
    return integer ? NODES.numberNode((long) Math.ceil(fallback)) : NODES.numberNode(fallback);
  }

  /** Splits {@code customer_requests_refund} or {@code customerRequestsRefund} into words. */
  static List<String> words(String identifier) {
    String spaced = CAMEL.matcher(identifier).replaceAll("$1 $2").toLowerCase(Locale.ROOT);
    List<String> words = new ArrayList<>();
    for (String w : WORD_SPLIT.split(spaced)) {
      if (!w.isEmpty()) {
        words.add(w);
      }
    }
    return words;
  }

  private static String eventBlock(String prompt) {
    int start = prompt.indexOf("<event>");
    int end = prompt.lastIndexOf("</event>");
    if (start >= 0 && end > start) {
      return prompt.substring(start + "<event>".length(), end).trim();
    }
    return prompt;
  }

  private static String longestText(String eventText) {
    try {
      JsonNode node = MAPPER.readTree(eventText);
      String longest = longestString(node);
      return longest.isEmpty() ? eventText : longest;
    } catch (JsonProcessingException e) {
      return eventText;
    }
  }

  private static String longestString(JsonNode node) {
    if (node == null) {
      return "";
    }
    if (node.isTextual()) {
      return node.asText();
    }
    String longest = "";
    for (JsonNode child : node) {
      String candidate = longestString(child);
      if (candidate.length() > longest.length()) {
        longest = candidate;
      }
    }
    return longest;
  }

  private static String firstSentence(String text, int maxLength) {
    String trimmed = text.strip();
    int end = trimmed.length();
    for (int i = 0; i < trimmed.length(); i++) {
      char c = trimmed.charAt(i);
      if (c == '.' || c == '!' || c == '?' || c == '\n') {
        end = i + 1;
        break;
      }
    }
    String sentence = trimmed.substring(0, end).strip();
    return sentence.length() <= maxLength ? sentence : sentence.substring(0, maxLength);
  }
}
