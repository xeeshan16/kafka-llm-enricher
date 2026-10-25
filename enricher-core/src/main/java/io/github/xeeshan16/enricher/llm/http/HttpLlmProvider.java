package io.github.xeeshan16.enricher.llm.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.LlmProviderException;
import io.github.xeeshan16.enricher.llm.LlmRequest;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import io.github.xeeshan16.enricher.llm.LlmThrottledException;
import io.github.xeeshan16.enricher.llm.LlmTimeoutException;
import io.github.xeeshan16.enricher.llm.TokenEstimator;
import io.github.xeeshan16.enricher.llm.TokenUsage;
import java.io.IOException;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Base class of the providers that speak JSON over HTTP. It owns the transport concerns every
 * provider shares, so the subclasses only map requests and responses:
 *
 * <ul>
 *   <li>HTTP 429 (and whatever {@link #isThrottled} adds, such as Anthropic's 529) becomes {@link
 *       LlmThrottledException} with the provider's {@code Retry-After}, which feeds the adaptive
 *       concurrency limiter;
 *   <li>408, 409 and 5xx are retryable, other 4xx are not (a bad request or key does not improve
 *       with retries);
 *   <li>timeouts become {@link LlmTimeoutException}, connection failures are retryable;
 *   <li>error bodies are cut to a short message, and the API key never appears in exceptions or
 *       logs.
 * </ul>
 *
 * <p>Thread-safe: one {@link HttpClient} is shared by all calls.
 */
public abstract class HttpLlmProvider implements LlmProvider {

  /** Longest {@code Retry-After} honoured; longer hints are capped. */
  static final Duration MAX_RETRY_AFTER = Duration.ofMinutes(10);

  private static final int MAX_ERROR_CHARS = 300;

  protected final HttpProviderSettings settings;
  protected final ObjectMapper mapper;
  private final HttpClient client;
  private final Clock clock;

  protected HttpLlmProvider(HttpProviderSettings settings) {
    this(settings, defaultClient(settings), Clock.systemUTC());
  }

  protected HttpLlmProvider(HttpProviderSettings settings, HttpClient client, Clock clock) {
    this.settings = Objects.requireNonNull(settings, "settings");
    this.client = Objects.requireNonNull(client, "client");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.mapper = new ObjectMapper();
  }

  /** An HTTP/1.1 client using the JVM's proxy settings ({@code https.proxyHost} and friends). */
  public static HttpClient defaultClient(HttpProviderSettings settings) {
    return HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(settings.connectTimeout())
        .proxy(ProxySelector.getDefault())
        .followRedirects(HttpClient.Redirect.NEVER)
        .build();
  }

  public HttpProviderSettings settings() {
    return settings;
  }

  /** Where the request goes. */
  protected abstract URI endpoint(LlmRequest request);

  /** Adds the authentication headers, if a key is configured. */
  protected abstract void authenticate(HttpRequest.Builder builder);

  /** The JSON request body. */
  protected abstract ObjectNode body(LlmRequest request);

  /** Maps a 2xx response body. */
  protected abstract LlmResponse parse(LlmRequest request, JsonNode body, Duration latency);

  /** Whether a non-429 status also means "throttled". */
  protected boolean isThrottled(int status) {
    return false;
  }

  @Override
  public final LlmResponse complete(LlmRequest request) {
    byte[] payload;
    try {
      payload = mapper.writeValueAsBytes(body(request));
    } catch (JsonProcessingException e) {
      throw new LlmProviderException(name() + ": cannot serialise request", false, e);
    }
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(endpoint(request))
            .timeout(settings.requestTimeout())
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .POST(HttpRequest.BodyPublishers.ofByteArray(payload));
    for (Map.Entry<String, String> h : settings.headers().entrySet()) {
      builder.header(h.getKey(), h.getValue());
    }
    authenticate(builder);
    long start = System.nanoTime();
    HttpResponse<String> response;
    try {
      response =
          client.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    } catch (HttpTimeoutException e) {
      throw new LlmTimeoutException(name(), settings.requestTimeout());
    } catch (IOException e) {
      throw new LlmProviderException(
          name() + ": request failed: " + e.getClass().getSimpleName(), true, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new LlmProviderException(name() + ": interrupted", true, e);
    }
    Duration latency = Duration.ofNanos(System.nanoTime() - start);
    int status = response.statusCode();
    if (status >= 200 && status < 300) {
      JsonNode json;
      try {
        json = mapper.readTree(response.body());
      } catch (JsonProcessingException e) {
        throw new LlmProviderException(name() + ": response is not JSON", true, e);
      }
      return parse(request, json, latency);
    }
    String message = name() + " returned HTTP " + status + ": " + errorMessage(response.body());
    Duration retryAfter = retryAfter(response.headers(), clock.instant()).orElse(null);
    if (status == 429 || isThrottled(status)) {
      throw new LlmThrottledException(message, retryAfter);
    }
    boolean retryable = status == 408 || status == 409 || status >= 500;
    throw new LlmProviderException(message, retryable, retryable ? retryAfter : null, null);
  }

  /** Usage as reported, or estimated from the texts when the provider reports none. */
  protected static TokenUsage usage(
      JsonNode usage, String inputField, String outputField, LlmRequest request, String text) {
    if (usage != null && usage.path(inputField).isNumber() && usage.path(outputField).isNumber()) {
      return new TokenUsage(
          Math.max(0, usage.path(inputField).asLong()),
          Math.max(0, usage.path(outputField).asLong()));
    }
    return new TokenUsage(
        TokenEstimator.estimate(request.systemPrompt())
            + TokenEstimator.estimate(request.userPrompt()),
        TokenEstimator.estimate(text));
  }

  /**
   * The system prompt with the response schema appended, for APIs that cannot enforce a schema
   * themselves. Without it the model would only see the schema if the template quoted it.
   */
  protected String systemWithSchema(LlmRequest request) {
    JsonNode schema = request.responseSchema();
    if (schema == null || schema.isNull() || schema.isMissingNode()) {
      return request.systemPrompt();
    }
    String block;
    try {
      block = "Response JSON Schema:\n" + mapper.writeValueAsString(schema);
    } catch (JsonProcessingException e) {
      throw new LlmProviderException(name() + ": cannot serialise schema", false, e);
    }
    return request.systemPrompt().isBlank() ? block : request.systemPrompt() + "\n\n" + block;
  }

  /** The provider's error message from an error body, shortened; never the whole body. */
  String errorMessage(String body) {
    if (body == null || body.isBlank()) {
      return "(empty body)";
    }
    String message = body;
    try {
      JsonNode json = mapper.readTree(body);
      JsonNode error = json.path("error");
      if (error.isTextual()) {
        message = error.asText();
      } else if (error.path("message").isTextual()) {
        message = error.path("message").asText();
      } else if (json.path("message").isTextual()) {
        message = json.path("message").asText();
      }
    } catch (JsonProcessingException e) {
      // Not JSON: use the raw text, shortened below.
    }
    message = message.replaceAll("\\s+", " ").strip();
    return message.length() <= MAX_ERROR_CHARS
        ? message
        : message.substring(0, MAX_ERROR_CHARS) + "...";
  }

  /**
   * The wait a provider asked for: {@code retry-after-ms} (OpenAI, Azure), or {@code Retry-After}
   * as seconds or an HTTP date. Capped at {@link #MAX_RETRY_AFTER}; absent or unparsable hints give
   * empty.
   */
  static Optional<Duration> retryAfter(HttpHeaders headers, Instant now) {
    Optional<String> ms = headers.firstValue("retry-after-ms");
    if (ms.isPresent()) {
      try {
        return Optional.of(cap(Duration.ofMillis((long) Double.parseDouble(ms.get().strip()))));
      } catch (NumberFormatException e) {
        // fall through to Retry-After
      }
    }
    Optional<String> value = headers.firstValue("retry-after");
    if (value.isEmpty()) {
      return Optional.empty();
    }
    String v = value.get().strip();
    try {
      double seconds = Double.parseDouble(v);
      return Optional.of(cap(Duration.ofMillis((long) (seconds * 1000))));
    } catch (NumberFormatException e) {
      // not a number: try an HTTP date
    }
    try {
      Instant at = ZonedDateTime.parse(v, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
      return Optional.of(cap(Duration.between(now, at)));
    } catch (DateTimeParseException e) {
      return Optional.empty();
    }
  }

  private static Duration cap(Duration d) {
    if (d.isNegative()) {
      return Duration.ZERO;
    }
    return d.compareTo(MAX_RETRY_AFTER) > 0 ? MAX_RETRY_AFTER : d;
  }
}
