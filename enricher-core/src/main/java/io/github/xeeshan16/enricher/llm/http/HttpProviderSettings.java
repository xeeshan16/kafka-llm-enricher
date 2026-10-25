package io.github.xeeshan16.enricher.llm.http;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * Connection settings shared by the HTTP providers.
 *
 * @param baseUrl the API root, for example {@code https://api.openai.com/v1}, {@code
 *     http://localhost:11434/v1} (Ollama), {@code https://api.anthropic.com} or an Azure OpenAI
 *     resource endpoint
 * @param apiKey the credential; blank for servers without authentication (local Ollama or vLLM).
 *     Never logged: {@link #toString()} redacts it.
 * @param connectTimeout TCP and TLS connect timeout
 * @param requestTimeout limit for one HTTP exchange, a backstop under the engine's own call timeout
 * @param headers extra headers sent with every request (for example a gateway's routing header)
 */
public record HttpProviderSettings(
    URI baseUrl,
    String apiKey,
    Duration connectTimeout,
    Duration requestTimeout,
    Map<String, String> headers) {

  public HttpProviderSettings {
    Objects.requireNonNull(baseUrl, "baseUrl");
    String scheme = baseUrl.getScheme();
    if ((!"http".equals(scheme) && !"https".equals(scheme)) || baseUrl.getHost() == null) {
      throw new IllegalArgumentException("base URL must be an http or https URL: " + baseUrl);
    }
    apiKey = apiKey == null ? "" : apiKey.strip();
    connectTimeout = connectTimeout == null ? Duration.ofSeconds(10) : connectTimeout;
    requestTimeout = requestTimeout == null ? Duration.ofSeconds(60) : requestTimeout;
    if (connectTimeout.isNegative() || connectTimeout.isZero()) {
      throw new IllegalArgumentException("connectTimeout must be positive");
    }
    if (requestTimeout.isNegative() || requestTimeout.isZero()) {
      throw new IllegalArgumentException("requestTimeout must be positive");
    }
    headers = headers == null ? Map.of() : Map.copyOf(headers);
  }

  public static HttpProviderSettings of(String baseUrl, String apiKey) {
    return new HttpProviderSettings(URI.create(baseUrl), apiKey, null, null, null);
  }

  /** Whether an API key is configured. */
  public boolean hasApiKey() {
    return !apiKey.isEmpty();
  }

  /** {@code baseUrl} joined with {@code path}, with exactly one slash between them. */
  public URI resolve(String path) {
    String base = baseUrl.toString();
    while (base.endsWith("/")) {
      base = base.substring(0, base.length() - 1);
    }
    return URI.create(base + (path.startsWith("/") ? path : "/" + path));
  }

  @Override
  public String toString() {
    return "HttpProviderSettings[baseUrl="
        + baseUrl
        + ", apiKey="
        + (hasApiKey() ? "***" : "<none>")
        + ", connectTimeout="
        + connectTimeout
        + ", requestTimeout="
        + requestTimeout
        + ", headers="
        + headers.keySet()
        + "]";
  }
}
