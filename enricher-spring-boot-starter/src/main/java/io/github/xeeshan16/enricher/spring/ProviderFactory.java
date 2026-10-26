package io.github.xeeshan16.enricher.spring;

import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.llm.fake.FakeLlmProvider;
import io.github.xeeshan16.enricher.llm.fake.SchemaAwareResponder;
import io.github.xeeshan16.enricher.llm.http.AnthropicProvider;
import io.github.xeeshan16.enricher.llm.http.AzureOpenAiProvider;
import io.github.xeeshan16.enricher.llm.http.HttpLlmProvider;
import io.github.xeeshan16.enricher.llm.http.HttpProviderSettings;
import io.github.xeeshan16.enricher.llm.http.OpenAiCompatibleProvider;
import java.net.URI;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Builds the {@link LlmProvider} named by {@code enricher.provider}. */
final class ProviderFactory {

  private static final Logger log = LoggerFactory.getLogger(ProviderFactory.class);

  static final String SUPPORTED = "fake, openai, anthropic, azure-openai";

  private ProviderFactory() {}

  static LlmProvider create(String name, EnricherProperties.Providers providers) {
    String provider = name == null ? "fake" : name.strip().toLowerCase(Locale.ROOT);
    LlmProvider created =
        switch (provider) {
          case "fake" -> fake(providers.fake());
          case "openai" -> openAi(providers.openai());
          case "anthropic" -> anthropic(providers.anthropic());
          case "azure-openai" -> azure(providers.azureOpenai());
          default ->
              throw new IllegalStateException(
                  "enricher.provider='" + name + "' is not supported; use one of: " + SUPPORTED);
        };
    if (created instanceof HttpLlmProvider http) {
      log.info("LLM provider {}: {}", http.name(), http.settings());
    }
    return created;
  }

  private static FakeLlmProvider fake(EnricherProperties.Fake c) {
    if (!c.latency().isZero() || !c.latencyJitter().isZero()) {
      log.info(
          "LLM provider fake: simulated latency {} + up to {}", c.latency(), c.latencyJitter());
    }
    return new FakeLlmProvider(new SchemaAwareResponder(), c.latency(), c.latencyJitter());
  }

  private static OpenAiCompatibleProvider openAi(EnricherProperties.OpenAi c) {
    HttpProviderSettings settings =
        settings(
            "openai", c.baseUrl(), c.apiKey(), c.connectTimeout(), c.requestTimeout(), c.headers());
    if (!settings.hasApiKey() && settings.baseUrl().getHost().endsWith("openai.com")) {
      throw new IllegalStateException(
          "enricher.providers.openai.api-key is required for "
              + settings.baseUrl()
              + "; leave it empty only for local servers such as Ollama or vLLM");
    }
    return new OpenAiCompatibleProvider(
        settings,
        new OpenAiCompatibleProvider.Options(
            c.structuredOutput(), temperature("openai", c.temperature()), c.maxCompletionTokens()));
  }

  private static AnthropicProvider anthropic(EnricherProperties.Anthropic c) {
    HttpProviderSettings settings =
        settings(
            "anthropic",
            c.baseUrl(),
            c.apiKey(),
            c.connectTimeout(),
            c.requestTimeout(),
            c.headers());
    if (!settings.hasApiKey()) {
      throw new IllegalStateException("enricher.providers.anthropic.api-key is required");
    }
    return new AnthropicProvider(settings, temperature("anthropic", c.temperature()));
  }

  private static AzureOpenAiProvider azure(EnricherProperties.AzureOpenAi c) {
    if (c.endpoint() == null || c.endpoint().isBlank()) {
      throw new IllegalStateException(
          "enricher.providers.azure-openai.endpoint is required, for example"
              + " https://my-resource.openai.azure.com");
    }
    HttpProviderSettings settings =
        settings(
            "azure-openai",
            c.endpoint(),
            c.apiKey(),
            c.connectTimeout(),
            c.requestTimeout(),
            c.headers());
    if (!settings.hasApiKey()) {
      throw new IllegalStateException("enricher.providers.azure-openai.api-key is required");
    }
    return new AzureOpenAiProvider(
        settings,
        new OpenAiCompatibleProvider.Options(
            c.structuredOutput(), temperature("azure-openai", c.temperature()), false),
        c.apiVersion());
  }

  /** A temperature setting: a number, or {@code none} (or empty) to send none. */
  static Double temperature(String provider, String value) {
    if (value == null || value.isBlank() || value.strip().equalsIgnoreCase("none")) {
      return null;
    }
    try {
      return Double.valueOf(value.strip());
    } catch (NumberFormatException e) {
      throw new IllegalStateException(
          "enricher.providers." + provider + ".temperature must be a number or none: " + value, e);
    }
  }

  private static HttpProviderSettings settings(
      String name,
      String baseUrl,
      String apiKey,
      Duration connectTimeout,
      Duration requestTimeout,
      Map<String, String> headers) {
    if (baseUrl == null || baseUrl.isBlank()) {
      throw new IllegalStateException("enricher.providers." + name + " needs a base URL");
    }
    try {
      return new HttpProviderSettings(
          URI.create(baseUrl.strip()), apiKey, connectTimeout, requestTimeout, headers);
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(
          "enricher.providers." + name + ": invalid base URL '" + baseUrl + "'", e);
    }
  }
}
