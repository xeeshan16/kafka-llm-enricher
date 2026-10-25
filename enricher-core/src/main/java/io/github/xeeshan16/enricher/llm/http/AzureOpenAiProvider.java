package io.github.xeeshan16.enricher.llm.http;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.xeeshan16.enricher.llm.LlmRequest;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Objects;

/**
 * Azure OpenAI: the Chat Completions API of a deployment, {@code POST
 * <endpoint>/openai/deployments/<deployment>/chat/completions?api-version=<version>} with an {@code
 * api-key} header.
 *
 * <p>A pipeline's {@code model} is the deployment name, so the result ledger and the price table
 * key on deployments, which is also what Azure bills by.
 */
public final class AzureOpenAiProvider extends OpenAiCompatibleProvider {

  /** A GA API version that supports {@code json_schema} response formats. */
  public static final String DEFAULT_API_VERSION = "2024-10-21";

  private final String apiVersion;

  public AzureOpenAiProvider(HttpProviderSettings settings, Options options, String apiVersion) {
    super(settings, options);
    this.apiVersion = requireVersion(apiVersion);
  }

  public AzureOpenAiProvider(
      HttpProviderSettings settings,
      Options options,
      String apiVersion,
      HttpClient client,
      Clock clock) {
    super(settings, options, client, clock);
    this.apiVersion = requireVersion(apiVersion);
  }

  private static String requireVersion(String apiVersion) {
    Objects.requireNonNull(apiVersion, "apiVersion");
    if (apiVersion.isBlank()) {
      throw new IllegalArgumentException("apiVersion must not be blank");
    }
    return apiVersion.strip();
  }

  @Override
  public String name() {
    return "azure-openai";
  }

  @Override
  protected URI endpoint(LlmRequest request) {
    return settings.resolve(
        "/openai/deployments/"
            + URLEncoder.encode(request.model(), StandardCharsets.UTF_8)
            + "/chat/completions?api-version="
            + URLEncoder.encode(apiVersion, StandardCharsets.UTF_8));
  }

  @Override
  protected void authenticate(HttpRequest.Builder builder) {
    if (settings.hasApiKey()) {
      builder.header("api-key", settings.apiKey());
    }
  }

  @Override
  protected ObjectNode body(LlmRequest request) {
    ObjectNode body = super.body(request);
    // The deployment in the URL selects the model.
    body.remove("model");
    return body;
  }
}
