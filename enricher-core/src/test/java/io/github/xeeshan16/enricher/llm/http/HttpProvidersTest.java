package io.github.xeeshan16.enricher.llm.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.xeeshan16.enricher.llm.LlmProviderException;
import io.github.xeeshan16.enricher.llm.LlmRequest;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import io.github.xeeshan16.enricher.llm.LlmThrottledException;
import io.github.xeeshan16.enricher.llm.LlmTimeoutException;
import io.github.xeeshan16.enricher.llm.TokenUsage;
import java.net.URI;
import java.net.http.HttpHeaders;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HttpProvidersTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String KEY = "sk-test-not-a-real-key";
  private static final String OPENAI_OK =
      "{\"id\":\"c1\",\"model\":\"gpt-4o-mini-2024-07-18\",\"choices\":[{\"index\":0,"
          + "\"message\":{\"role\":\"assistant\",\"content\":\"{\\\"category\\\":\\\"fraud\\\"}\"},"
          + "\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":120,\"completion_tokens\":9}}";

  private StubHttpServer server;
  private JsonNode schema;

  @BeforeEach
  void start() throws Exception {
    server = new StubHttpServer();
    schema =
        MAPPER.readTree(
            "{\"type\":\"object\",\"properties\":{\"category\":{\"enum\":[\"fraud\",\"other\"]}}}");
  }

  @AfterEach
  void stop() {
    server.close();
  }

  private LlmRequest request(String model) {
    return new LlmRequest(model, "Be precise.", "Classify <event>{}</event>", schema, 64);
  }

  private HttpProviderSettings settings(String path, Duration timeout) {
    return new HttpProviderSettings(
        URI.create(server.baseUrl() + path), KEY, null, timeout, Map.of("X-Team", "payments"));
  }

  private OpenAiCompatibleProvider openAi(OpenAiCompatibleProvider.Options options) {
    return new OpenAiCompatibleProvider(settings("/v1/", Duration.ofSeconds(5)), options);
  }

  @Test
  void openAiSendsSchemaAsResponseFormatAndParsesUsage() throws Exception {
    server.reply(StubHttpServer.Reply.json(200, OPENAI_OK));

    LlmResponse response =
        openAi(OpenAiCompatibleProvider.Options.defaults()).complete(request("gpt-4o-mini"));

    assertThat(response.text()).isEqualTo("{\"category\":\"fraud\"}");
    assertThat(response.model()).isEqualTo("gpt-4o-mini-2024-07-18");
    assertThat(response.usage()).isEqualTo(new TokenUsage(120, 9));

    StubHttpServer.Received sent = server.last();
    assertThat(sent.method()).isEqualTo("POST");
    assertThat(sent.path()).isEqualTo("/v1/chat/completions");
    assertThat(sent.header("Authorization")).isEqualTo("Bearer " + KEY);
    assertThat(sent.header("X-Team")).isEqualTo("payments");
    JsonNode body = MAPPER.readTree(sent.body());
    assertThat(body.path("model").asText()).isEqualTo("gpt-4o-mini");
    assertThat(body.path("max_tokens").asInt()).isEqualTo(64);
    assertThat(body.path("temperature").asDouble()).isEqualTo(0.0);
    assertThat(body.path("messages").get(0).path("role").asText()).isEqualTo("system");
    assertThat(body.path("messages").get(0).path("content").asText()).isEqualTo("Be precise.");
    assertThat(body.path("messages").get(1).path("content").asText())
        .isEqualTo("Classify <event>{}</event>");
    JsonNode format = body.path("response_format");
    assertThat(format.path("type").asText()).isEqualTo("json_schema");
    assertThat(format.path("json_schema").path("schema")).isEqualTo(schema);
  }

  @Test
  void openAiWithoutStructuredOutputPutsTheSchemaInTheSystemPrompt() throws Exception {
    server.reply(StubHttpServer.Reply.json(200, OPENAI_OK));
    OpenAiCompatibleProvider ollama =
        new OpenAiCompatibleProvider(
            new HttpProviderSettings(URI.create(server.baseUrl() + "/v1"), "", null, null, null),
            new OpenAiCompatibleProvider.Options(
                OpenAiCompatibleProvider.StructuredOutput.NONE, null, true));

    ollama.complete(request("llama3.1:8b"));

    StubHttpServer.Received sent = server.last();
    assertThat(sent.header("Authorization")).isNull();
    JsonNode body = MAPPER.readTree(sent.body());
    assertThat(body.has("response_format")).isFalse();
    assertThat(body.has("temperature")).isFalse();
    assertThat(body.path("max_completion_tokens").asInt()).isEqualTo(64);
    assertThat(body.path("messages").get(0).path("content").asText())
        .startsWith("Be precise.\n\nResponse JSON Schema:\n{\"type\":\"object\"");
  }

  @Test
  void missingUsageIsEstimated() {
    server.reply(
        StubHttpServer.Reply.json(
            200,
            "{\"choices\":[{\"message\":{\"content\":\"{\\\"category\\\":\\\"other\\\"}\"}}]}"));

    LlmResponse response =
        openAi(OpenAiCompatibleProvider.Options.defaults()).complete(request("local"));

    assertThat(response.model()).isEqualTo("local");
    assertThat(response.usage().inputTokens()).isPositive();
    assertThat(response.usage().outputTokens()).isEqualTo(5);
  }

  @Test
  void http429BecomesThrottledWithRetryAfter() {
    server.reply(
        StubHttpServer.Reply.json(429, "{\"error\":{\"message\":\"Rate limit reached\"}}")
            .withHeader("Retry-After", "7"));

    Throwable thrown =
        catchThrowable(
            () -> openAi(OpenAiCompatibleProvider.Options.defaults()).complete(request("m")));

    assertThat(thrown).isInstanceOf(LlmThrottledException.class);
    LlmThrottledException throttled = (LlmThrottledException) thrown;
    assertThat(throttled.retryable()).isTrue();
    assertThat(throttled.retryAfter()).contains(Duration.ofSeconds(7));
    assertThat(throttled.getMessage()).isEqualTo("openai returned HTTP 429: Rate limit reached");
  }

  @Test
  void serverErrorsAreRetryableAndClientErrorsAreNot() {
    OpenAiCompatibleProvider provider = openAi(OpenAiCompatibleProvider.Options.defaults());
    server.reply(StubHttpServer.Reply.json(503, "upstream unavailable"));
    server.reply(
        StubHttpServer.Reply.json(401, "{\"error\":{\"message\":\"Incorrect API key provided\"}}"));

    Throwable unavailable = catchThrowable(() -> provider.complete(request("m")));
    Throwable unauthorised = catchThrowable(() -> provider.complete(request("m")));

    assertThat(unavailable).isInstanceOf(LlmProviderException.class);
    assertThat(((LlmProviderException) unavailable).retryable()).isTrue();
    assertThat(unauthorised).isInstanceOf(LlmProviderException.class);
    assertThat(((LlmProviderException) unauthorised).retryable()).isFalse();
    // The key is never part of an error.
    assertThat(unauthorised.getMessage()).doesNotContain(KEY).contains("HTTP 401");
  }

  @Test
  void slowServerTimesOut() {
    server.reply(StubHttpServer.Reply.json(200, OPENAI_OK).after(Duration.ofSeconds(2)));
    OpenAiCompatibleProvider provider =
        new OpenAiCompatibleProvider(
            settings("/v1", Duration.ofMillis(200)), OpenAiCompatibleProvider.Options.defaults());

    assertThatThrownBy(() -> provider.complete(request("m")))
        .isInstanceOf(LlmTimeoutException.class);
  }

  @Test
  void unreachableServerIsRetryable() {
    int port = URI.create(server.baseUrl()).getPort();
    server.close();
    OpenAiCompatibleProvider provider =
        new OpenAiCompatibleProvider(
            HttpProviderSettings.of("http://127.0.0.1:" + port + "/v1", ""),
            OpenAiCompatibleProvider.Options.defaults());

    Throwable thrown = catchThrowable(() -> provider.complete(request("m")));

    assertThat(thrown).isInstanceOf(LlmProviderException.class);
    assertThat(((LlmProviderException) thrown).retryable()).isTrue();
  }

  @Test
  void azureUsesTheDeploymentUrlAndApiKeyHeader() throws Exception {
    server.reply(StubHttpServer.Reply.json(200, OPENAI_OK));
    AzureOpenAiProvider azure =
        new AzureOpenAiProvider(
            settings("", Duration.ofSeconds(5)),
            OpenAiCompatibleProvider.Options.defaults(),
            AzureOpenAiProvider.DEFAULT_API_VERSION);

    LlmResponse response = azure.complete(request("disputes-gpt4o"));

    StubHttpServer.Received sent = server.last();
    assertThat(sent.path()).isEqualTo("/openai/deployments/disputes-gpt4o/chat/completions");
    assertThat(sent.query()).isEqualTo("api-version=2024-10-21");
    assertThat(sent.header("api-key")).isEqualTo(KEY);
    assertThat(sent.header("Authorization")).isNull();
    assertThat(MAPPER.readTree(sent.body()).has("model")).isFalse();
    assertThat(response.text()).isEqualTo("{\"category\":\"fraud\"}");
  }

  @Test
  void anthropicMessagesApi() throws Exception {
    server.reply(
        StubHttpServer.Reply.json(
            200,
            "{\"id\":\"msg_1\",\"type\":\"message\",\"model\":\"claude-test\",\"content\":["
                + "{\"type\":\"text\",\"text\":\"{\\\"category\\\":\"},"
                + "{\"type\":\"text\",\"text\":\"\\\"fraud\\\"}\"}],\"stop_reason\":\"end_turn\","
                + "\"usage\":{\"input_tokens\":200,\"output_tokens\":12}}"));
    AnthropicProvider anthropic = new AnthropicProvider(settings("", Duration.ofSeconds(5)), 0.0);

    LlmResponse response = anthropic.complete(request("claude-test"));

    assertThat(response.text()).isEqualTo("{\"category\":\"fraud\"}");
    assertThat(response.usage()).isEqualTo(new TokenUsage(200, 12));
    StubHttpServer.Received sent = server.last();
    assertThat(sent.path()).isEqualTo("/v1/messages");
    assertThat(sent.header("x-api-key")).isEqualTo(KEY);
    assertThat(sent.header("anthropic-version")).isEqualTo(AnthropicProvider.API_VERSION);
    JsonNode body = MAPPER.readTree(sent.body());
    assertThat(body.path("max_tokens").asInt()).isEqualTo(64);
    assertThat(body.path("system").asText()).startsWith("Be precise.\n\nResponse JSON Schema:");
    assertThat(body.path("messages").get(0).path("role").asText()).isEqualTo("user");
  }

  @Test
  void anthropicOverloadedIsThrottled() {
    server.reply(
        StubHttpServer.Reply.json(
            529,
            "{\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}"));
    AnthropicProvider anthropic = new AnthropicProvider(settings("", Duration.ofSeconds(5)), null);

    assertThatThrownBy(() -> anthropic.complete(request("claude-test")))
        .isInstanceOf(LlmThrottledException.class)
        .hasMessage("anthropic returned HTTP 529: Overloaded");
  }

  @Test
  void retryAfterHeaderForms() {
    Instant now = Instant.parse("2026-10-25T12:00:00Z");
    assertThat(HttpLlmProvider.retryAfter(headers(Map.of("retry-after-ms", "1500")), now))
        .contains(Duration.ofMillis(1500));
    assertThat(HttpLlmProvider.retryAfter(headers(Map.of("retry-after", "2.5")), now))
        .contains(Duration.ofMillis(2500));
    assertThat(
            HttpLlmProvider.retryAfter(
                headers(Map.of("retry-after", "Sun, 25 Oct 2026 12:00:30 GMT")), now))
        .contains(Duration.ofSeconds(30));
    assertThat(HttpLlmProvider.retryAfter(headers(Map.of("retry-after", "86400")), now))
        .contains(HttpLlmProvider.MAX_RETRY_AFTER);
    assertThat(HttpLlmProvider.retryAfter(headers(Map.of("retry-after", "soon")), now)).isEmpty();
    assertThat(HttpLlmProvider.retryAfter(headers(Map.of()), now)).isEmpty();
  }

  @Test
  void settingsNeverPrintTheKey() {
    HttpProviderSettings s = HttpProviderSettings.of("https://api.openai.com/v1/", KEY);
    assertThat(s.toString()).doesNotContain(KEY).contains("apiKey=***");
    assertThat(s.resolve("/chat/completions").toString())
        .isEqualTo("https://api.openai.com/v1/chat/completions");
    assertThatThrownBy(() -> HttpProviderSettings.of("ftp://example.com", ""))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private static HttpHeaders headers(Map<String, String> values) {
    Map<String, List<String>> map = new java.util.HashMap<>();
    values.forEach((k, v) -> map.put(k, List.of(v)));
    return HttpHeaders.of(map, (a, b) -> true);
  }
}
