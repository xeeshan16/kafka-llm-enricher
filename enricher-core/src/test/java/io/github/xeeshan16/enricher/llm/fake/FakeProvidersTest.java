package io.github.xeeshan16.enricher.llm.fake;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.xeeshan16.enricher.TestPipelines;
import io.github.xeeshan16.enricher.llm.EmbeddingResponse;
import io.github.xeeshan16.enricher.llm.LlmRequest;
import io.github.xeeshan16.enricher.llm.LlmResponse;
import io.github.xeeshan16.enricher.schema.OutputSchema;
import java.util.List;
import org.junit.jupiter.api.Test;

class FakeProvidersTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static LlmRequest request(String event, JsonNode schema) {
    return new LlmRequest("fake-1", "sys", "Classify:\n<event>\n" + event + "\n</event>", schema, 64);
  }

  @Test
  void answersAreDeterministicAndSchemaValid() throws Exception {
    OutputSchema schema = TestPipelines.disputeSchema();
    FakeLlmProvider provider = new FakeLlmProvider();
    String[] events = {
      "{\"note\":\"Someone stole my card, this is fraud. Amount 250\"}",
      "{\"note\":\"My subscription was cancelled in May but I keep paying\"}",
      "{\"note\":\"Merchant promised a credit that was never processed\"}",
      "{\"note\":\"\"}",
      "\"plain text event\""
    };
    for (String event : events) {
      LlmResponse first = provider.complete(request(event, schema.node()));
      LlmResponse second = provider.complete(request(event, schema.node()));

      assertThat(first.text()).isEqualTo(second.text());
      assertThat(schema.validate(MAPPER.readTree(first.text()))).as(event).isEmpty();
      assertThat(first.usage().inputTokens()).isPositive();
    }
    assertThat(provider.calls()).isEqualTo(10);
  }

  @Test
  void picksEnumByKeywordsAndFallsBackToOther() throws Exception {
    JsonNode schema = TestPipelines.disputeSchema().node();
    SchemaAwareResponder responder = new SchemaAwareResponder();

    JsonNode fraud = MAPPER.readTree(responder.apply(request("{\"n\":\"Fraudulent use\"}", schema)));
    JsonNode other = MAPPER.readTree(responder.apply(request("{\"n\":\"Hello there\"}", schema)));

    assertThat(fraud.path("category").asText()).isEqualTo("fraud");
    assertThat(other.path("category").asText()).isEqualTo("other");
  }

  @Test
  void withoutSchemaAnswersWithSummary() throws Exception {
    JsonNode answer =
        MAPPER.readTree(
            new SchemaAwareResponder()
                .apply(request("{\"n\":\"First sentence. Second one.\"}", null)));

    assertThat(answer.path("summary").asText()).isEqualTo("First sentence.");
  }

  @Test
  void splitsIdentifiersIntoWords() {
    assertThat(SchemaAwareResponder.words("customer_requests_refund"))
        .containsExactly("customer", "requests", "refund");
    assertThat(SchemaAwareResponder.words("customerRequestsRefund"))
        .containsExactly("customer", "requests", "refund");
  }

  @Test
  void embeddingsAreNormalisedAndShareDirectionForSharedWords() {
    FakeEmbeddingProvider provider = new FakeEmbeddingProvider(64);

    EmbeddingResponse response =
        provider.embed(List.of("duplicate charge refund", "refund duplicate charge", "weather"));

    float[] a = response.vectors().get(0);
    float[] b = response.vectors().get(1);
    float[] c = response.vectors().get(2);
    assertThat(a).hasSize(64);
    assertThat(dot(a, a)).isCloseTo(1.0, org.assertj.core.data.Offset.offset(1e-5));
    assertThat(dot(a, b)).isCloseTo(1.0, org.assertj.core.data.Offset.offset(1e-5));
    assertThat(dot(a, c)).isLessThan(0.9);
    assertThat(provider.embed(List.of("")).vectors().get(0)).containsOnly(0f);
  }

  private static double dot(float[] x, float[] y) {
    double sum = 0;
    for (int i = 0; i < x.length; i++) {
      sum += (double) x[i] * y[i];
    }
    return sum;
  }
}
