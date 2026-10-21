package io.github.xeeshan16.enricher.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.xeeshan16.enricher.TestPipelines;
import org.junit.jupiter.api.Test;

class OutputSchemaTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void acceptsConformingAndReportsEveryViolation() throws Exception {
    OutputSchema schema = TestPipelines.disputeSchema();

    assertThat(
            schema.validate(
                MAPPER.readTree(
                    "{\"category\":\"fraud\",\"urgency\":\"low\",\"summary\":\"s\","
                        + "\"customer_requests_refund\":false,\"amount\":null}")))
        .isEmpty();
    assertThat(
            schema.validate(
                MAPPER.readTree(
                    "{\"category\":\"fraud\",\"urgency\":\"urgent\",\"summary\":\"\","
                        + "\"customer_requests_refund\":\"yes\",\"extra\":1}")))
        .hasSize(4);
  }

  @Test
  void rootMustBeAnObjectSchema() {
    assertThatThrownBy(() -> OutputSchema.parse("{\"type\":\"string\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("object");
    assertThatThrownBy(() -> OutputSchema.parse("{not json"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
