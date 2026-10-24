package io.github.xeeshan16.enricher.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.xeeshan16.enricher.cost.SkipRule;
import io.github.xeeshan16.enricher.cost.TenantResolver;
import io.github.xeeshan16.enricher.engine.PreparedRecord;
import io.github.xeeshan16.enricher.llm.LlmRequest;
import io.github.xeeshan16.enricher.pipeline.InputRecord;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import io.github.xeeshan16.enricher.pipeline.PromptTemplate;
import io.github.xeeshan16.enricher.schema.OutputSchema;
import org.junit.jupiter.api.Test;

class SpelRecordExpressionsTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final PipelineDefinition PIPELINE =
      new PipelineDefinition(
          "p",
          "in",
          "out",
          "dlq",
          "v1",
          "m",
          "",
          PromptTemplate.compile("{{event}}"),
          OutputSchema.parse("{\"type\":\"object\"}"),
          16);

  private static PreparedRecord record(String key, String json) throws Exception {
    return new PreparedRecord(
        PIPELINE,
        new InputRecord("in", 3, 99L, key, json),
        MAPPER.readTree(json),
        "sha256:x",
        new LlmRequest("m", "", json, null, 16));
  }

  @Test
  void skipRulesReadEventFieldsAndTreatMissingOnesAsNull() throws Exception {
    SkipRule small =
        SpelRecordExpressions.skipRule("small", "event.amount != null and event.amount < 5");

    assertThat(small.condition().test(record("k", "{\"amount\":2.5}"))).isTrue();
    assertThat(small.condition().test(record("k", "{\"amount\":12}"))).isFalse();
    assertThat(small.condition().test(record("k", "{\"note\":\"no amount\"}"))).isFalse();
  }

  @Test
  void skipRulesCanUseNestedFieldsIndexingMethodsAndRecordMetadata() throws Exception {
    PreparedRecord r =
        record(
            "card-7",
            "{\"type\":\"STATUS_PING\",\"merchant\":{\"country\":\"AE\"},\"merchant-id\":\"m-1\","
                + "\"note\":\"Please REFUND\"}");

    assertThat(test("event.type == 'STATUS_PING'", r)).isTrue();
    assertThat(test("event.merchant.country == 'AE'", r)).isTrue();
    assertThat(test("event['merchant-id'] == 'm-1'", r)).isTrue();
    assertThat(test("event.note.toLowerCase().contains('refund')", r)).isTrue();
    assertThat(test("key.startsWith('card-') and partition == 3 and offset == 99", r)).isTrue();
    assertThat(test("event.merchant.city == null", r)).isTrue();
  }

  @Test
  void nonBooleanOrNullResultsDoNotSkip() throws Exception {
    assertThat(test("event.missing", record("k", "{}"))).isFalse();
  }

  @Test
  void typeReferencesAndConstructorsAreNotAvailable() throws Exception {
    PreparedRecord r = record("k", "{}");
    SkipRule exit = SpelRecordExpressions.skipRule("exit", "T(java.lang.System).exit(1) == null");
    SkipRule create = SpelRecordExpressions.skipRule("new", "new java.io.File('/tmp').exists()");

    assertThatThrownBy(() -> exit.condition().test(r)).isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> create.condition().test(r)).isInstanceOf(RuntimeException.class);
  }

  @Test
  void invalidExpressionsFailAtStartup() {
    assertThatThrownBy(() -> SpelRecordExpressions.skipRule("bad", "event.amount <"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("skip rule 'bad'");
    assertThatThrownBy(() -> SpelRecordExpressions.skipRule("blank", " "))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void tenantResolverFallsBackToTheDefaultTenant() throws Exception {
    TenantResolver byMerchant = SpelRecordExpressions.tenantResolver("p", "event.merchantId");
    TenantResolver broken = SpelRecordExpressions.tenantResolver("p", "event.merchantId.foo()");

    assertThat(byMerchant.tenantOf(record("k", "{\"merchantId\":\"acme\"}"))).isEqualTo("acme");
    assertThat(byMerchant.tenantOf(record("k", "{\"merchantId\":42}"))).isEqualTo("42");
    assertThat(byMerchant.tenantOf(record("k", "{}"))).isEqualTo(TenantResolver.DEFAULT_TENANT);
    assertThat(byMerchant.tenantOf(record("k", "{\"merchantId\":\" \"}")))
        .isEqualTo(TenantResolver.DEFAULT_TENANT);
    assertThat(broken.tenantOf(record("k", "{\"merchantId\":\"acme\"}")))
        .isEqualTo(TenantResolver.DEFAULT_TENANT);
  }

  @Test
  void nonJsonValuesAreStrings() throws Exception {
    PreparedRecord r =
        new PreparedRecord(
            PIPELINE,
            new InputRecord("in", 0, 1L, null, "plain text"),
            MAPPER.getNodeFactory().textNode("plain text"),
            "sha256:x",
            new LlmRequest("m", "", "plain text", null, 16));
    assertThat(test("event.startsWith('plain')", r)).isTrue();
  }

  private static boolean test(String expression, PreparedRecord r) {
    return SpelRecordExpressions.skipRule("t", expression).condition().test(r);
  }
}
