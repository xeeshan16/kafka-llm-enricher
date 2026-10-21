package io.github.xeeshan16.enricher;

import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import io.github.xeeshan16.enricher.pipeline.PromptTemplate;
import io.github.xeeshan16.enricher.schema.OutputSchema;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

public final class TestPipelines {

  private TestPipelines() {}

  public static OutputSchema disputeSchema() {
    try (InputStream in = TestPipelines.class.getResourceAsStream("/dispute-triage.schema.json")) {
      if (in == null) {
        throw new IllegalStateException("missing dispute-triage.schema.json");
      }
      return OutputSchema.parse(new String(in.readAllBytes(), StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static PipelineDefinition disputeTriage() {
    return new PipelineDefinition(
        "dispute-triage",
        "payments.disputes",
        "payments.disputes.enriched",
        "payments.disputes.enricher-dlq",
        "v1",
        "fake-1",
        "You triage card payment disputes. Answer with JSON only.",
        PromptTemplate.compile("Classify this dispute for merchant {{event.merchant}}:\n{{event}}"),
        disputeSchema(),
        256);
  }
}
