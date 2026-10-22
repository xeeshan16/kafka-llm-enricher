package io.github.xeeshan16.enricher.spring;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.xeeshan16.enricher.engine.EnrichmentOutcome;
import io.github.xeeshan16.enricher.engine.ResultSource;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Publishes envelopes with a {@link KafkaTemplate} and waits for the broker acknowledgement. With a
 * transactional template inside a listener transaction, the record becomes visible to {@code
 * read_committed} consumers only when the transaction, which also carries the input offset,
 * commits.
 */
public final class KafkaEnrichedRecordSink implements EnrichedRecordSink {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final KafkaTemplate<String, String> template;
  private final Duration sendTimeout;

  public KafkaEnrichedRecordSink(KafkaTemplate<String, String> template, Duration sendTimeout) {
    this.template = template;
    this.sendTimeout = sendTimeout;
  }

  @Override
  public void publish(
      PipelineDefinition pipeline,
      String key,
      EnrichmentOutcome.Enriched outcome,
      ResultSource source) {
    String value;
    try {
      value = MAPPER.writeValueAsString(outcome.envelope());
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("cannot serialise envelope", e);
    }
    ProducerRecord<String, String> record =
        new ProducerRecord<>(pipeline.outputTopic(), key, value);
    record.headers().add(EnricherHeaders.PIPELINE, utf8(pipeline.name()));
    record.headers().add(EnricherHeaders.PROMPT_VERSION, utf8(pipeline.promptVersion()));
    record.headers().add(EnricherHeaders.INPUT_HASH, utf8(outcome.inputHash()));
    record
        .headers()
        .add(EnricherHeaders.RESULT_SOURCE, utf8(source.name().toLowerCase(Locale.ROOT)));
    try {
      template.send(record).get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new KafkaException("interrupted while publishing to " + pipeline.outputTopic(), e);
    } catch (ExecutionException | TimeoutException e) {
      throw new KafkaException("cannot publish to " + pipeline.outputTopic(), e);
    }
  }

  private static byte[] utf8(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
