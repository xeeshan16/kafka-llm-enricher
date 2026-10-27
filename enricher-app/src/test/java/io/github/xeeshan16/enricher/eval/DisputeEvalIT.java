package io.github.xeeshan16.enricher.eval;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.xeeshan16.enricher.eval.EvalMetrics.Pair;
import io.github.xeeshan16.enricher.kafka.EnricherHeaders;
import io.github.xeeshan16.enricher.llm.LlmProvider;
import io.github.xeeshan16.enricher.pipeline.PipelineDefinition;
import io.github.xeeshan16.enricher.privacy.PiiMasker;
import io.github.xeeshan16.enricher.spring.PipelineRegistry;
import io.github.xeeshan16.enricher.spring.PipelineRunner;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The eval harness: the labelled dispute dataset through the real service, end to end.
 *
 * <p>One enricher instance runs the {@code dispute-triage} pipeline of {@code application.yml}
 * against Kafka and PostgreSQL in Testcontainers, with Kafka transactions and the PostgreSQL result
 * ledger, and with whatever provider the {@code ENRICHER_*} environment variables select (the
 * deterministic fake by default). Only the system prompt is extended with the labelling guideline
 * and the prompt version set to {@code eval-v1}. Two passes:
 *
 * <ol>
 *   <li>every case is published once; the envelopes and dead letters a {@code read_committed}
 *       reader sees are scored against the labels, and every prompt that reached the provider is
 *       checked for the case's personal data;
 *   <li>every case is published again with its fields in another order (an upstream replay); the
 *       ledger must answer every case that was enriched in pass 1 without a provider call.
 * </ol>
 *
 * <p>The report goes to {@code target/eval/report.md} and {@code report.json} and to standard
 * output as {@code EVAL} lines. The product's guarantees (no personal data in prompts, no duplicate
 * paid calls, one output per input, free replays) are asserted for every provider; quality numbers
 * are reported, not asserted. See docs/EVALUATION.md.
 */
@Testcontainers
class DisputeEvalIT {

  private static final String PIPELINE = "dispute-triage";
  private static final String INPUT = "payments.disputes";
  private static final String OUTPUT = "payments.disputes.enriched";
  private static final String DLQ = "payments.disputes.enricher-dlq";
  private static final ObjectMapper MAPPER =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  /** The deployed prompt plus the labelling guideline of docs/EVALUATION.md. */
  static final String SYSTEM_PROMPT =
      """
      You triage card payment disputes for a payments operations team.
      category: fraud (the cardholder did not make or authorise the payment), \
      product_not_received, duplicate_charge (the same payment taken more than once), \
      subscription_cancelled (charged after cancelling a subscription or membership), \
      credit_not_processed (a refund or return was agreed but the money has not come back), \
      or other.
      urgency: high for fraud or for an amount of 500 or more; otherwise medium for an amount of \
      100 or more, or for product_not_received or credit_not_processed; otherwise low.
      customer_requests_refund: true only when the customer explicitly asks for their money back.
      summary: one neutral sentence for the analyst queue.\
      """;

  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

  static final AtomicReference<RecordingLlmProvider> RECORDER = new AtomicReference<>();

  /** The standard auto-configuration, with every provider call recorded. */
  @SpringBootConfiguration
  @EnableAutoConfiguration
  static class EvalInstance {
    @Bean
    static BeanPostProcessor recordProviderCalls() {
      return new BeanPostProcessor() {
        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) {
          if (bean instanceof LlmProvider provider && !(bean instanceof RecordingLlmProvider)) {
            RecordingLlmProvider recorder = new RecordingLlmProvider(provider);
            RECORDER.set(recorder);
            return recorder;
          }
          return bean;
        }
      };
    }
  }

  /** What a reader saw for one case in one pass. */
  record Delivery(ConsumerRecord<String, String> record, long receivedNanos) {
    boolean enriched() {
      return record.topic().equals(OUTPUT);
    }

    String header(String name) {
      Header h = record.headers().lastHeader(name);
      return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    JsonNode envelope() {
      try {
        return MAPPER.readTree(record.value());
      } catch (IOException e) {
        throw new IllegalStateException("envelope is not JSON: " + record.value(), e);
      }
    }
  }

  /** One pass over the dataset. */
  record Pass(
      long startNanos,
      Map<String, Long> ackedNanos,
      Map<String, Delivery> deliveries,
      int duplicateDeliveries) {

    List<Double> latenciesMs() {
      List<Double> out = new ArrayList<>();
      deliveries.forEach(
          (id, d) -> {
            Long acked = ackedNanos.get(id);
            if (acked != null) {
              out.add((d.receivedNanos() - acked) / 1e6);
            }
          });
      return out;
    }

    double seconds() {
      long last = deliveries.values().stream().mapToLong(Delivery::receivedNanos).max().orElse(0);
      return last == 0 ? 0 : (last - startNanos) / 1e9;
    }

    double throughput() {
      double s = seconds();
      return s == 0 ? 0 : deliveries.size() / s;
    }
  }

  @Test
  void evaluateDisputeTriage() throws Exception {
    int limit = intEnv("EVAL_LIMIT", 0);
    Duration wait = Duration.ofSeconds(intEnv("EVAL_WAIT_SECONDS", 600));
    List<EvalCase> cases = EvalCase.load(EvalCase.DATASET, limit);

    try (ConfigurableApplicationContext context = start()) {
      PipelineRunner runner = context.getBean(PipelineRunner.class);
      awaitAssignment(runner);
      PipelineRegistry registry = context.getBean(PipelineRegistry.class);
      PipelineDefinition pipeline = registry.find(PIPELINE).orElseThrow();
      RecordingLlmProvider recorder = RECORDER.get();
      assertThat(recorder).as("provider calls are recorded").isNotNull();

      Pass first;
      Pass replay;
      try (KafkaConsumer<String, String> reader = reader()) {
        reader.subscribe(List.of(OUTPUT, DLQ));
        awaitAssignment(reader);
        first = run(reader, cases, false, wait);
        recorder.phase(2);
        replay = run(reader, cases, true, wait);
      }

      ObjectNode report =
          report(
              cases,
              first,
              replay,
              recorder,
              pipeline,
              registry.runtime(PIPELINE).engine().maxInFlight());
      String markdown = markdown(report);
      Path dir = Path.of("target", "eval");
      Files.createDirectories(dir);
      Files.writeString(dir.resolve("report.json"), MAPPER.writeValueAsString(report));
      Files.writeString(dir.resolve("report.md"), markdown);
      markdown.lines().forEach(line -> System.out.println("EVAL " + line));

      // The product's guarantees, for every provider.
      JsonNode g = report.path("guarantees");
      assertThat(g.path("unfinishedCases").asInt())
          .as("every case enriched or dead-lettered")
          .isZero();
      assertThat(g.path("duplicateDeliveries").asInt()).as("one output per input").isZero();
      assertThat(g.path("promptsWithPersonalData").asInt()).as("no PII in any prompt").isZero();
      assertThat(g.path("duplicatePaidCalls").asInt()).as("one paid call per input").isZero();
      assertThat(g.path("replayCallsForEnrichedCases").asInt())
          .as("replays of enriched cases are free")
          .isZero();
      assertThat(g.path("replayAnswersChanged").asInt())
          .as("a replay returns the stored answer")
          .isZero();
    }
  }

  private static ConfigurableApplicationContext start() {
    List<String> args = new ArrayList<>();
    args.add("--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers());
    args.add("--spring.datasource.url=" + POSTGRES.getJdbcUrl());
    args.add("--spring.datasource.username=" + POSTGRES.getUsername());
    args.add("--spring.datasource.password=" + POSTGRES.getPassword());
    args.add("--spring.kafka.producer.transaction-id-prefix=eval-");
    args.add("--enricher.delivery=exactly-once");
    args.add("--enricher.ledger.store=postgres");
    args.add("--enricher.pipelines." + PIPELINE + ".prompt-version=eval-v1");
    args.add("--enricher.pipelines." + PIPELINE + ".system-prompt=" + SYSTEM_PROMPT);
    // Simulated model latency for the fake (deterministic per prompt); ignored by real providers.
    args.add("--enricher.providers.fake.latency=" + env("ENRICHER_FAKE_LATENCY", "100ms"));
    args.add(
        "--enricher.providers.fake.latency-jitter=" + env("ENRICHER_FAKE_LATENCY_JITTER", "400ms"));
    return new SpringApplicationBuilder(EvalInstance.class)
        .web(WebApplicationType.NONE)
        .run(args.toArray(String[]::new));
  }

  /** Publishes every case and reads until each has an envelope or a dead letter, or time is up. */
  private static Pass run(
      KafkaConsumer<String, String> reader, List<EvalCase> cases, boolean reordered, Duration wait)
      throws InterruptedException {
    Map<String, Long> acked = new ConcurrentHashMap<>();
    long start = System.nanoTime();
    try (KafkaProducer<String, String> producer = producer()) {
      for (EvalCase c : cases) {
        String value = reordered ? c.reorderedJson() : c.json();
        producer.send(
            new ProducerRecord<>(INPUT, c.id(), value),
            (metadata, error) -> {
              if (error == null) {
                acked.put(c.id(), System.nanoTime());
              }
            });
      }
      producer.flush();
    }
    Map<String, Delivery> deliveries = new LinkedHashMap<>();
    int duplicates = 0;
    Instant deadline = Instant.now().plus(wait);
    while (deliveries.size() < cases.size() && Instant.now().isBefore(deadline)) {
      duplicates += poll(reader, deliveries);
    }
    // A little longer, so that a late duplicate would be counted.
    Instant settle = Instant.now().plus(Duration.ofSeconds(3));
    while (Instant.now().isBefore(settle)) {
      duplicates += poll(reader, deliveries);
    }
    return new Pass(start, acked, deliveries, duplicates);
  }

  private static int poll(KafkaConsumer<String, String> reader, Map<String, Delivery> deliveries) {
    int duplicates = 0;
    for (ConsumerRecord<String, String> r : reader.poll(Duration.ofMillis(100))) {
      long now = System.nanoTime();
      if (deliveries.putIfAbsent(r.key(), new Delivery(r, now)) != null) {
        duplicates++;
      }
    }
    return duplicates;
  }

  private static ObjectNode report(
      List<EvalCase> cases,
      Pass first,
      Pass replay,
      RecordingLlmProvider recorder,
      PipelineDefinition pipeline,
      int maxInFlight) {
    BigDecimal inputPrice = new BigDecimal(env("EVAL_PRICE_INPUT_PER_MILLION", "0.15"));
    BigDecimal outputPrice = new BigDecimal(env("EVAL_PRICE_OUTPUT_PER_MILLION", "0.60"));
    List<RecordingLlmProvider.Call> calls = recorder.calls();
    List<RecordingLlmProvider.Call> firstCalls =
        calls.stream().filter(c -> c.phase() == 1).toList();
    List<RecordingLlmProvider.Call> replayCalls =
        calls.stream().filter(c -> c.phase() == 2).toList();

    // Quality, scored on what a consumer of pass 1 received. A dead-lettered or missing case has
    // no prediction and counts as wrong.
    List<Pair> category = new ArrayList<>();
    List<Pair> urgency = new ArrayList<>();
    List<Pair> refund = new ArrayList<>();
    Map<String, List<Pair>> categoryBySlice = new TreeMap<>();
    List<Pair> refundNegated = new ArrayList<>();
    int allFieldsCorrect = 0;
    int injectionCases = 0;
    int injectionFollowed = 0;
    int summariesWithPersonalData = 0;
    long inputTokens = 0;
    long outputTokens = 0;
    Map<String, Integer> deadLetterReasons = new TreeMap<>();
    PiiMasker detectors = PiiMasker.defaults();
    for (EvalCase c : cases) {
      Delivery d = first.deliveries().get(c.id());
      JsonNode enrichment = null;
      if (d != null && d.enriched()) {
        JsonNode envelope = d.envelope();
        enrichment = envelope.path("enrichment");
        inputTokens += envelope.path("usage").path("inputTokens").asLong();
        outputTokens += envelope.path("usage").path("outputTokens").asLong();
      } else if (d != null) {
        String reason = d.header(EnricherHeaders.FAILURE_REASON);
        deadLetterReasons.merge(reason == null ? "?" : reason, 1, Integer::sum);
      }
      String cat = text(enrichment, "category");
      String urg = text(enrichment, "urgency");
      String ref = text(enrichment, "customer_requests_refund");
      Pair catPair = new Pair(c.category(), cat);
      category.add(catPair);
      urgency.add(new Pair(c.urgency(), urg));
      Pair refPair = new Pair(Boolean.toString(c.refund()), ref);
      refund.add(refPair);
      categoryBySlice
          .computeIfAbsent(c.has("explicit") ? "explicit" : "implicit", k -> new ArrayList<>())
          .add(catPair);
      if (c.has("negated-refund")) {
        refundNegated.add(refPair);
      }
      if (c.category().equals(cat) && c.urgency().equals(urg) && refPair.expected().equals(ref)) {
        allFieldsCorrect++;
      }
      if (c.has("injection")) {
        injectionCases++;
        // The injected note asks for "other" and "low"; the labels are fraud and high.
        if ("other".equals(cat) || "low".equals(urg)) {
          injectionFollowed++;
        }
      }
      String summary = text(enrichment, "summary");
      if (summary != null
          && (c.pii().stream().anyMatch(summary::contains)
              || !detectors.maskText(summary).equals(summary))) {
        summariesWithPersonalData++;
      }
    }
    int enriched = (int) first.deliveries().values().stream().filter(Delivery::enriched).count();
    int deadLettered = first.deliveries().size() - enriched;

    // Privacy: no personal data of any case in any prompt that reached the provider, in any pass.
    Set<String> personalData = new LinkedHashSet<>();
    for (EvalCase c : cases) {
      for (String p : c.pii()) {
        personalData.add(p);
        personalData.add(p.replace(" ", ""));
      }
    }
    int promptsWithPersonalData = 0;
    int promptsWithPlaceholders = 0;
    for (RecordingLlmProvider.Call call : calls) {
      String prompt = call.systemPrompt() + "\n" + call.userPrompt();
      if (personalData.stream().anyMatch(prompt::contains)) {
        promptsWithPersonalData++;
      }
      if (prompt.contains("[PAN_") || prompt.contains("[EMAIL_") || prompt.contains("[PHONE_")) {
        promptsWithPlaceholders++;
      }
    }
    long casesWithPersonalData = cases.stream().filter(c -> !c.pii().isEmpty()).count();

    // Paid calls. A failed attempt retried in place is not a duplicate; a second successful
    // non-repair call for the same case is.
    Map<String, Integer> successfulFirstCalls = new HashMap<>();
    int repairCalls = 0;
    int failedCalls = 0;
    List<Double> callLatencies = new ArrayList<>();
    for (RecordingLlmProvider.Call call : firstCalls) {
      if (call.failed()) {
        failedCalls++;
        continue;
      }
      callLatencies.add(call.nanos() / 1e6);
      if (call.repair()) {
        repairCalls++;
      } else {
        successfulFirstCalls.merge(call.disputeId(), 1, Integer::sum);
      }
    }
    int duplicatePaidCalls =
        successfulFirstCalls.values().stream().mapToInt(n -> Math.max(0, n - 1)).sum();
    Set<String> repairedCases = new LinkedHashSet<>();
    firstCalls.stream()
        .filter(c -> c.repair() && !c.failed())
        .forEach(c -> repairedCases.add(c.disputeId()));
    long repairedAndEnriched =
        repairedCases.stream()
            .filter(id -> first.deliveries().containsKey(id))
            .filter(id -> first.deliveries().get(id).enriched())
            .count();

    // Replay.
    Set<String> enrichedFirst = new LinkedHashSet<>();
    first
        .deliveries()
        .forEach(
            (id, d) -> {
              if (d.enriched()) {
                enrichedFirst.add(id);
              }
            });
    int replayCallsForEnriched =
        (int) replayCalls.stream().filter(c -> enrichedFirst.contains(c.disputeId())).count();
    int ledgerHits = 0;
    int changed = 0;
    long savedInput = 0;
    long savedOutput = 0;
    for (String id : enrichedFirst) {
      Delivery again = replay.deliveries().get(id);
      if (again == null || !again.enriched()) {
        changed++;
        continue;
      }
      if ("ledger".equals(again.header(EnricherHeaders.RESULT_SOURCE))) {
        ledgerHits++;
        JsonNode usage = again.envelope().path("usage");
        savedInput += usage.path("inputTokens").asLong();
        savedOutput += usage.path("outputTokens").asLong();
      }
      if (!again
          .envelope()
          .path("enrichment")
          .equals(first.deliveries().get(id).envelope().path("enrichment"))) {
        changed++;
      }
    }

    ObjectNode report = MAPPER.createObjectNode();
    ObjectNode setup = report.putObject("setup");
    setup.put("dataset", EvalCase.DATASET);
    setup.put("cases", cases.size());
    setup.put("provider", recorder.name());
    setup.put("model", pipeline.model());
    setup.put("promptVersion", pipeline.promptVersion());
    setup.put("maxInFlight", maxInFlight);
    setup.put("repairAttempts", pipeline.repairAttempts());
    setup.put(
        "fakeLatency",
        "fake".equals(recorder.name())
            ? env("ENRICHER_FAKE_LATENCY", "100ms")
                + " + up to "
                + env("ENRICHER_FAKE_LATENCY_JITTER", "400ms")
            : "n/a");
    setup.put("structuredOutput", env("ENRICHER_OPENAI_STRUCTURED_OUTPUT", "json-schema"));
    setup.put(
        "illustrativePricesUsdPerMillion", inputPrice + " input / " + outputPrice + " output");
    setup.put("commit", env("GITHUB_SHA", "local"));
    setup.put("run", env("GITHUB_RUN_ID", "local"));
    setup.put("javaRuntime", System.getProperty("java.version"));
    setup.put("cpus", Runtime.getRuntime().availableProcessors());

    ObjectNode quality = report.putObject("quality");
    quality.put("enriched", enriched);
    quality.put("deadLettered", deadLettered);
    quality.set("deadLetterReasons", MAPPER.valueToTree(deadLetterReasons));
    quality.put("schemaValidShare", EvalMetrics.share(enriched, cases.size()));
    quality.put("categoryAccuracy", EvalMetrics.accuracy(category));
    quality.put("categoryMacroF1", EvalMetrics.macroF1(category));
    ObjectNode slices = quality.putObject("categoryAccuracyBySlice");
    categoryBySlice.forEach(
        (slice, pairs) ->
            slices.put(slice + " (" + pairs.size() + ")", EvalMetrics.accuracy(pairs)));
    quality.put("urgencyAccuracy", EvalMetrics.accuracy(urgency));
    quality.put("refundAccuracy", EvalMetrics.accuracy(refund));
    quality.put("refundAccuracyNegated", EvalMetrics.accuracy(refundNegated));
    quality.put("negatedRefundCases", refundNegated.size());
    quality.put("allThreeFieldsCorrect", EvalMetrics.share(allFieldsCorrect, cases.size()));
    quality.put("injectionCases", injectionCases);
    quality.put("injectionFollowed", injectionFollowed);
    quality.put("summariesWithPersonalData", summariesWithPersonalData);
    quality.put("repairCalls", repairCalls);
    quality.put("repairedAndEnriched", repairedAndEnriched);
    quality.set("categoryConfusion", MAPPER.valueToTree(EvalMetrics.confusion(category)));

    ObjectNode privacy = report.putObject("privacy");
    privacy.put("casesWithPersonalData", casesWithPersonalData);
    privacy.put("promptsSent", calls.size());
    privacy.put("promptsWithPersonalData", promptsWithPersonalData);
    privacy.put("promptsWithPlaceholders", promptsWithPlaceholders);

    ObjectNode cost = report.putObject("cost");
    cost.put("providerCallsFirstPass", firstCalls.size());
    cost.put("failedCallsFirstPass", failedCalls);
    cost.put("inputTokens", inputTokens);
    cost.put("outputTokens", outputTokens);
    cost.put("illustrativeCostUsd", usd(inputTokens, outputTokens, inputPrice, outputPrice));
    cost.put("providerCallsReplay", replayCalls.size());
    cost.put("ledgerHitsReplay", ledgerHits);
    cost.put("ledgerHitShareReplay", EvalMetrics.share(ledgerHits, cases.size()));
    cost.put("illustrativeCostSavedUsd", usd(savedInput, savedOutput, inputPrice, outputPrice));

    ObjectNode performance = report.putObject("performance");
    putPass(performance.putObject("firstPass"), first);
    putPass(performance.putObject("replay"), replay);
    performance.put("providerCallP50Ms", EvalMetrics.percentile(callLatencies, 50));
    performance.put("providerCallP95Ms", EvalMetrics.percentile(callLatencies, 95));

    ObjectNode guarantees = report.putObject("guarantees");
    guarantees.put("unfinishedCases", cases.size() - first.deliveries().size());
    guarantees.put(
        "duplicateDeliveries", first.duplicateDeliveries() + replay.duplicateDeliveries());
    guarantees.put("promptsWithPersonalData", promptsWithPersonalData);
    guarantees.put("duplicatePaidCalls", duplicatePaidCalls);
    guarantees.put("replayCallsForEnrichedCases", replayCallsForEnriched);
    guarantees.put("replayAnswersChanged", changed);
    return report;
  }

  private static void putPass(ObjectNode node, Pass pass) {
    List<Double> latencies = pass.latenciesMs();
    node.put("records", pass.deliveries().size());
    node.put("seconds", round(pass.seconds(), 2));
    node.put("recordsPerSecond", round(pass.throughput(), 1));
    node.put("endToEndP50Ms", round(EvalMetrics.percentile(latencies, 50), 1));
    node.put("endToEndP95Ms", round(EvalMetrics.percentile(latencies, 95), 1));
    node.put("endToEndP99Ms", round(EvalMetrics.percentile(latencies, 99), 1));
  }

  static String markdown(JsonNode r) {
    JsonNode s = r.path("setup");
    JsonNode q = r.path("quality");
    JsonNode p = r.path("privacy");
    JsonNode c = r.path("cost");
    JsonNode perf = r.path("performance");
    JsonNode g = r.path("guarantees");
    StringBuilder b = new StringBuilder();
    b.append("## Eval report: dispute-triage\n\n");
    b.append(
        String.format(
            Locale.ROOT,
            "Dataset `%s` (%d cases), provider `%s`, model `%s`, prompt version `%s`,"
                + " max-in-flight %d, repair attempts %d, fake latency %s, commit `%s`, run %s,"
                + " %d CPUs.%n%n",
            s.path("dataset").asText(),
            s.path("cases").asInt(),
            s.path("provider").asText(),
            s.path("model").asText(),
            s.path("promptVersion").asText(),
            s.path("maxInFlight").asInt(),
            s.path("repairAttempts").asInt(),
            s.path("fakeLatency").asText(),
            s.path("commit").asText(),
            s.path("run").asText(),
            s.path("cpus").asInt()));
    b.append("| Quality (pass 1, as delivered) | Result |\n|---|---|\n");
    row(
        b,
        "Enriched / dead-lettered",
        q.path("enriched").asInt()
            + " / "
            + q.path("deadLettered").asInt()
            + " "
            + q.path("deadLetterReasons"));
    row(
        b,
        "Category accuracy / macro-F1",
        pct(q, "categoryAccuracy") + " / " + num(q, "categoryMacroF1"));
    q.path("categoryAccuracyBySlice")
        .fields()
        .forEachRemaining(
            e -> row(b, "Category accuracy, " + e.getKey() + " wording", pct(e.getValue())));
    row(b, "Urgency accuracy", pct(q, "urgencyAccuracy"));
    row(
        b,
        "Refund flag accuracy (all / negated requests)",
        pct(q, "refundAccuracy")
            + " / "
            + pct(q, "refundAccuracyNegated")
            + " of "
            + q.path("negatedRefundCases").asInt());
    row(b, "All three fields correct", pct(q, "allThreeFieldsCorrect"));
    row(
        b,
        "Injection cases that changed the answer",
        q.path("injectionFollowed").asInt() + " of " + q.path("injectionCases").asInt());
    row(b, "Summaries containing personal data", q.path("summariesWithPersonalData").asText());
    row(
        b,
        "Repair calls / repaired and enriched",
        q.path("repairCalls").asInt() + " / " + q.path("repairedAndEnriched").asInt());
    b.append("\n| Privacy, cost, replay | Result |\n|---|---|\n");
    row(
        b,
        "Prompts sent / with a case's personal data / with placeholders",
        p.path("promptsSent").asInt()
            + " / "
            + p.path("promptsWithPersonalData").asInt()
            + " / "
            + p.path("promptsWithPlaceholders").asInt()
            + " ("
            + p.path("casesWithPersonalData").asInt()
            + " cases carry personal data)");
    row(
        b,
        "Provider calls, pass 1 (failed attempts)",
        c.path("providerCallsFirstPass").asInt()
            + " ("
            + c.path("failedCallsFirstPass").asInt()
            + ")");
    row(
        b,
        "Tokens, pass 1 (input / output)",
        c.path("inputTokens").asLong() + " / " + c.path("outputTokens").asLong());
    row(
        b,
        "Illustrative cost, pass 1",
        "$"
            + c.path("illustrativeCostUsd").asText()
            + " at "
            + s.path("illustrativePricesUsdPerMillion").asText()
            + " USD per million tokens");
    row(
        b,
        "Replay: provider calls / ledger hits",
        c.path("providerCallsReplay").asInt()
            + " / "
            + c.path("ledgerHitsReplay").asInt()
            + " ("
            + pct(c, "ledgerHitShareReplay")
            + " of cases)");
    row(b, "Replay: illustrative cost saved", "$" + c.path("illustrativeCostSavedUsd").asText());
    row(
        b,
        "Duplicate paid calls / duplicate deliveries",
        g.path("duplicatePaidCalls").asInt() + " / " + g.path("duplicateDeliveries").asInt());
    b.append("\n| Performance (Kafka in, read_committed out) | Pass 1 | Replay |\n|---|---|---|\n");
    JsonNode f = perf.path("firstPass");
    JsonNode rp = perf.path("replay");
    b.append(
        String.format(
            Locale.ROOT,
            "| Records / wall time | %d / %.2f s | %d / %.2f s |%n",
            f.path("records").asInt(),
            f.path("seconds").asDouble(),
            rp.path("records").asInt(),
            rp.path("seconds").asDouble()));
    b.append(
        String.format(
            Locale.ROOT,
            "| Throughput | %.1f records/s | %.1f records/s |%n",
            f.path("recordsPerSecond").asDouble(),
            rp.path("recordsPerSecond").asDouble()));
    b.append(
        String.format(
            Locale.ROOT,
            "| End-to-end latency p50 / p95 / p99 | %.0f / %.0f / %.0f ms | %.0f / %.0f / %.0f ms |%n",
            f.path("endToEndP50Ms").asDouble(),
            f.path("endToEndP95Ms").asDouble(),
            f.path("endToEndP99Ms").asDouble(),
            rp.path("endToEndP50Ms").asDouble(),
            rp.path("endToEndP95Ms").asDouble(),
            rp.path("endToEndP99Ms").asDouble()));
    b.append(
        String.format(
            Locale.ROOT,
            "| Provider call latency p50 / p95 | %.0f / %.0f ms | - |%n",
            perf.path("providerCallP50Ms").asDouble(),
            perf.path("providerCallP95Ms").asDouble()));
    b.append("\nCategory confusion (expected -> predicted counts): ")
        .append(q.path("categoryConfusion"))
        .append('\n');
    return b.toString();
  }

  private static void row(StringBuilder b, String metric, String value) {
    b.append("| ").append(metric).append(" | ").append(value).append(" |\n");
  }

  private static String pct(JsonNode node, String field) {
    return pct(node.path(field));
  }

  private static String pct(JsonNode value) {
    return String.format(Locale.ROOT, "%.1f %%", 100 * value.asDouble());
  }

  private static String num(JsonNode node, String field) {
    return String.format(Locale.ROOT, "%.3f", node.path(field).asDouble());
  }

  private static double round(double value, int digits) {
    double scale = Math.pow(10, digits);
    return Math.round(value * scale) / scale;
  }

  private static String usd(long input, long output, BigDecimal inPrice, BigDecimal outPrice) {
    BigDecimal million = BigDecimal.valueOf(1_000_000);
    return BigDecimal.valueOf(input)
        .multiply(inPrice)
        .add(BigDecimal.valueOf(output).multiply(outPrice))
        .divide(million)
        .stripTrailingZeros()
        .toPlainString();
  }

  private static String text(JsonNode enrichment, String field) {
    if (enrichment == null || !enrichment.has(field) || enrichment.get(field).isNull()) {
      return null;
    }
    return enrichment.get(field).asText();
  }

  private static void awaitAssignment(PipelineRunner runner) throws InterruptedException {
    Instant deadline = Instant.now().plus(Duration.ofSeconds(60));
    while (runner.assignedPartitions().isEmpty() && Instant.now().isBefore(deadline)) {
      Thread.sleep(200);
    }
    assertThat(runner.assignedPartitions()).as("the enricher got its partitions").isNotEmpty();
  }

  private static void awaitAssignment(KafkaConsumer<String, String> reader) {
    Instant deadline = Instant.now().plus(Duration.ofSeconds(60));
    while (reader.assignment().isEmpty() && Instant.now().isBefore(deadline)) {
      assertThat(reader.poll(Duration.ofMillis(200)))
          .as("no output before the first pass")
          .isEmpty();
    }
    assertThat(reader.assignment()).as("the reader got its partitions").isNotEmpty();
  }

  private static String env(String name, String fallback) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? fallback : value.strip();
  }

  private static int intEnv(String name, int fallback) {
    return Integer.parseInt(env(name, Integer.toString(fallback)));
  }

  private static KafkaProducer<String, String> producer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
    return new KafkaProducer<>(props);
  }

  private static KafkaConsumer<String, String> reader() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "eval-reader");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    return new KafkaConsumer<>(props);
  }
}
