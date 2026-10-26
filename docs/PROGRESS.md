# Progress

## Day 1 (2026-10-21): foundations and first slice

**Built**

- Maven multi-module build with the wrapper (Maven 3.9.11): `enricher-core`, `enricher-spring-boot-starter`, `enricher-app`. Spring Boot 3.5.8 parent, Java 21.
- Static analysis and style: Error Prone 2.36.0 on every compile, Spotless with google-java-format 1.35.0, JaCoCo 0.8.13.
- `enricher-core`
  - `LlmProvider` / `EmbeddingProvider` with request, response and usage types; `LlmProviderException` carries `retryable`.
  - Deterministic fakes: `FakeLlmProvider` (schema-aware keyword responder, call counter) and `FakeEmbeddingProvider` (hashed bag of words, L2-normalised).
  - `PromptTemplate`: `{{event}}`, `{{event.path}}`, `{{key}}`, `{{topic}}`; event data is JSON-encoded and `<` escaped so it cannot close the `<event>` block; unknown placeholders fail at compile time.
  - `OutputSchema` (JSON Schema 2020-12 via networknt), `CanonicalJson` (sorted keys, SHA-256), `Enricher` (render, call, extract JSON from fences or chatter, validate, build a deterministic envelope).
- `enricher-spring-boot-starter`
  - `enricher.*` properties as records, validated at startup with the pipeline name in errors.
  - One `ConcurrentMessageListenerContainer` per pipeline, record ack mode; `DefaultErrorHandler` with a dead-letter recoverer. Unusable answers are dead-lettered at once with `enricher-failure-reason` / `enricher-failure-detail` headers; other errors are retried first.
  - Optional topic creation; every bean is replaceable.
- `enricher-app`: standalone service with the `dispute-triage` example pipeline and schema.
- CI (`.github/workflows/ci.yml`): a format job and a build job (Error Prone compile, unit tests, Testcontainers Kafka integration test, JaCoCo, report artifacts).
- Repo hygiene: LICENSE, CONTRIBUTING, CHANGELOG, issue and PR templates, PLAN, ADR 0001.

**Verified**

- In CI (run [39605714943](https://github.com/xeeshan16/kafka-llm-enricher/actions/runs/39605714943)): 26 core unit tests, 10 starter tests, and the end-to-end integration test (3 disputes in, 2 enriched envelopes out, 1 dead-lettered with `INVALID_JSON`) all pass; formatting check passes.
- Locally only the core module could be compiled and run, against older distro jars and a stub validator (Maven Central is not reachable from the build workspace). Everything else was verified in CI only.

**Notes and plan adjustments**

- The Testcontainers `KafkaContainer` bootstrap advertises `0.0.0.0`, which Kafka 3.9 brokers reject, so the integration test runs on `apache/kafka:3.8.0`. Clients are Spring Boot's managed `kafka-clients` 3.9.1.
- Delivery is at-least-once today. The listener publishes synchronously and commits per record; day 2 replaces this with transactions plus the PostgreSQL result ledger.
- The second commit of the day also added the docs and repository templates, although its message only describes the code fixes.

**Next (day 2):** Kafka read-process-write transactions, PostgreSQL result ledger with Flyway and in-flight leases, chaos test for duplicate paid calls, ADR on ledger vs Kafka-only idempotency.

## Day 2 (2026-10-22): exactly-once delivery and the result ledger

**Built**

- `enricher-core`
  - `Enricher` split into `prepare` (parse, canonical hash, render), the provider call, and `interpret`; `fromStored` rebuilds a byte-identical envelope from a stored answer. Enrichments are written in canonical key order.
  - `ResultLedger` contract (`claim` returning hit / lease / busy, idempotent first-wins `complete`, token-checked `release`, `purgeCompletedBefore`), `InMemoryResultLedger`, `NoopResultLedger`.
  - `LedgeredEnricher`: ledger hit without a call; lease, call, store; short in-place wait on a busy key, then `LedgerBusyException`; a late completion publishes the winning answer.
- `enricher-ledger-postgres` (new module): `PostgresResultLedger` with one auto-committed statement per operation (`INSERT .. ON CONFLICT .. WHERE` for claims and completions), lease expiry on the database clock, and `LedgerSchema` running the module's own Flyway migrations with a separate history table baselined at 0.
- `enricher-spring-boot-starter`
  - Kafka transactions whenever Boot has a Kafka transaction manager; `DefaultAfterRollbackProcessor` with transactional dead-lettering. `enricher.delivery` = `auto` / `exactly-once` / `at-least-once`, each failing fast on a contradicting setup.
  - `enricher.ledger.*` (store, lease TTL, busy wait, poll and retry intervals, schema init, owner). Busy keys retry on a back-off sized to one lease; exhausted ones are dead-lettered as `LEDGER_BUSY`.
  - `enricher-result-source` header.
- `enricher-app`: PostgreSQL datasource, transactions on by default with a random per-process `transaction-id-prefix`, `read_committed` consumers.
- Tests: in-memory ledger and `LedgeredEnricher` unit tests (including concurrent claims), `PostgresResultLedgerIT` (24 concurrent workers on separate connections, lapsed-lease takeover, first-wins completion, purge, migration next to an existing application table), new starter tests for delivery and ledger selection, `DisputeTriagePipelineIT` on Kafka + PostgreSQL with a replayed event answered from the ledger, and `ExactlyOnceChaosIT`.
- ADR 0002: ledger versus Kafka-only idempotency.

**Verified**

- In CI on `dev`: runs [39785366118](https://github.com/xeeshan16/kafka-llm-enricher/actions/runs/39785366118) and [39785919072](https://github.com/xeeshan16/kafka-llm-enricher/actions/runs/39785919072) green (Error Prone, formatting, unit, Testcontainers integration and chaos tests).
- Chaos test (run 39785919072): 40 inputs, 8 aborted-after-store transactions, one worker hung 14 s past a 6 s `max.poll.interval.ms`. Result: 40 envelopes for a `read_committed` reader (49 in the log including aborted attempts), 40 provider calls (one per input), ledger 40 completed rows and 9 hits.
- Locally: the core module compiled and its 43 unit tests ran against distro jars with a stub JSON Schema validator; the ledger SQL and `PostgresResultLedger` were exercised against a local PostgreSQL 16 (24 concurrent claims: 1 lease, 23 busy). The starter, Flyway migration and Kafka tests ran in CI only.

**Notes and plan adjustments**

- The first chaos run failed because `SpringApplicationBuilder.properties()` sets default properties, which lose to `application.yml` placeholders; the test now passes its settings as command-line arguments.
- CI artifacts cannot be downloaded from the build workspace, so the offline Maven cache idea is dropped. The chaos counters and sample envelopes are printed to the job log instead.
- Each record is its own Kafka transaction for now. Day 3's per-key parallelism will revisit transaction batching.

**Next (day 3):** per-key ordered parallelism within a partition, pause/resume so the poll loop never blocks on slow calls, timeouts and full-jitter retries, retry topics and poison-message tests.

## Day 3 (2026-10-23): concurrency, backpressure and retry topics

**Built**

- `enricher-core`
  - `ResilientLlmProvider`: each attempt runs on a virtual thread with a timeout; retryable failures are retried with full-jitter back-off (`FullJitterBackoff`), never sooner than a provider's `Retry-After`.
  - `LlmProviderException` carries an optional retry-after; timeouts surface as a retryable `LlmTimeoutException`. New `FailureReason.RETRIES_EXHAUSTED`.
- `enricher-kafka` (new module, no Spring dependency), see [ADR 0003](adr/0003-own-consumer-loop-with-key-lanes.md)
  - `ConsumerLoop`: one poll thread per consumer that never blocks on a model call. Records are buffered per partition and run on virtual threads by lane (per key within a partition by default, or per partition, or unordered), up to `max-in-flight`.
  - Finished prefixes are written and committed together, one Kafka transaction per `commit-interval`. Partitions with too many uncommitted records are paused and resumed at half. Failed commits abort and rewind; revoked partitions drop their in-flight output.
  - `EnrichmentProcessor` routes every outcome: output topic, `<input>.enricher-retry-N` with a due-time header, or the dead-letter topic with reason, attempt count and original coordinates. `LaneScheduler`, `PartitionState`, `RetryTopics`, `FailureRecords`, `EngineStats`.
- `enricher-spring-boot-starter`: one `ConsumerLoop` per pipeline consumer, transactional ids derived from `spring.kafka.producer.transaction-id-prefix`; per-pipeline `ordering`, `max-in-flight`, `max-pending-per-partition`, `commit-interval`, `retry-delays`, and `enricher.resilience.*`. The Spring Kafka listener, sink and after-rollback wiring are gone.
- Tests: `ConsumerLoopTest` (MockConsumer/MockProducer), `EnrichmentProcessorTest`, `LaneSchedulerTest`, `PartitionStateTest`, `ResilientLlmProviderTest`, `FullJitterBackoffTest`; `ConcurrencyAndRetryIT` on Kafka + PostgreSQL; the chaos test now aborts batched transactions and moves partitions while a call hangs past `max.poll.interval.ms`.

**Verified**

- In CI on `dev`: run [39960069430](https://github.com/xeeshan16/kafka-llm-enricher/actions/runs/39960069430) green for `dacf435` (Error Prone, formatting, unit and Testcontainers tests). `main` was fast-forwarded to `dacf435` at the start of day 4.
- `ConcurrencyAndRetryIT` (same run): 8 keys x 5 records on one partition, one call of 5 s with `max.poll.interval.ms` = 3 s: all 40 records in 7.9 s, up to 8 concurrent calls, 35 records of other keys finished while the slow call ran, 0 partitions revoked or lost, 5 commits. Every key's output in input order.
- Routing in the same run: `transient-1` recovered on the output topic after 5 calls through both retry topics; `always-1` dead-lettered as `RETRIES_EXHAUSTED` after 3 stages (6 calls); `fatal-1` as `PROVIDER_ERROR` and `poison-1` as `PROCESSING_ERROR` after 1 call each, without blocking the record behind it on the same key.
- Chaos test (same run): 40 committed envelopes, 1 provider call per input, 4 aborted transactions, a hung worker, 12 partitions revoked, 0 lost; ledger 40 completed rows, 54 hits.
- Locally: nothing beyond the core unit tests (against distro jars with a stub schema validator).

**Notes and plan adjustments**

- Day 3 replaced Spring Kafka's listener containers with an own loop on `kafka-clients` (ADR 0003): container-managed transactions cannot be combined with asynchronous listeners, and a blocking listener cannot stop slow calls from triggering rebalances.
- At the start of day 4 there was no day-3 progress entry, `main` had not been fast-forwarded and no day-3 Slack post had gone out, although the day-3 code was green on `dev`. Day 4 picked those up first.

## Day 4 (2026-10-24): cost and rate control

**Built**

- `enricher-core`
  - `llm.ratelimit`: `TokenBucket` (reservations with debt, Retry-After blocking), `AimdLimiter` (additive increase, multiplicative decrease with a cooldown), `ProviderRateLimiter` (RPM and TPM buckets plus AIMD; estimates settled against reported usage; refuses waits longer than `max-wait` as retryable), `RateLimitedLlmProvider`, `LlmThrottledException`.
  - `cost`: `PriceTable` (USD per million tokens, cost in micro-USD), `BudgetPeriod`, `BudgetLimit`, `BudgetStore` with `InMemoryBudgetStore`, `TenantBudgets` (worst-case reservations, settle, refund), `TenantResolver`, `SkipRule`, `OverBudgetAction`, `CostPolicy`, `UsageMeter` and `UsageAccounting`.
  - `LedgeredEnricher` applies a `CostPolicy`: skip rules, then the ledger (hits are free), then a budget reservation only for paid calls, then downgrade / skip / defer. `EnrichmentOutcome.Skipped` and `Deferred`, skipped envelopes.
- `enricher-kafka`: skipped envelopes on the output topic (or nothing), deferred records on `<input>.enricher-deferred` with a not-before at the window reset, `enricher-tenant` and `enricher-skip-reason` headers.
- `enricher-ledger-postgres`: `PostgresBudgetStore` (one conditional upsert per charge) and migration V2.
- `enricher-spring-boot-starter`: `enricher.rate-limit.*`, `enricher.pricing.*`, `enricher.budgets.store`, per-pipeline `tenant`, `skip.*`, `emit-skipped`, `budget.*`. SpEL in a `SimpleEvaluationContext` with a null-safe, read-only map accessor; expressions and budget settings are validated at startup. Deferred topics are subscribed to and created when a pipeline can defer.
- `enricher-app`: rate-limit and pricing settings with environment variables, merchant as tenant.
- Tests: `TokenBucketTest`, `AimdLimiterTest`, `ProviderRateLimiterTest` (including convergence against a provider with a hidden capacity), `PriceTableTest`, `BudgetPeriodTest`, `TenantBudgetsTest` (500 concurrent reservations), `UsageAccountingTest`, `LedgeredEnricherCostTest`, `CostRoutingTest`, `SpelRecordExpressionsTest`, new auto-configuration tests, `PostgresBudgetStoreIT`, `CostControlIT`.
- ADR 0004: cost and rate controls before the call, in a fixed order.

**Verified**

- In CI on `dev`: run [40097250912](https://github.com/xeeshan16/kafka-llm-enricher/actions/runs/40097250912) for `6bf16b6`: the build job (Error Prone, unit and all Testcontainers tests) passed on the first push; the formatting job failed on line wrapping only, fixed in the next commit.
- `CostControlIT` (same run): acme 5 x `fake-1`, 1 x `fake-mini`, 6 deferred, spend 10,735 of 12,000 micro-USD; 3 replays answered from the ledger while acme was out of budget; globex 6 enriched; initech 6 skipped with no calls; 12 paid calls, 0 duplicates. Throttling provider (capacity 3): 40 of 40 enriched, 0 dead-lettered, 27 429s, 7 decreases, limit 16 -> 4, at most 3 calls served at once.
- `PostgresBudgetStoreIT` (same run): 16 threads, 640 charges, 100 granted, counter exactly at the 10,000-token cap.
- Locally, against distro jars (Maven Central is blocked in the build workspace): the core module's 90 unit tests (with a stub schema validator); the routing classes of `enricher-kafka` with stub Kafka types and `CostRoutingTest` (5 tests); `SpelRecordExpressionsTest` (7 tests) against spring-expression 4.3; `PostgresBudgetStoreIT` (6 tests) against a local PostgreSQL 16. The starter and the Kafka integration tests ran in CI only.

**Notes and plan adjustments**

- `enricher.rate-limit.max-wait` defaults to 5 s so that the default lease (2 min) still covers a worst-case call: 3 attempts x (30 s timeout + 5 s admission) + back-off = 115 s.
- Rate limits are per instance. A limiter shared across replicas (PostgreSQL or Redis) moves to the roadmap; ADR 0004 explains why it is not needed for v0.1.
- Metrics export of the new counters (`UsageAccounting`, `ProviderRateLimiter.stats()`) is day 6, together with the rest of the Micrometer and OpenTelemetry work.

**Next (day 5):** JSON Schema repair prompt, PII masking before the prompt, the written threat model, and real providers (OpenAI-compatible including Ollama and vLLM, Anthropic, Azure OpenAI) behind the interface, with 429s mapped to `LlmThrottledException`.

## Day 5 (2026-10-25): output quality and safety

**Built**

- `enricher-core`
  - `privacy`: `PiiDetector` (extension point), `PiiDetectors` (PAN with issuer range and Luhn, IBAN with mod 97 and trailing-word handling, email, international and North American phone numbers), `PiiMasker` (detectors plus whole-field redaction by path, stable per-record placeholders such as `[PAN_1]`, card numbers sent as JSON numbers too), `PromptSafety` (masker, injection guard, `OutputPiiAction`).
  - `Enricher` renders the prompt from a masked copy of the event and key and prepends the guard; the hash, skip rules and envelope keep the original. Validated answers are scanned: mask and re-validate, reject as `PII_IN_OUTPUT`, or allow. `repairRequest` quotes the rejected answer and errors as escaped JSON strings.
  - `PipelineDefinition` gains `safety` and `repairAttempts` (0 to 2); the old constructor keeps the previous behaviour. `LedgeredEnricher` runs the repair loop inside the ledger lease, budgets and meters each repair, sums usage into the envelope and ledger entry, and keeps the original failure when a repair cannot be made.
  - `llm.http`: `HttpLlmProvider` (status mapping, `Retry-After` / `retry-after-ms` / HTTP dates capped at 10 min, short error messages, key never printed, no redirects), `OpenAiCompatibleProvider` (OpenAI, Ollama, vLLM; `json_schema`, JSON mode or prompt-only schema), `AzureOpenAiProvider`, `AnthropicProvider` (529 as throttling).
  - `UsageMeter.repairAttempt` / `piiMasked`, `UsageAccounting.quality`.
- `enricher-spring-boot-starter`: `enricher.provider` = `openai` / `anthropic` / `azure-openai` with `enricher.providers.*` (`ProviderFactory`; hosted providers fail startup without credentials); per pipeline `mask-pii`, `mask-fields`, `prompt-guard`, `output-pii`, `repair-attempts`. Default lease 4 min; the lease warning covers repair calls.
- `enricher-app`: provider settings from environment variables only; the demo pipeline is masked, guarded and moved to prompt version `v2`.
- Tests: `PiiMaskerTest` (11), `EnricherSafetyAndRepairTest` (9), `LedgeredEnricherRepairTest` (3), `HttpProvidersTest` (12, against a local `com.sun.net.httpserver` stub), new auto-configuration tests for providers and safety settings; `DisputeTriagePipelineIT` extended with masking and repair.
- Docs: [THREAT-MODEL.md](THREAT-MODEL.md), [ADR 0005](adr/0005-mask-before-the-prompt-repair-after-the-answer.md), README sections on connecting real models, masking and repair.

**Verified**

- In CI on `dev`: run [40196149410](https://github.com/xeeshan16/kafka-llm-enricher/actions/runs/40196149410) green for `b7e36c8` (format, Error Prone, unit and all Testcontainers tests). The first push (`15a605c`, run [40196005593](https://github.com/xeeshan16/kafka-llm-enricher/actions/runs/40196005593)) failed in the starter tests: a single `@DefaultValue` string is not split into list elements, so the default `mask-pii` bound as one unknown name. Fixed in `b7e36c8`.
- `DisputeTriagePipelineIT` (same run): no prompt contained the test card number or email, the placeholders were sent; D-1004 was fixed by one repair (2 calls, usage of both in the envelope); D-1003 was dead-lettered as `INVALID_JSON` after its repair; 6 calls for 4 disputes, 0 for the replay.
- The chaos, concurrency and cost tests still pass with masking, guard and repair on (the cost test turns the guard off, see below): chaos 40 committed envelopes and 1 call per input; cost outcomes identical to day 4 (acme spend 10,735 of 12,000 micro-USD).
- Locally (Maven Central is blocked in the build workspace): the core module's 127 unit tests, including all new ones, against distro jars with a stub schema validator; formatting with google-java-format 1.35.0 (`--skip-reflowing-long-strings`, matching Spotless). The starter and integration tests ran in CI only. No hosted model was called anywhere.

**Notes and plan adjustments**

- `CostControlIT` sets `prompt-guard=false` for its `costed` pipeline: its caps are sized for a short prompt, and the guard's ~160 extra input tokens per call (four-characters-per-token estimate) could change which calls fit under the cap. The guard is covered by the other tests.
- The default ledger lease went from 2 to 4 minutes so that a call plus one repair (2 x 115 s worst case with default settings) fits in one lease.
- Prompt injection is mitigated (escaping, guard, schema, no tools), not solved; the threat model says so and lists what operators must do.

**Next (day 6):** Micrometer metrics and OpenTelemetry traces (Kafka record to LLM span, GenAI semantic conventions) including the new repair and masking counters, a Grafana dashboard, the Docker image built and smoke-tested in CI, a `docker compose up` demo, a Helm chart and `jitpack.yml`.

## Day 6 (2026-10-26): observability and packaging

**Built**

- `enricher-core`: `llm.observation` with `ObservedLlmProvider`, `LlmCallContext` (a request-reply `CLIENT` context), `LlmObservationConvention` and `DefaultLlmObservationConvention` (OpenTelemetry GenAI names and tags: `gen_ai.client.operation`, span `chat <model>`, token usage as high-cardinality tags, bounded `error.type`). `ResilientLlmProvider` re-opens the caller's observation on its attempt threads. `UsageMeter.composite`. Deterministic simulated latency for `FakeLlmProvider`.
- `enricher-kafka`: `kafka.observation` with `ObservedRecordProcessor`: `enricher.record.process` per input (a `CONSUMER` context that reads `traceparent` from the record headers) and `enricher.record.publish` per output (a `PRODUCER` context that writes the trace context into the output headers, replacing copied ones). Outcome per record (`enriched`, `skipped`, `deferred`, `retry`, `dead_letter`, `dropped`); failures are dead-lettered inside the observation.
- `enricher-spring-boot-starter`: `EnricherMetricsAutoConfiguration` with `MicrometerUsageMeter` (GenAI token histogram, calls, tokens, cost and savings per tenant, skips, deferrals, downgrades, repairs, masked values) and `EnricherMeterBinder` (engine and rate-limiter counters and gauges read at scrape time); bounded tenant tags (`enricher.metrics.max-tenants`, `TenantTags`). `EnricherHealthAutoConfiguration` with `PipelinesHealthIndicator`. Observation wiring in `EnricherAutoConfiguration` and `PipelineRunner` when an `ObservationRegistry` exists. `enricher.providers.fake.latency` / `latency-jitter`.
- `enricher-app`: Actuator, Prometheus registry, Micrometer tracing with the OpenTelemetry bridge and the OTLP exporter, web server with graceful shutdown, liveness group including the `enricher` component. `demo` profile and `DisputeGenerator` / `SyntheticDisputes` (reproducible stream: test card numbers, `example.com` emails, replays, injection attempts), with a skip rule and per-merchant budgets.
- Packaging: multi-stage `Dockerfile` (layered jar, uid 10001), `docker-compose.yml` (Kafka, PostgreSQL, enricher, Prometheus, Grafana, Jaeger), Grafana dashboard generated by `scripts/build-dashboard.py`, `scripts/check-dashboard-metrics.py`, `scripts/smoke-test.sh`, Helm chart `deploy/helm/kafka-llm-enricher`, `jitpack.yml`.
- CI: `docker` job (build the image with the GHA cache, `docker compose up`, smoke test) and `helm` job (`helm lint --strict`, render with defaults and all options, sanity checks).
- Tests: `ObservedLlmProviderTest` (4), `FakeLlmProviderLatencyTest` (3), `CompositeUsageMeterTest` (2), `ObservedRecordProcessorTest` (6), `EnricherObservabilityAutoConfigurationTest` (4), `SyntheticDisputesTest` (3), and `ObservabilityIT` (Kafka + PostgreSQL, tracing and Prometheus on).
- Docs: [OBSERVABILITY.md](OBSERVABILITY.md), README sections (demo, Docker and Helm, observability), threat T10 (telemetry exposure).

**Verified**

- In CI on `dev`, run [40330115135](https://github.com/xeeshan16/kafka-llm-enricher/actions/runs/40330115135) for `571e918`: format, build (Error Prone, all unit and Testcontainers tests including `ObservabilityIT`) and helm passed on the first push. The docker job built the image, started the stack and got through health, 20 enriched envelopes with `traceparent` headers (ledger replays visible as `enricher-result-source:ledger`) and the scrape, then failed on a `grep | head` SIGPIPE under `pipefail` in the smoke script; fixed in `4757275`.
- Second push (`af71256`, run [40330806643](https://github.com/xeeshan16/kafka-llm-enricher/actions/runs/40330806643)): the dashboard check reported two label names (`gen_ai_token_type`, `gen_ai_request_model`) as missing metrics; fixed in `2577c48`.
- Run [40331475558](https://github.com/xeeshan16/kafka-llm-enricher/actions/runs/40331475558) for `2577c48`: all four jobs green. Compose smoke test: enricher healthy, 20 enriched envelopes with `traceparent` headers, all 29 metrics the dashboard queries present except three event-driven ones not yet triggered in the first minute (`deferred`, `downgraded`, `repairs`), Prometheus scraping the target, and a Jaeger trace `process payments.disputes` (364.8 ms) with children `chat fake-1` (360.6 ms, simulated latency) and `send payments.disputes.enriched`. About a minute into the stream the scrape showed 191 paid calls and 34 ledger hits across 8 merchants.
- `ObservabilityIT` (run 40330115135): one dispute published under an upstream `traceparent` produced `process payments.disputes` (CONSUMER, child of the upstream span, 120.6 ms), `chat fake-1` (CLIENT, child of process, 32.7 ms) and `send payments.disputes.enriched` (PRODUCER, child of process, 1.4 ms); the output's `traceparent` carried the send span; no span attribute contained the card number; the scrape held `gen_ai_client_operation_seconds`, `enricher_record_process_seconds`, the token histogram, `enricher_cost_usd_total` (5.8E-5 USD for that record at the test's prices) and the engine and limiter series.
- The chaos, concurrency and cost tests passed unchanged with observation on (chaos: 40 committed, 1 call per input, ledger 40 completed, 55 hits).
- Locally: formatting with google-java-format 1.35.0 only. Nothing was compiled or run locally (Maven Central is blocked in the build workspace), and the Helm chart was not rendered locally.

**Notes and plan adjustments**

- Spans and metrics come from Micrometer observations rather than direct OpenTelemetry API calls, so the framework-free modules carry one small API dependency, and Spring Boot users get whatever tracer and registry they already run.
- The demo moved its settings into a `demo` profile instead of environment variables, so `application.yml` stays a production baseline.
- No image is pushed to a registry; publishing to GHCR on tags is on the roadmap for day 7 or later.

**Next (day 7):** the eval harness on a labelled synthetic dataset (field-level extraction accuracy with the fake provider, duplicate paid calls under chaos, ledger hit rate and cost saved on replay, throughput and p95 latency), measured results in the README, ROADMAP, a clean-clone quick-start job in CI, and the `v0.1.0` tag.
