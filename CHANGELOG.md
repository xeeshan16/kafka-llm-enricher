# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Observability. `ObservedLlmProvider` records each provider exchange as a
  `gen_ai.client.operation` observation (OpenTelemetry GenAI names and tags, `CLIENT` span, custom
  `LlmObservationConvention` supported); `ObservedRecordProcessor` records each input record as
  `enricher.record.process` (continuing the producer's trace from the record headers) and each
  output as `enricher.record.publish` (writing the trace context into its headers). The retry
  layer carries the record's observation onto its attempt threads.
- Micrometer metrics in the starter when a `MeterRegistry` is present: `MicrometerUsageMeter`
  (token histogram, calls, tokens, cost and ledger savings per tenant with bounded tenant tags,
  skips, deferrals, downgrades, repairs, masked values) and `EnricherMeterBinder` (engine and
  rate-limiter counters and gauges). `enricher.metrics.tenant-tag` and `max-tenants`.
- `enricher` health component (`PipelinesHealthIndicator`), `DOWN` when a consumer loop has died.
- Standalone app: Actuator, Prometheus endpoint, OpenTelemetry tracing over OTLP, structured
  logging, graceful shutdown, and the `demo` profile with `DisputeGenerator`, a reproducible
  synthetic dispute stream.
- `enricher.providers.fake.latency` / `latency-jitter`: deterministic simulated latency.
- Multi-stage `Dockerfile` (layered jar, non-root), `docker-compose.yml` demo with Prometheus,
  Grafana (provisioned dashboard) and Jaeger, a Helm chart, `jitpack.yml`, and CI jobs that build
  the image, smoke-test the compose stack and lint the chart.
- [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md) and threat T10 (telemetry exposure).

### Changed
- Every `UsageMeter` bean now receives spend events (`UsageMeter.composite`); previously only a
  single meter was used.
- PII masking before the prompt (`mask-pii`, `mask-fields`): `PiiMasker` with checked detectors for
  card numbers (issuer range and Luhn), IBANs (mod 97), emails and phone numbers, whole-field
  redaction by path, stable per-record placeholders (`[PAN_1]`), and `PiiDetector` as the extension
  point for custom identifiers. The record key is masked too.
- Injection guard (`prompt-guard`, on by default) prepended to the system prompt, and `output-pii`
  (`mask`, `reject`, `allow`) for personal data found in answers. New failure reason
  `PII_IN_OUTPUT`.
- Schema repair prompts (`repair-attempts`, 0 to 2, default 1 in the starter) for `INVALID_JSON`
  and `SCHEMA_VIOLATION`, budgeted and metered like any call; envelope and ledger usage include
  every call. `UsageMeter.repairAttempt` / `piiMasked` and `UsageAccounting.quality`.
- Real providers in `enricher-core` on the JDK HTTP client: `OpenAiCompatibleProvider` (OpenAI,
  Ollama, vLLM, gateways; `json_schema`, JSON mode or prompt-only schema), `AnthropicProvider`
  (Messages API, 529 treated as throttling) and `AzureOpenAiProvider` (deployment URL,
  `api-version`, `api-key`), all mapping 429 to `LlmThrottledException` with `Retry-After`.
  Configured with `enricher.provider` and `enricher.providers.*`.
- [Threat model](docs/THREAT-MODEL.md) and ADR 0005.
- Provider rate limiting (`enricher.rate-limit.*`): requests-per-minute and tokens-per-minute token
  buckets plus an AIMD concurrency limit that halves on 429s and honours `Retry-After`.
  `ProviderRateLimiter`, `RateLimitedLlmProvider` and `LlmThrottledException` for provider adapters.
- Per-tenant budgets (`enricher.pipelines.<name>.budget.*`): daily or monthly token and USD caps,
  per-tenant overrides, and `on-exceeded` = `defer` (to `<input>.enricher-deferred` until the
  window resets), `skip` or `downgrade` (to `fallback-model`, then `after-downgrade`). Worst-case
  reservations settled to real usage; ledger hits are never charged. See ADR 0004.
- `PostgresBudgetStore` and migration `V2__tenant_budget_usage.sql`: one cap across instances via a
  conditional upsert. `enricher.budgets.store` selects PostgreSQL or memory.
- Skip rules (`skip.<rule>`) and a `tenant` expression per pipeline, in SpEL with a restricted
  `SimpleEvaluationContext`; skipped records produce a skipped envelope unless `emit-skipped` is
  off.
- Cost accounting: `enricher.pricing.<model>.*`, `PriceTable` (micro-USD), and `UsageAccounting`
  with calls, tokens, cost, ledger hits and cost saved per pipeline, tenant and model.
- Headers `enricher-tenant` and `enricher-skip-reason`; failure reason `BUDGET_EXCEEDED`;
  `ResultSource.NONE`.
- `CostControlIT` and `PostgresBudgetStoreIT`; their measured lines appear in the CI summary.
- `enricher-kafka` module: `ConsumerLoop`, a poll loop that never blocks on model calls. Records run
  in parallel by lane (per key within a partition by default, `partition` or `unordered`), finished
  prefixes commit together in one Kafka transaction per `commit-interval`, and full partitions are
  paused and resumed. See ADR 0003.
- Retry topics `<input>.enricher-retry-N` with per-stage delays (`retry-delays`) and an
  `enricher-retry-not-before` header; dead-lettered records carry `enricher-attempts` and the
  original topic, partition and offset. New reason `RETRIES_EXHAUSTED`.
- `ResilientLlmProvider`: per-call timeout and full-jitter retries honouring `Retry-After`,
  configured under `enricher.resilience.*`.
- `ConcurrencyAndRetryIT`: per-key order under a slow call, retry topic delays, dead-letter reasons
  and poison records against real Kafka and PostgreSQL.
- Exactly-once delivery: with `spring.kafka.producer.transaction-id-prefix` set, each record is
  processed read-process-write in one Kafka transaction, and dead-lettering commits its offset in a
  transaction too. `enricher.delivery` (`auto`, `exactly-once`, `at-least-once`) fails startup when
  the Kafka setup contradicts it.
- Result ledger: `ResultLedger` contract with leases, `LedgeredEnricher`, in-memory and no-op
  ledgers in the core, and the new `enricher-ledger-postgres` module (`PostgresResultLedger`, own
  Flyway history table). Configured under `enricher.ledger.*`. Keys leased by another worker are
  retried on a back-off spanning one lease and dead-lettered as `LEDGER_BUSY` only after that.
- `enricher-result-source` header (`provider` or `ledger`) on enriched records.
- `ExactlyOnceChaosIT`: two instances, aborted transactions and a zombie worker expelled from the
  group; asserts one committed envelope and one paid call per input.
- ADR 0002 on the ledger versus Kafka-only idempotency.
- `enricher-core`: `LlmProvider` / `EmbeddingProvider` interfaces, deterministic fakes
  (`FakeLlmProvider` with a schema-aware responder, `FakeEmbeddingProvider`), `PromptTemplate` with
  untrusted-event framing, JSON Schema output validation, canonical JSON hashing, and `Enricher`,
  which builds a deterministic output envelope.
- `enricher-spring-boot-starter`: `enricher.*` configuration, one listener container per pipeline,
  dead-letter routing with `enricher-failure-reason` headers, optional topic creation.
- `enricher-app`: standalone service with a `dispute-triage` example pipeline.
- Build and CI: Maven wrapper, Error Prone, Spotless (google-java-format 1.35.0), JaCoCo,
  Testcontainers Kafka integration test.

### Changed
- The default `enricher.ledger.lease-ttl` is 4 minutes (was 2), and the lease warning covers the
  first call plus every repair call.
- The demo `dispute-triage` pipeline uses prompt version `v2`: its prompt is now masked and guarded.
- `enricher.provider` accepts `openai`, `anthropic` and `azure-openai`; hosted providers fail
  startup without credentials.
- `EnrichmentOutcome` gains `Skipped` and `Deferred`; `LedgeredEnricher.Result` carries the tenant.
- The enricher calls the provider through `ResilientLlmProvider(RateLimitedLlmProvider(provider))`.
- The ledger lease warning includes rate-limiter waits.
- The starter runs pipelines on `ConsumerLoop` instead of Spring Kafka listener containers.
  `EnrichedRecordSink`, `EnrichingListener` and the after-rollback wiring are removed; Kafka client
  settings still come from `spring.kafka.*`.
- `EnrichedRecordSink.publish` takes the `ResultSource`; `PipelineRunner` takes a
  `LedgeredEnricher`, a busy back-off and an optional transaction manager.
- The enrichment in the envelope is written in canonical (sorted) key order.
- The standalone app now needs PostgreSQL (`ENRICHER_DB_URL`, `ENRICHER_DB_USER`,
  `ENRICHER_DB_PASSWORD`) and reads only committed records.
