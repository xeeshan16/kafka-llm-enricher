# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
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
