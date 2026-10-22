# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
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
- `EnrichedRecordSink.publish` takes the `ResultSource`; `PipelineRunner` takes a
  `LedgeredEnricher`, a busy back-off and an optional transaction manager.
- The enrichment in the envelope is written in canonical (sorted) key order.
- The standalone app now needs PostgreSQL (`ENRICHER_DB_URL`, `ENRICHER_DB_USER`,
  `ENRICHER_DB_PASSWORD`) and reads only committed records.
