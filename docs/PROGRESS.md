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
