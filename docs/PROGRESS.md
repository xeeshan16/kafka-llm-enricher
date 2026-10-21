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
