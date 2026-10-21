# Plan: kafka-llm-enricher v0.1.0

Seven milestones, one per working day. Every step is pushed to `dev`; `main` is fast-forwarded
only after CI is green on `dev`.

## Day 1. Foundations and first slice
- Maven wrapper, Spotless (google-java-format), Error Prone, JaCoCo, CI workflow with Testcontainers.
- Module layout: `enricher-core`, `enricher-spring-boot-starter`, `enricher-app`.
- `LlmProvider` and `EmbeddingProvider` interfaces, plus deterministic fakes.
- One pipeline configured in YAML (prompt template, prompt version, JSON Schema): consume, call the
  fake model, validate, publish an enriched envelope. Integration test on a Kafka container.
- Repo hygiene: LICENSE, CONTRIBUTING, CHANGELOG, issue and PR templates, PLAN and PROGRESS.

## Day 2. Exactly-once and the result ledger
- Kafka transactions for read-process-write.
- PostgreSQL ledger (Flyway) keyed by pipeline, prompt version, model and canonical input hash,
  with in-flight leases.
- Chaos test: kill and rebalance mid-batch, then assert exactly one output per input and zero
  duplicate paid calls.
- ADR on the ledger versus Kafka-only idempotency.

## Day 3. Concurrency and backpressure
- Per-key ordered parallelism within a partition.
- Pause and resume partitions so the poll loop never blocks.
- Timeouts and retries with full-jitter backoff.
- Retry topics and a DLQ with reason headers. Poison-message tests.

## Day 4. Cost and rate control
- RPM and TPM token buckets per provider, with adaptive concurrency (AIMD on 429 and Retry-After).
- Per-tenant token and cost budgets in PostgreSQL, with a configurable over-budget action: defer,
  skip, or route to a cheaper model.
- SpEL skip predicates. Token and cost accounting.

## Day 5. Output quality and safety
- JSON Schema validation with a one-shot repair prompt.
- PII masking (PAN, IBAN, email, phone) before the prompt.
- Injection-resistant prompt framing and a written threat model.
- Real providers behind the interface: OpenAI-compatible (Ollama, vLLM), Anthropic and Azure OpenAI.

## Day 6. Observability and packaging
- Micrometer metrics and OpenTelemetry traces linking each Kafka record to its LLM span (GenAI
  semantic conventions). A Grafana dashboard.
- Multi-stage Docker image built and smoke-tested in CI.
- `docker compose up` demo with a synthetic payments-dispute stream, a Helm chart, and
  `jitpack.yml` for the starter.

## Day 7. Evals, polish and v0.1.0
- Eval harness on a labelled synthetic dataset: field-level extraction accuracy with the fake
  provider (and Ollama if reachable in CI); duplicate paid calls under chaos; ledger hit rate and
  cost saved on replay; throughput and p95 latency.
- README with measured results, ROADMAP, a clean-clone quick-start job in CI, and the `v0.1.0` tag.
