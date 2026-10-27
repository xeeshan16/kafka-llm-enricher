# Roadmap

What comes after `v0.1.0`. Items marked **good first issue** are small, self-contained and covered
by existing tests to copy from. Open a pull request against `dev` (see
[CONTRIBUTING.md](../CONTRIBUTING.md)); larger items deserve a short design note in the PR first.

## Delivery and the ledger

- **Scheduled purge** of completed ledger rows and old budget windows (`enricher.ledger.retention`,
  a `@Scheduled` job calling `purgeCompletedBefore` / `purgeWindowsBefore`, with a metric for rows
  removed). **good first issue**
- **Store dead-lettered answers** (opt-in) so that replaying a dead-lettered record does not pay
  again; needs a failure state in the ledger table and a TTL.
- **Write-ahead "answer received" marker** to close the last duplicate-call window (a worker that
  dies after the model answered but before the answer was stored).
- **Redis ledger** for teams without PostgreSQL next to the service, behind the existing
  `ResultLedger` interface, with the same Testcontainers contract tests as `PostgresResultLedgerIT`.

## Cost and rate limits

- **Shared rate limits across replicas** (a token bucket in PostgreSQL or Redis) instead of a
  per-instance share of the key's quota.
- **Provider-reported token usage everywhere**: the fake estimates four characters per token; add a
  pluggable `TokenEstimator` (for example a tokenizer per model family). **good first issue** for the
  interface, larger for real tokenizers.
- **Budget reservations closer to reality**: reserve the prompt estimate plus the median output size
  seen so far instead of the full output cap.

## Quality and safety

- **More PII detectors**: national identifiers (with check digits) and postal addresses, as
  `PiiDetector` implementations with tests in `PiiMaskerTest`. **good first issue** (one detector per
  PR)
- **Eval dataset v2**: more categories, multilingual notes, adversarial injections that target the
  summary field; keep the generator seeded and the labels exact.
- **Eval gates in CI**: fail the build when a prompt or model change drops accuracy below a stored
  baseline for a chosen provider (the harness already writes `report.json`).
- **Semantic cache** (opt-in): answer near-duplicate events from the ledger by embedding similarity,
  with the hit threshold reported by the eval harness. `EmbeddingProvider` exists for this.

## Integration

- **Publish the image to GHCR** on tags, with an SBOM and a signature.
- **Avro and Protobuf input** via Schema Registry serdes, with the event rendered as JSON for the
  prompt.
- **More providers**: AWS Bedrock and Google Vertex AI behind `LlmProvider`, tested against a local
  stub server like `HttpProvidersTest`. **good first issue** for one provider.
- **Consumer lag and ledger table size** on the Grafana dashboard. **good first issue**
- **Kotlin DSL example** of a pipeline bean and a `@Bean LlmProvider` override in the README.
  **good first issue**

## Operations

- **`kubectl`-free replay tooling**: a small CLI or actuator endpoint that re-publishes a time range
  of an input topic for a pipeline, relying on the ledger to keep it free.
- **Per-pipeline dashboards** generated from the configured pipelines by
  `scripts/build-dashboard.py`.
