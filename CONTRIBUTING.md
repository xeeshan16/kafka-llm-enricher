# Contributing

Thanks for helping. Issues and pull requests are welcome.

## Build

Requirements: JDK 21 and Docker (for the Testcontainers integration tests).

```bash
./mvnw verify                # compile with Error Prone, unit + integration tests, coverage, format check
./mvnw spotless:apply        # format the code (google-java-format)
./mvnw -pl enricher-core test   # fast loop on the core only, no Docker needed
```

Integration tests are named `*IT` and run in the `integration-test` phase with Failsafe. Their
console output goes to `*/target/failsafe-reports/*-output.txt`. Changes to the listener, the
ledger or transaction handling must keep `ExactlyOnceChaosIT` green.

`scripts/eval.sh` runs the eval harness (`DisputeEvalIT`) on its own and prints its report; see
[docs/EVALUATION.md](docs/EVALUATION.md). Changes to prompts, masking or the schema should come with
its report before and after. To grow the dataset, change `scripts/generate-eval-dataset.py` and
regenerate the file rather than editing it by hand.

## Where things live

| Module | What belongs there |
|---|---|
| `enricher-core` | Framework-free logic: providers, prompts, validation, hashing, the ledger contract. No Spring or Kafka imports. |
| `enricher-ledger-postgres` | The PostgreSQL ledger and its migrations. Plain JDBC plus Flyway, no Spring. |
| `enricher-kafka` | The consumer loop, key lanes, retry and deferred topics, routing. Plain `kafka-clients`, no Spring. |
| `enricher-spring-boot-starter` | Auto-configuration, properties, metrics and health wiring. |
| `enricher-app` | The runnable service, its example configuration, the demo stream and the eval harness. |

## Extension points

- `LlmProvider` / `EmbeddingProvider`: add a model backend. Report usage when the API returns it,
  throw `LlmProviderException` with `retryable=true` for timeouts, 429 and 5xx.
- `ResultLedger`: keep leases and stored results somewhere else (Redis, DynamoDB, another SQL
  database). `claim` must grant at most one live lease per key, `complete` must keep the first
  stored result, and `release` must only drop the caller's own lease.
- `PiiDetector`: recognise another kind of personal data (a national id, an account reference);
  add it to `PiiMasker` and a case to `PiiMaskerTest`.
- `UsageMeter`: receive every spend, ledger-hit, skip, deferral, repair and masking event (the
  Micrometer exporter is one implementation; every `UsageMeter` bean is called).
- `LlmObservationConvention`: change span names and tags of model calls.
- `OutputRecordFactory` and `KafkaClientFactory`: shape output records and headers, or wrap the
  Kafka clients (the chaos test injects failures this way).
- Any starter bean can be replaced by defining your own bean of the same type.

## Pull requests

- One logical change per PR, with tests. Keep `enricher-core` free of framework dependencies.
- Run `./mvnw verify` before pushing; CI runs the same.
- Update `CHANGELOG.md` under `Unreleased`.
- Never commit real keys, customer data or production payloads. Test data must be synthetic.
