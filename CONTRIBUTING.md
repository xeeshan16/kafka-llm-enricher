# Contributing

Thanks for helping. Issues and pull requests are welcome.

## Build

Requirements: JDK 21 and Docker (for the Testcontainers integration tests).

```bash
./mvnw verify                # compile with Error Prone, unit + integration tests, coverage, format check
./mvnw spotless:apply        # format the code (google-java-format)
./mvnw -pl enricher-core test   # fast loop on the core only, no Docker needed
```

Integration tests are named `*IT` and run in the `integration-test` phase with Failsafe.

## Where things live

| Module | What belongs there |
|---|---|
| `enricher-core` | Framework-free logic: providers, prompts, validation, hashing. No Spring or Kafka imports. |
| `enricher-spring-boot-starter` | Auto-configuration, properties, Kafka listener wiring. |
| `enricher-app` | The runnable service and its example configuration. |

## Extension points

- `LlmProvider` / `EmbeddingProvider`: add a model backend. Report usage when the API returns it,
  throw `LlmProviderException` with `retryable=true` for timeouts, 429 and 5xx.
- `EnrichedRecordSink`: publish enriched envelopes somewhere other than a Kafka topic.
- Any starter bean can be replaced by defining your own bean of the same type.

## Pull requests

- One logical change per PR, with tests. Keep `enricher-core` free of framework dependencies.
- Run `./mvnw verify` before pushing; CI runs the same.
- Update `CHANGELOG.md` under `Unreleased`.
- Never commit real keys, customer data or production payloads. Test data must be synthetic.
