# ADR 0001: Module layout and the provider interface

- Status: accepted
- Date: 2026-10-21

## Context

Teams adopt this in two ways: inside an existing Spring Boot service, or as a separate container
that only needs YAML. The enrichment logic (prompting, validation, hashing, later the ledger and
budgets) must be testable without a broker, and it must not tie users to one LLM vendor or one
Java AI framework.

## Decision

Three Maven modules:

- `enricher-core`: plain Java 21 plus Jackson and a JSON Schema validator. No Spring, no Kafka
  client. Holds `LlmProvider`, `EmbeddingProvider`, the fakes, `PromptTemplate`, `OutputSchema`,
  `CanonicalJson` and `Enricher`.
- `enricher-spring-boot-starter`: auto-configuration, `enricher.*` properties, one Spring Kafka
  listener container per pipeline, dead-letter routing. Every bean is `@ConditionalOnMissingBean`.
- `enricher-app`: a runnable Spring Boot application with no code beyond `main`.

`LlmProvider` is our own interface (`complete(LlmRequest) -> LlmResponse`) with
`LlmProviderException.retryable()` for retry classification. Spring AI or LangChain4j can sit behind
it as adapters, but the core does not depend on either.

The default provider is a deterministic fake that reads the response schema and fills it from the
event with keyword rules. Tests, CI and the demo need no keys and no network.

## Consequences

- Core tests run in milliseconds and cover most logic; only wiring needs a broker.
- Adding a provider is one class plus a property value, with no change to pipelines.
- We maintain a thin abstraction over vendor features (structured output, usage reporting). Features
  only one vendor has are passed through `LlmRequest` fields or ignored.
- The fake is not a model. Quality numbers measured with it describe the pipeline, not an LLM;
  the eval harness must say which provider produced every number.
