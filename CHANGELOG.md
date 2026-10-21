# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- `enricher-core`: `LlmProvider` / `EmbeddingProvider` interfaces, deterministic fakes
  (`FakeLlmProvider` with a schema-aware responder, `FakeEmbeddingProvider`), `PromptTemplate` with
  untrusted-event framing, JSON Schema output validation, canonical JSON hashing, and `Enricher`,
  which builds a deterministic output envelope.
- `enricher-spring-boot-starter`: `enricher.*` configuration, one listener container per pipeline,
  dead-letter routing with `enricher-failure-reason` headers, optional topic creation.
- `enricher-app`: standalone service with a `dispute-triage` example pipeline.
- Build and CI: Maven wrapper, Error Prone, Spotless (google-java-format), JaCoCo, Testcontainers
  Kafka integration test.
