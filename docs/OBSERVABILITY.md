# Observability

What the enricher tells you while it runs: metrics for Prometheus (through Micrometer), traces
for any OpenTelemetry backend, a health indicator for Kubernetes probes, and JSON logs that carry
the trace id. Everything here switches on when the application has Spring Boot Actuator and a
Micrometer registry on its classpath (the standalone app ships both); the framework-free modules
depend only on the Micrometer observation API and do nothing without a registry.

## Traces

One trace per record, continued from whoever produced it:

```
upstream producer span            (traceparent header on the input record)
└── process payments.disputes     CONSUMER  enricher.record.process
    ├── chat fake-1               CLIENT    gen_ai.client.operation   (one per attempt and repair)
    └── send payments.disputes.enriched  PRODUCER  enricher.record.publish
        └── downstream consumer span     (traceparent header written on the output record)
```

- The process span reads the W3C `traceparent` (and `tracestate`, baggage) from the input record's
  headers, so the enricher shows up inside the payment or dispute flow that produced the event,
  not as a disconnected root.
- Model-call spans are children of the process span even though attempts run on their own virtual
  threads: the retry layer re-opens the record's observation on each attempt thread. Every HTTP
  exchange is its own span, so a 429 followed by a retry shows as two `chat` spans, the first with
  `error.type=throttled`. Rate-limiter waits are not inside the span.
- Each output record (enriched, skipped, retry, deferred or dead-letter) gets a send span whose
  context is written into its headers, replacing any trace headers copied from the input. A record
  that goes through a retry topic therefore continues the same trace when it is retried.
- The send span ends when the record is handed to the engine, before the batch's Kafka transaction
  commits; the transaction itself is not a span.

Span names and attributes follow the OpenTelemetry semantic conventions for GenAI client calls and
messaging:

| Span | Attributes |
|---|---|
| `chat <model>` | `gen_ai.operation.name`, `gen_ai.provider.name`, `gen_ai.request.model`, `gen_ai.request.max_tokens`, `gen_ai.response.model`, `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`, `error.type` |
| `process <topic>` | `messaging.system`, `messaging.operation.name`, `messaging.destination.name`, `messaging.destination.partition.id`, `messaging.kafka.offset`, `pipeline`, `outcome`, `enricher.tenant`, `error.type` |
| `send <topic>` | `messaging.system`, `messaging.operation.name`, `messaging.destination.name`, `pipeline`, `outcome` |

`error.type` is a short, bounded class: `timeout`, `throttled`, `provider_retryable`,
`provider_error`, an exception's simple class name, or `none`.

**Prompts, answers, record keys and record values are never attached to spans.** Tracing backends
are usually readable by more people than the Kafka topics, and a masked prompt can still hold
personal data the detectors do not know about. The tenant is attached to process spans; if your
tenant is itself sensitive (a customer id rather than a merchant), see "Changing names and tags".

Export: set `management.otlp.tracing.endpoint` (the standalone app reads
`MANAGEMENT_OTLP_TRACING_ENDPOINT`, for example `http://otel-collector:4318/v1/traces`) and
`management.tracing.sampling.probability` (`ENRICHER_TRACE_SAMPLING`, default `0.1`). Records that
arrive with a sampled `traceparent` are always traced, whatever the local probability.

## Metrics

Prometheus names, as served at `/actuator/prometheus`. Every series also carries `application` and
`deployment_environment`.

### Records and the engine

| Metric | Type | Tags | Meaning |
|---|---|---|---|
| `enricher_record_process_seconds` | histogram | `pipeline`, `outcome`, `messaging_destination_name`, `error_type` | Processing time per input record; `outcome` is `enriched`, `skipped`, `deferred`, `retry`, `dead_letter` or `dropped`. |
| `enricher_record_publish_seconds_count` | counter | `pipeline`, `outcome`, `messaging_destination_name` | Output records by route. |
| `enricher_records_polled_total`, `_processed_total`, `_discarded_total`, `_committed_total` | counter | `pipeline` | Records returned by `poll()`, finished, dropped after a revoke or rewind, committed. |
| `enricher_outputs_written_total` | counter | `pipeline` | Output, retry, deferred and dead-letter records written with commits. |
| `enricher_commits_total`, `enricher_commit_failures_total` | counter | `pipeline` | Transactions (or offset commits) and failed ones; each failure rewinds. |
| `enricher_partitions_rebalanced_total` | counter | `pipeline`, `event` | Partitions `assigned`, `revoked`, `lost`. |
| `enricher_partitions_rewound_total`, `enricher_partitions_pauses_total` | counter | `pipeline` | Rewinds after failed commits; pauses for backpressure. |
| `enricher_records_in_flight`, `enricher_partitions_paused` | gauge | `pipeline` | Records running now; partitions paused now. |

### Model calls, tokens and the rate limiter

| Metric | Type | Tags | Meaning |
|---|---|---|---|
| `gen_ai_client_operation_seconds` | histogram | `gen_ai_operation_name`, `gen_ai_provider_name`, `gen_ai_request_model`, `error_type` | Duration of each provider exchange. |
| `gen_ai_client_token_usage` | histogram | `gen_ai_token_type` (`input`, `output`), `gen_ai_provider_name`, `gen_ai_request_model`, `pipeline` | Tokens per call, with the semantic conventions' bucket boundaries (1, 4, 16, ... 1,048,576). |
| `enricher_provider_calls_total` | counter | `pipeline`, `tenant`, `model` | Paid calls, repairs included. |
| `enricher_tokens_total` | counter | `pipeline`, `tenant`, `model`, `type` | Tokens per tenant. |
| `enricher_ratelimit_admitted_total`, `_rejected_total`, `_throttled_total`, `_decreases_total` | counter | `provider` | Admitted attempts; refused because quota frees up later than `max-wait`; 429/529 answers; cuts of the concurrency limit. |
| `enricher_ratelimit_wait_seconds_total` | counter | `provider` | Time admitted calls waited for request and token quota. |
| `enricher_ratelimit_concurrency_limit`, `enricher_ratelimit_in_flight` | gauge | `provider` | The adaptive (AIMD) limit and the calls running under it. |

### Cost, budgets, quality and privacy

| Metric | Type | Tags | Meaning |
|---|---|---|---|
| `enricher_cost_usd_total` | counter | `pipeline`, `tenant`, `model` | Spend, priced with `enricher.pricing`. |
| `enricher_ledger_hits_total`, `enricher_tokens_saved_total`, `enricher_cost_saved_usd_total` | counter | `pipeline`, `tenant`, `model` | Records answered from the result ledger and what the call would have cost. |
| `enricher_records_skipped_total` | counter | `pipeline`, `tenant`, `reason` | Not sent to the model: `rule:<name>` or `over-budget`. |
| `enricher_records_deferred_total`, `enricher_records_downgraded_total` | counter | `pipeline`, `tenant` (+ `from_model`, `to_model`) | Budget actions. |
| `enricher_repairs_total` | counter | `pipeline`, `model`, `result` (`fixed`, `failed`) | Repair prompts for unusable answers. |
| `enricher_pii_masked_total` | counter | `pipeline`, `type` (`PAN`, `IBAN`, `EMAIL`, `PHONE`, custom) | Distinct values replaced by placeholders in prompts. |

Counters that depend on an event (skips, deferrals, downgrades, repairs) appear after the first
one.

### Cardinality

Tags are bounded by design: pipelines, models, topics, outcomes and error classes are small, fixed
sets. Tenants are not: with `tenant: event.merchantId`, a payments platform can have hundreds of
thousands. The first `enricher.metrics.max-tenants` tenants (default 100) a process sees keep their
own tag value; later ones share `other`. Set `enricher.metrics.tenant-tag=false` to tag everything
`all` and keep per-tenant numbers in the PostgreSQL budget table instead.

## Dashboard

[`deploy/grafana/dashboards/kafka-llm-enricher.json`](../deploy/grafana/dashboards/kafka-llm-enricher.json)
(generated by [`scripts/build-dashboard.py`](../scripts/build-dashboard.py)): records per second by
outcome, record and model-call latency percentiles, in-flight records and paused partitions,
commits and rebalances, retries and dead letters, model calls by result, tokens per second, the
AIMD limit and throttling, repairs, spend and savings per hour by tenant, cost-control decisions
and masked personal data. It has `pipeline` and `tenant` variables and reads from a Prometheus
datasource. The compose demo provisions it; elsewhere, import the JSON. CI checks that every metric
it queries is exported by the running service (`scripts/check-dashboard-metrics.py`).

Alerts worth adding (not shipped): `enricher_commit_failures_total` increasing,
`enricher_partitions_rebalanced_total{event="lost"}` increasing, the share of `outcome="dead_letter"`,
`enricher_ratelimit_rejected_total` increasing (quota too small for the traffic), and spend per
tenant approaching its budget.

## Health

`/actuator/health` includes an `enricher` component: `DOWN` when a consumer loop has died (it does
not restart by itself, and its partitions stay unconsumed until the group rebalances them), with
the partitions and in-flight records of each loop. The standalone app puts it in the liveness group
(`management.endpoint.health.group.liveness.include=livenessState,enricher`), so Kubernetes restarts
a pod whose loop died; the Helm chart's probes use `/actuator/health/liveness` and `/readiness`.

## Logs

Set `logging.structured.format.console` (`LOGGING_STRUCTURED_FORMAT_CONSOLE`) to `ecs`, `logstash`
or `gelf` for one JSON object per line; the trace and span ids are in every line written while a
record is processed. The compose demo and the Helm chart use `ecs`. Log lines name the pipeline,
topic, partition and offset of a record, never its value.

## Changing names and tags

- Model calls: define a bean implementing `LlmObservationConvention` (extend
  `DefaultLlmObservationConvention` to keep the defaults and add tags).
- Spend metrics: define your own `UsageMeter` bean; every `UsageMeter` bean receives the events.
- Turn off an observation family with Spring Boot's `management.observations.enable.<prefix>`, for
  example `management.observations.enable.enricher.record=false`.
