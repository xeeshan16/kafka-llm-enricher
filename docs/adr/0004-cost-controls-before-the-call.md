# ADR 0004: Cost and rate controls run before the call, in a fixed order

- Status: accepted
- Date: 2026-10-24

## Context

An enrichment pipeline spends money on every provider call, and in multi-tenant payment systems one
merchant's burst (a fraud wave, a replayed backfill, a misbehaving integration) can use the whole
provider quota and the month's budget. Teams need four things, and they interact:

- stay inside the provider's requests-per-minute and tokens-per-minute quotas, and back off when
  the provider answers 429 anyway (shared keys, other services, provider-side load);
- cap spend per tenant and window, across every instance of the service;
- decide what happens to a record that does not fit: wait, use a cheaper model, or skip it;
- never send routine events to the model at all.

The result ledger (ADR 0002) adds a constraint: a stored answer is already paid for, so it must
stay available whatever the budget says. Otherwise a replay of yesterday's topic would be deferred
for a tenant whose budget is used up, although it costs nothing.

## Decision

`LedgeredEnricher` applies a per-pipeline `CostPolicy` in this order:

1. **Skip rules** (SpEL in the starter). A match produces a skipped envelope and no call.
2. **Result ledger claim.** A hit is answered from the ledger, budget or not.
3. **Budget reservation**, only for a claim that will really call the provider. The reservation is
   the worst case (prompt estimate plus the output cap) priced with the pipeline's model, charged
   atomically against the tenant's caps, and settled to the reported usage after the call.
   Provider errors refund it; an unusable answer stays charged, since it was paid for.
4. **Over budget:** `downgrade` retries steps 2 and 3 with the fallback model (it has its own
   ledger key, so answers of the two models never mix); `skip` writes a skipped envelope; `defer`
   parks the record on `<input>.enricher-deferred` with a not-before at the window reset. The
   pipeline consumes that topic and the consumer loop holds the record without blocking others.
5. **Rate limiting** happens inside the provider call, per attempt: `ProviderRateLimiter` admits
   the attempt against RPM and TPM token buckets and an AIMD concurrency limit, and feeds 429s back
   into that limit. An attempt that would wait longer than `max-wait` fails as retryable, so the
   existing retry policy and retry topics apply.

Budget counters live in PostgreSQL (`enricher_budget_usage`, migration V2 of the ledger schema).
A charge is one `INSERT .. ON CONFLICT DO UPDATE .. WHERE` statement: the upsert's row lock
serialises concurrent charges of one tenant and the `WHERE` clause sees the latest counters, so a
charge that would cross the cap changes nothing.

Rate limits stay in process memory.

## Alternatives considered

- **Budget check before the ledger claim.** Simpler, but it would reserve (and then refund) budget
  for every ledger hit, and would defer free replays of a tenant that is out of budget.
- **Charging after the call only.** Exact, but N workers that all see "under the cap" can
  together overshoot it by N calls. Reserving the worst case keeps the cap hard at the cost of
  stopping a tenant slightly early.
- **A distributed rate limiter (Redis, PostgreSQL).** Gives one quota across replicas, but adds a
  network round trip and a new dependency in front of every call. Provider quotas are coarse and
  429s are fed back anyway, so per-instance limits (1/N of the quota each) plus AIMD are enough for
  v0.1. A shared limiter is on the roadmap.
- **Deferring through the retry topics.** Retry delays are seconds to minutes; a budget resets
  after hours or weeks. A dedicated deferred topic keeps the retry stages and their attempt counts
  meaningful.
- **SpEL with a full `StandardEvaluationContext`.** More expressive, but it allows type
  references and constructors. A `SimpleEvaluationContext` with a read-only map accessor covers
  field access, comparisons and string methods, which is what skip rules need.

## Consequences

- One cost decision point, testable without Kafka (`LedgeredEnricherCostTest`).
- A tenant's cap holds across instances when budgets use PostgreSQL; with the in-memory store each
  instance counts on its own.
- Worst-case reservations stop a tenant before its last few calls could fit. Smaller
  `max-output-tokens` makes reservations tighter.
- A deferred record waits in the consumer's memory until its window resets, and holds back the
  commit point of its deferred-topic partition. Records on that topic are all waiting for a reset,
  so this delays nothing else; a restart reads them again.
- Rate limits are per instance. With N replicas on one key, each should get about 1/N of the
  key's quota.
- Costs are only as good as `enricher.pricing`; a model without a price counts as free, and the
  starter warns about that when a pipeline has a cost cap.
