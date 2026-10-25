# ADR 0005: Mask before the prompt, validate and repair after the answer

- Status: accepted
- Date: 2026-10-25

## Context

Payments events carry personal data (card numbers, IBANs, emails, phone numbers) and free text
written by customers and merchants. Sending them to a hosted model as they are puts that data in a
third party's logs and brings the provider into PCI DSS scope; letting the event text speak to the
model as instructions invites prompt injection. On the way back, real models answer with prose,
truncated JSON or fields that break the schema often enough that a pipeline has to plan for it.

Three questions had to be answered:

1. Where does masking happen, and what does the output envelope contain?
2. What does the engine do with an unusable answer: dead-letter it, retry the whole record, or ask
   the model to fix it?
3. How do real providers plug in without tying the core to one SDK?

## Decision

**Masking is a prompt concern.** `Enricher.prepare` masks a copy of the parsed event (and the
record key) with the pipeline's `PiiMasker` and renders the prompt from the copy. The input hash,
skip rules, tenant expressions and the envelope's `input` use the original event. Placeholders are
numbered per record and stable (`[PAN_1]` for the same card everywhere in the record), so the
model can still relate fields, and the same record always produces the same prompt. Detectors
validate candidates (issuer range and Luhn, ISO 7064 mod 97) rather than masking every long number,
because over-masking hides amounts, dates and ids the model needs.

**Answers are checked for personal data too.** After schema validation, the same detectors scan the
answer; `output-pii` masks (and re-validates), rejects (`PII_IN_OUTPUT`) or allows.

**One bounded repair loop, inside the ledger lease.** An answer that is not JSON or breaks the
schema gets up to `repair-attempts` (0 to 2, default 1 in the starter) follow-up prompts: the
original task, the rejected answer and the validation errors, both quoted as escaped JSON strings.
Each repair is reserved against the tenant budget and metered like the first call; the envelope and
ledger entry carry the summed usage. A repair that cannot be made (no budget left, provider error)
keeps the original failure and does not throw, because throwing would send the record to the retry
topics and pay for the first call again.

**Providers on the JDK HTTP client.** `HttpLlmProvider` owns transport behaviour (429 and 529 to
`LlmThrottledException` with `Retry-After`, 408/409/5xx retryable, other 4xx not, timeouts, short
error messages, a key that never prints). `OpenAiCompatibleProvider` covers OpenAI, Ollama, vLLM
and gateways; `AzureOpenAiProvider` and `AnthropicProvider` adapt URLs, headers and bodies. The
response schema goes to the server as a `json_schema` response format where supported, otherwise
into the system prompt; the engine validates every answer either way.

## Consequences

- Prompts sent to providers contain no card numbers, IBANs, emails or phone numbers that the
  detectors recognise; `DisputeTriagePipelineIT` asserts it end to end.
- The output topic still carries the original event and needs the input topic's access controls.
  Dropping or masking `input` in the envelope is a roadmap option.
- Masking, guard and repair settings change the prompt, so they are part of the prompt's meaning:
  the pipeline's `prompt-version` should be bumped when they change, or ledger entries made under
  the old settings are reused. The demo pipeline moved to `v2` for this reason.
- A repaired record costs at least two calls. The ledger lease must cover them: the default
  `lease-ttl` grew from 2 to 4 minutes, and startup warns when it is shorter than the worst case of
  the first call plus every repair call.
- No provider SDK is on the classpath. Teams that prefer Spring AI or LangChain4j can still wrap
  them in an `LlmProvider` bean, which replaces the built-in providers.
- Anthropic's structured output is not used: the schema goes into the system prompt and the repair
  loop catches what still breaks it. This avoids depending on beta API features in v0.1.0.

## Alternatives considered

- **Mask the whole envelope.** Safer for downstream consumers, but most consumers (case tools,
  analysts) need the original event, and the input topic already holds it. Kept as an option for
  later rather than the default.
- **Reversible tokenisation** (restore the original values in the answer). It would put personal
  data back into model-written free text, which is exactly what `output-pii` guards against.
- **Retry the whole record on an invalid answer.** Simpler, but it pays for the first call again
  and goes through the retry topics with their delays, while the useful signal (what was wrong) is
  lost. A repair prompt costs one call and tells the model what to fix.
- **Provider SDKs or Spring AI in the core.** More features, but each SDK brings its own HTTP stack,
  retry policy and dependency tree, which would duplicate and fight the engine's own retries and
  rate limiting.
