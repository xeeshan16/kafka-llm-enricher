-- Result ledger: one row per paid enrichment, keyed by what determines the answer.
-- IN_FLIGHT rows are leases: a worker is calling the provider for this key right now.
-- COMPLETED rows hold the validated, canonical answer and are reused instead of paying again.
CREATE TABLE enricher_result_ledger (
    pipeline          text        NOT NULL,
    prompt_version    text        NOT NULL,
    model             text        NOT NULL,
    input_hash        text        NOT NULL,
    status            text        NOT NULL CHECK (status IN ('IN_FLIGHT', 'COMPLETED')),
    lease_token       text,
    lease_owner       text,
    lease_expires_at  timestamptz,
    attempts          integer     NOT NULL DEFAULT 1,
    result_json       text,
    response_model    text,
    input_tokens      bigint,
    output_tokens     bigint,
    hits              bigint      NOT NULL DEFAULT 0,
    created_at        timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at      timestamptz,
    last_hit_at       timestamptz,
    PRIMARY KEY (pipeline, prompt_version, model, input_hash),
    CONSTRAINT enricher_result_ledger_completed_has_result
        CHECK (status <> 'COMPLETED' OR (result_json IS NOT NULL AND completed_at IS NOT NULL)),
    CONSTRAINT enricher_result_ledger_in_flight_has_lease
        CHECK (status <> 'IN_FLIGHT' OR (lease_token IS NOT NULL AND lease_expires_at IS NOT NULL))
);

-- Retention purges scan completed rows by age.
CREATE INDEX enricher_result_ledger_completed_at_idx
    ON enricher_result_ledger (completed_at)
    WHERE status = 'COMPLETED';

COMMENT ON TABLE enricher_result_ledger IS
    'kafka-llm-enricher result ledger: leases and stored LLM enrichments, keyed by pipeline, prompt version, model and canonical input hash';
