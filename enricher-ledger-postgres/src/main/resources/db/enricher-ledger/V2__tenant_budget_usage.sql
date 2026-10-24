-- Budget counters: what each tenant has spent on a pipeline in one budget window.
-- Calls reserve their worst-case estimate with a conditional upsert (see PostgresBudgetStore)
-- and settle to the real usage afterwards, so concurrent instances can never jointly overspend.
CREATE TABLE enricher_budget_usage (
    pipeline      text        NOT NULL,
    tenant        text        NOT NULL,
    period_start  timestamptz NOT NULL,
    tokens        bigint      NOT NULL DEFAULT 0 CHECK (tokens >= 0),
    cost_micros   bigint      NOT NULL DEFAULT 0 CHECK (cost_micros >= 0),
    calls         bigint      NOT NULL DEFAULT 0 CHECK (calls >= 0),
    updated_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (pipeline, tenant, period_start)
);

-- Old windows can be dropped by age.
CREATE INDEX enricher_budget_usage_period_start_idx ON enricher_budget_usage (period_start);

COMMENT ON TABLE enricher_budget_usage IS
    'kafka-llm-enricher tenant budgets: tokens, cost (micro-USD) and calls per pipeline, tenant and budget window';
