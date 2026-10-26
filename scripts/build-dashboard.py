#!/usr/bin/env python3
"""Generates deploy/grafana/dashboards/kafka-llm-enricher.json.

The dashboard is written as code so panels stay consistent and reviewable; edit this file and
re-run it rather than editing the JSON by hand:

    python3 scripts/build-dashboard.py
"""
import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "${datasource}"}
P = 'pipeline=~"$pipeline"'
_next_id = [0]


def pid():
    _next_id[0] += 1
    return _next_id[0]


def target(expr, legend, ref="A"):
    return {"datasource": DS, "expr": expr, "legendFormat": legend, "refId": ref}


def timeseries(title, x, y, targets, unit="short", w=8, h=8, stack=False, desc=""):
    return {
        "id": pid(),
        "type": "timeseries",
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "custom": {
                    "lineWidth": 1,
                    "fillOpacity": 15 if stack else 5,
                    "stacking": {"mode": "normal" if stack else "none"},
                },
            },
            "overrides": [],
        },
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom"},
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
        "targets": [target(*t, ref=chr(65 + i)) for i, t in enumerate(targets)],
    }


def stat(title, x, y, expr, unit="short", w=4, h=4, desc="", decimals=None):
    panel = {
        "id": pid(),
        "type": "stat",
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "colorMode": "none",
            "graphMode": "area",
            "textMode": "value",
        },
        "targets": [target(expr, title)],
    }
    if decimals is not None:
        panel["fieldConfig"]["defaults"]["decimals"] = decimals
    return panel


def row(title, y):
    return {"id": pid(), "type": "row", "title": title, "collapsed": False,
            "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []}


R = "[$__rate_interval]"

panels = [
    row("Overview", 0),
    stat("Records / s", 0, 1,
         f'sum(rate(enricher_record_process_seconds_count{{{P}}}{R}))', "reqps"),
    stat("Paid model calls / s", 4, 1,
         f'sum(rate(enricher_provider_calls_total{{{P}}}{R}))', "reqps"),
    stat("Ledger hit ratio", 8, 1,
         f'sum(rate(enricher_ledger_hits_total{{{P}}}{R})) / '
         f'(sum(rate(enricher_ledger_hits_total{{{P}}}{R})) + '
         f'sum(rate(enricher_provider_calls_total{{{P}}}{R})))',
         "percentunit", desc="Records answered from the result ledger instead of a paid call."),
    stat("Spend (range)", 12, 1,
         f'sum(increase(enricher_cost_usd_total{{{P}}}[$__range]))', "currencyUSD",
         decimals=4, desc="Priced with enricher.pricing."),
    stat("Saved by ledger (range)", 16, 1,
         f'sum(increase(enricher_cost_saved_usd_total{{{P}}}[$__range]))', "currencyUSD",
         decimals=4),
    stat("Model call p95", 20, 1,
         f'histogram_quantile(0.95, sum by (le) '
         f'(rate(gen_ai_client_operation_seconds_bucket{R})))', "s"),

    row("Records", 5),
    timeseries("Records by outcome", 0, 6, [
        (f'sum by (outcome) (rate(enricher_record_process_seconds_count{{{P}}}{R}))',
         "{{outcome}}")], "reqps", stack=True,
        desc="Where each input record went: enriched, skipped, deferred, retry, dead_letter."),
    timeseries("Record processing latency", 8, 6, [
        (f'histogram_quantile(0.50, sum by (le) '
         f'(rate(enricher_record_process_seconds_bucket{{{P}}}{R})))', "p50"),
        (f'histogram_quantile(0.95, sum by (le) '
         f'(rate(enricher_record_process_seconds_bucket{{{P}}}{R})))', "p95"),
        (f'histogram_quantile(0.99, sum by (le) '
         f'(rate(enricher_record_process_seconds_bucket{{{P}}}{R})))', "p99")], "s",
        desc="From the start of processing to the outputs being handed to the engine; "
             "excludes time waiting in the partition queue."),
    timeseries("In flight and backpressure", 16, 6, [
        (f'sum(enricher_records_in_flight{{{P}}})', "records in flight"),
        (f'sum(enricher_partitions_paused{{{P}}})', "partitions paused")]),
    timeseries("Commits and rebalances", 0, 14, [
        (f'sum(rate(enricher_commits_total{{{P}}}{R}))', "commits"),
        (f'sum(rate(enricher_commit_failures_total{{{P}}}{R}))', "commit failures"),
        (f'sum by (event) (rate(enricher_partitions_rebalanced_total{{{P}}}{R}))',
         "partitions {{event}}")], "ops"),
    timeseries("Consumed vs committed", 8, 14, [
        (f'sum(rate(enricher_records_polled_total{{{P}}}{R}))', "polled"),
        (f'sum(rate(enricher_records_committed_total{{{P}}}{R}))', "committed"),
        (f'sum(rate(enricher_outputs_written_total{{{P}}}{R}))', "outputs written")], "reqps"),
    timeseries("Retries and dead letters", 16, 14, [
        (f'sum by (outcome) (rate(enricher_record_publish_seconds_count'
         f'{{{P},outcome=~"retry|dead_letter|deferred"}}{R}))', "{{outcome}}")], "reqps"),

    row("Model calls", 22),
    timeseries("Model calls by result", 0, 23, [
        ('sum by (gen_ai_request_model, error_type) '
         f'(rate(gen_ai_client_operation_seconds_count{R}))',
         "{{gen_ai_request_model}} {{error_type}}")], "reqps", stack=True,
        desc="One series point per HTTP exchange, including retried attempts and repairs."),
    timeseries("Model call latency", 8, 23, [
        ('histogram_quantile(0.50, sum by (le) '
         f'(rate(gen_ai_client_operation_seconds_bucket{R})))', "p50"),
        ('histogram_quantile(0.95, sum by (le) '
         f'(rate(gen_ai_client_operation_seconds_bucket{R})))', "p95"),
        ('histogram_quantile(0.99, sum by (le) '
         f'(rate(gen_ai_client_operation_seconds_bucket{R})))', "p99")], "s"),
    timeseries("Tokens / s", 16, 23, [
        (f'sum by (gen_ai_token_type) (rate(gen_ai_client_token_usage_sum{{{P}}}{R}))',
         "{{gen_ai_token_type}}")], "short", stack=True),
    timeseries("Adaptive concurrency", 0, 31, [
        ('sum(enricher_ratelimit_concurrency_limit)', "AIMD limit"),
        ('sum(enricher_ratelimit_in_flight)', "calls in flight")],
        desc="The limit halves on 429/529 answers and grows by one per success window."),
    timeseries("Throttling", 8, 31, [
        (f'sum(rate(enricher_ratelimit_throttled_total{R}))', "429/529 answers"),
        (f'sum(rate(enricher_ratelimit_rejected_total{R}))', "refused (quota wait > max-wait)"),
        (f'sum(rate(enricher_ratelimit_wait_seconds_total{R}))', "seconds waiting / s")]),
    timeseries("Answer repairs", 16, 31, [
        (f'sum by (result) (rate(enricher_repairs_total{{{P}}}{R}))', "{{result}}")], "ops",
        desc="Repair prompts for answers that were not JSON or broke the schema."),

    row("Cost and budgets", 39),
    timeseries("Spend per hour by tenant", 0, 40, [
        (f'topk(10, sum by (tenant) (rate(enricher_cost_usd_total{{{P},tenant=~"$tenant"}}{R}))'
         ' * 3600)', "{{tenant}}")], "currencyUSD", stack=True),
    timeseries("Saved per hour by the ledger", 8, 40, [
        (f'sum(rate(enricher_cost_saved_usd_total{{{P}}}{R})) * 3600', "saved"),
        (f'sum(rate(enricher_cost_usd_total{{{P}}}{R})) * 3600', "spent")], "currencyUSD"),
    timeseries("Cost controls", 16, 40, [
        (f'sum by (reason) (rate(enricher_records_skipped_total{{{P}}}{R}))', "skipped {{reason}}"),
        (f'sum(rate(enricher_records_downgraded_total{{{P}}}{R}))', "downgraded"),
        (f'sum(rate(enricher_records_deferred_total{{{P}}}{R}))', "deferred")], "reqps"),
    timeseries("Tokens per hour by tenant", 0, 48, [
        (f'topk(10, sum by (tenant) (rate(enricher_tokens_total{{{P},tenant=~"$tenant"}}{R}))'
         ' * 3600)', "{{tenant}}")], "short", w=12),
    timeseries("Personal data masked before the prompt", 12, 48, [
        (f'sum by (type) (rate(enricher_pii_masked_total{{{P}}}{R}))', "{{type}}")], "ops", w=12),
]

dashboard = {
    "uid": "kafka-llm-enricher",
    "title": "Kafka LLM Enricher",
    "description": "Records, model calls, tokens, cost and backpressure of kafka-llm-enricher.",
    "tags": ["kafka", "llm", "genai", "cost"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "editable": True,
    "refresh": "10s",
    "time": {"from": "now-15m", "to": "now"},
    "templating": {"list": [
        {"name": "datasource", "type": "datasource", "query": "prometheus",
         "current": {"selected": False, "text": "Prometheus", "value": "prometheus"}},
        {"name": "pipeline", "type": "query", "datasource": DS,
         "query": {"query": "label_values(enricher_records_polled_total, pipeline)",
                   "refId": "pipeline"},
         "definition": "label_values(enricher_records_polled_total, pipeline)",
         "includeAll": True, "multi": True, "allValue": ".*", "refresh": 2,
         "current": {"selected": True, "text": "All", "value": "$__all"}},
        {"name": "tenant", "type": "query", "datasource": DS,
         "query": {"query": "label_values(enricher_cost_usd_total, tenant)", "refId": "tenant"},
         "definition": "label_values(enricher_cost_usd_total, tenant)",
         "includeAll": True, "multi": True, "allValue": ".*", "refresh": 2,
         "current": {"selected": True, "text": "All", "value": "$__all"}},
    ]},
    "annotations": {"list": []},
    "panels": panels,
}

out = Path(__file__).resolve().parent.parent / "deploy/grafana/dashboards/kafka-llm-enricher.json"
out.write_text(json.dumps(dashboard, indent=2) + "\n")
print(f"wrote {out.relative_to(Path.cwd()) if out.is_relative_to(Path.cwd()) else out}")
