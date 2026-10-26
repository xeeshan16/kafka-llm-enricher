#!/usr/bin/env python3
"""Checks that every metric the Grafana dashboard queries is exported by a running enricher.

    curl -s localhost:8080/actuator/prometheus > scrape.txt
    python3 scripts/check-dashboard-metrics.py scrape.txt

Exits non-zero when a metric is missing, except for series that only appear once their event has
happened (listed in OPTIONAL), which are reported but do not fail the check.
"""
import json
import re
import sys
from pathlib import Path

DASHBOARD = Path(__file__).resolve().parent.parent / "deploy/grafana/dashboards/kafka-llm-enricher.json"
OPTIONAL = {
    # Counted on first occurrence of the event.
    "enricher_repairs_total",
    "enricher_records_deferred_total",
    "enricher_records_downgraded_total",
    "enricher_records_skipped_total",
}


def metric_names(expr):
    # Drop label matchers and grouping clauses: they hold label names, not metric names.
    expr = re.sub(r"\{[^}]*\}", "", expr)
    expr = re.sub(r"\b(?:by|without)\s*\([^)]*\)", "", expr)
    return re.findall(r"\b((?:enricher|gen_ai)_[a-z0-9_]+)\b", expr)


def queried_metrics(dashboard):
    names = set()
    for panel in dashboard["panels"]:
        for t in panel.get("targets", []):
            names.update(metric_names(t["expr"]))
    for var in dashboard["templating"]["list"]:
        q = var.get("definition", "")
        names.update(re.findall(r"label_values\(((?:enricher|gen_ai)_[a-z0-9_]+)", q))
    return names


def exported_metrics(scrape):
    names = set()
    for line in scrape.splitlines():
        if line and not line.startswith("#"):
            names.add(re.split(r"[{ ]", line, maxsplit=1)[0])
    return names


def main():
    scrape = Path(sys.argv[1]).read_text()
    wanted = queried_metrics(json.loads(DASHBOARD.read_text()))
    have = exported_metrics(scrape)
    missing = sorted(n for n in wanted if n not in have and n not in OPTIONAL)
    for name in sorted(wanted):
        state = "ok" if name in have else ("not yet (optional)" if name in OPTIONAL else "MISSING")
        print(f"{state:20} {name}")
    if missing:
        print(f"\n{len(missing)} metric(s) queried by the dashboard are not exported", file=sys.stderr)
        sys.exit(1)
    print(f"\nall {len(wanted)} dashboard metrics checked")


if __name__ == "__main__":
    main()
