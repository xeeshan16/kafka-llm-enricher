#!/usr/bin/env bash
# Smoke test of the docker compose demo: run after `docker compose up -d`.
#   1. the enricher reports UP, liveness included;
#   2. enriched envelopes arrive on payments.disputes.enriched (prints a few);
#   3. every metric the Grafana dashboard queries is exported;
#   4. Jaeger has traces with process, chat and send spans.
set -euo pipefail

BASE=${ENRICHER_URL:-http://localhost:8080}
JAEGER=${JAEGER_URL:-http://localhost:16686}
OUT=${SMOKE_OUT:-smoke}
mkdir -p "$OUT"

wait_for() { # description, command...
  local what=$1; shift
  for _ in $(seq 1 90); do
    if "$@" >/dev/null 2>&1; then echo "ok: $what"; return 0; fi
    sleep 2
  done
  echo "timed out: $what" >&2
  return 1
}

wait_for "enricher healthy" sh -c "curl -fsS $BASE/actuator/health/liveness | grep -q '\"UP\"'"
curl -fsS "$BASE/actuator/health" | tee "$OUT/health.json"
echo

echo "--- enriched envelopes"
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic payments.disputes.enriched \
  --from-beginning --max-messages 20 --timeout-ms 120000 \
  --isolation-level read_committed \
  --property print.headers=true --property print.key=true 2>/dev/null | tee "$OUT/enriched.txt"
count=$(grep -c '"enrichment"' "$OUT/enriched.txt" || true)
echo "envelopes read: $count"
[ "$count" -ge 1 ]
grep -q 'traceparent:00-' "$OUT/enriched.txt" || { echo "no traceparent header on outputs" >&2; exit 1; }

# Let a few scrape intervals and cost decisions accumulate.
sleep 20
echo "--- prometheus scrape"
curl -fsS "$BASE/actuator/prometheus" > "$OUT/prometheus.txt"
grep -E '^(enricher|gen_ai)_' "$OUT/prometheus.txt" | grep -v '_bucket{' | head -60
python3 "$(dirname "$0")/check-dashboard-metrics.py" "$OUT/prometheus.txt"

echo "--- prometheus server sees the target"
wait_for "prometheus scraping the enricher" sh -c \
  "curl -fsS 'http://localhost:9090/api/v1/query?query=up%7Bjob%3D%22kafka-llm-enricher%22%7D' | grep -q '\"1\"\\]'"

echo "--- jaeger traces"
wait_for "traces in jaeger" sh -c \
  "curl -fsS '$JAEGER/api/traces?service=kafka-llm-enricher&operation=process%20payments.disputes&limit=5' | grep -q 'chat fake'"
curl -fsS "$JAEGER/api/traces?service=kafka-llm-enricher&operation=process%20payments.disputes&limit=10" > "$OUT/traces.json"
python3 - "$OUT/traces.json" <<'PY'
import json, sys
data = json.load(open(sys.argv[1]))["data"]
trace = next(t for t in data if any(s["operationName"].startswith("chat ") for s in t["spans"]))
spans = {s["spanID"]: s for s in trace["spans"]}
for s in sorted(trace["spans"], key=lambda s: s["startTime"]):
    parent = next((r["spanID"] for r in s.get("references", []) if r["refType"] == "CHILD_OF"), None)
    pname = spans[parent]["operationName"] if parent in spans else "-"
    print(f"{s['operationName']:36} {s['duration'] / 1000:8.1f} ms  parent: {pname}")
names = {s["operationName"] for s in trace["spans"]}
assert any(n.startswith("chat ") for n in names), names
assert any(n.startswith("send ") for n in names), names
PY
echo "smoke test passed"
