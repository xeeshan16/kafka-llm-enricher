#!/usr/bin/env bash
# Runs the eval harness: the labelled dispute dataset through the dispute-triage pipeline, against
# Kafka and PostgreSQL in Testcontainers (Docker required, no API keys with the default fake).
#
#   scripts/eval.sh                                   # deterministic fake provider
#   ENRICHER_PROVIDER=openai ENRICHER_OPENAI_BASE_URL=http://localhost:11434/v1 \
#     ENRICHER_DISPUTE_MODEL=qwen2.5:0.5b EVAL_LIMIT=80 scripts/eval.sh    # local Ollama model
#
# The provider is chosen with the same ENRICHER_* variables as the service. EVAL_LIMIT runs the
# first N cases only. The report is written to enricher-app/target/eval/report.{md,json}.
set -euo pipefail
cd "$(dirname "$0")/.."

./mvnw -B -ntp -pl enricher-app -am verify \
  -Dtest=EvalMetricsTest -Dsurefire.failIfNoSpecifiedTests=false \
  -Dit.test=DisputeEvalIT -Dit.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false \
  -Dspotless.check.skip=true -Djacoco.skip=true "$@"

echo
cat enricher-app/target/eval/report.md
