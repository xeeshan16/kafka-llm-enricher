#!/usr/bin/env python3
"""Sanity checks on `helm template` output: expected kinds, per-pod transactional ids, probes."""
import sys

import yaml

for path in sys.argv[1:]:
    docs = [d for d in yaml.safe_load_all(open(path)) if d]
    kinds = sorted(d["kind"] for d in docs)
    dep = next(d for d in docs if d["kind"] == "Deployment")
    container = dep["spec"]["template"]["spec"]["containers"][0]
    env = {e["name"]: e for e in container["env"]}
    assert env["ENRICHER_TX_ID_PREFIX"]["value"] == "enricher-$(POD_NAME)-", env
    assert container["livenessProbe"]["httpGet"]["path"] == "/actuator/health/liveness"
    assert container["readinessProbe"]["httpGet"]["path"] == "/actuator/health/readiness"
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    print(path, kinds)
    print("  env:", sorted(env))
