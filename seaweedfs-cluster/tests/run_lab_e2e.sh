#!/usr/bin/env bash
# Full lab lifecycle: clean -> derive lab inventory -> validate -> render -> deploy primary ->
# bootstrap -> deploy DR -> bootstrap DR -> E2E suite (DR replication phase is started by G01).
set -euo pipefail
cd "$(dirname "$0")/.."
INV=inventory/lab.yaml
SW="bin/swctl -i $INV"
LAB_RUN=../lab/run

[[ -f $INV ]] && $SW stop >/dev/null 2>&1 || true
rm -rf "$LAB_RUN" build/lab reports && mkdir -p reports

python3 tests/make_lab_inventory.py inventory/production.yaml $INV
bin/swctl -i inventory/production.yaml validate --json reports/validate-production.json | tee reports/validate-production.txt
$SW render --generate-secrets

T0=$(date +%s)
$SW deploy --cluster primary --report reports/deploy-primary.json
$SW bootstrap --cluster primary
$SW deploy --cluster dr --report reports/deploy-dr.json
$SW bootstrap --cluster dr
echo "deploy+bootstrap of both clusters: $(( $(date +%s) - T0 ))s" | tee reports/deploy-time.txt
$SW verify | tee reports/verify.txt

python3 tests/e2e_test.py -i $INV --destructive --out reports "$@"
