#!/usr/bin/env bash
# Enterprise lab: clean -> derive lab-ent inventory -> render -> deploy both DCs -> bootstrap (tiering.apply)
# -> Enterprise acceptance suite (Volume Tiering + Data Movement). Needs the Enterprise weed binary:
#   https://github.com/seaweedfs/artifactory/releases (weed-enterprise-linux_amd64_large_disk.tar.gz)
set -euo pipefail
cd "$(dirname "$0")/.."
WEED=${WEED:-/home/claude/lab/bin-ent/weed}
INV=inventory/lab-ent.yaml
SW="bin/swctl -i $INV"
[[ -f $INV ]] && $SW stop >/dev/null 2>&1 || true
rm -rf ../lab/run-lab-ent build/lab-ent reports/enterprise && mkdir -p reports/enterprise
python3 tests/make_lab_inventory.py inventory/production.yaml $INV --edition enterprise --name lab-ent --weed "$WEED"
$SW render --generate-secrets
T0=$(date +%s)
$SW deploy --cluster primary --report reports/enterprise/deploy-primary.json
$SW bootstrap --cluster primary | tee reports/enterprise/bootstrap-primary.txt
$SW deploy --cluster dr --report reports/enterprise/deploy-dr.json
$SW bootstrap --cluster dr | tee reports/enterprise/bootstrap-dr.txt
echo "enterprise deploy+bootstrap of both clusters: $(( $(date +%s) - T0 ))s" | tee reports/enterprise/deploy-time.txt
python3 tests/e2e_enterprise.py -i $INV --out reports/enterprise "$@"
