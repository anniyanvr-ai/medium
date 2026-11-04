#!/usr/bin/env bash
# Local 3-volume-server SeaweedFS cluster for integration tests.
#   master :9333 | volume :8081 :8082 :8083 | filer :8888 | S3 :8333
# Replication 001 = every file stored on 2 different volume servers.
set -euo pipefail
BASE="${SW_BASE:-/tmp/sw-cluster}"
mkdir -p "$BASE"/{m,v1,v2,v3,f,logs}
SW_KEY="${SW_KEY:-seaweedadmin}"; SW_SECRET="${SW_SECRET:-seaweedadmin}"
cat >"$BASE/s3.json" <<JSON
{"identities":[{"name":"admin","credentials":[{"accessKey":"$SW_KEY","secretKey":"$SW_SECRET"}],
  "actions":["Admin","Read","Write","List","Tagging"]}]}
JSON
weed master -ip=127.0.0.1 -port=9333 -mdir="$BASE/m" -defaultReplication=001 -volumeSizeLimitMB=64 \
  >"$BASE/logs/master.log" 2>&1 &
sleep 2
for i in 1 2 3; do
  weed volume -ip=127.0.0.1 -port=808$i -dir="$BASE/v$i" -max=300 -mserver=127.0.0.1:9333 \
    -dataCenter=dc1 -rack=rack1 >"$BASE/logs/volume$i.log" 2>&1 &
done
sleep 2
weed filer -ip=127.0.0.1 -port=8888 -master=127.0.0.1:9333 -s3 -s3.port=8333 -s3.config="$BASE/s3.json" \
  -defaultReplicaPlacement=001 -defaultStoreDir="$BASE/f" >"$BASE/logs/filer.log" 2>&1 &
for _ in $(seq 1 60); do
  curl -sf -o /dev/null http://127.0.0.1:8333/ 2>/dev/null && break
  curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8333/ 2>/dev/null | grep -qE '^[2-4]' && break
  sleep 1
done
echo "SeaweedFS up: S3 http://127.0.0.1:8333  filer http://127.0.0.1:8888  master http://127.0.0.1:9333"
