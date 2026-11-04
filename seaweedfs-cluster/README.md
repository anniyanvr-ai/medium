# SeaweedFS Enterprise – Primary + DR deployment toolkit

One inventory file drives validation, rendering, deployment, bootstrap and acceptance testing
of the two-DC design described in *SeaweedFS-Cluster-Setup v2.2*.

```
inventory/production-1m.yaml  RECOMMENDED v2.2 topology for 1M+ files/day (postgres2, EC 6+3, 2 hot + 2 cold
                              nodes per rack, dedicated workers, DM tuning, canary + guard) – validates 0 FAIL
inventory/production.yaml     v2.1 baseline topology (3+3 volume nodes, EC 10+4) – validator FAILs EC on purpose
inventory/lab.yaml            GENERATED single-machine lab (same topology on 127.x.y.z)
bin/swctl                     lifecycle tool (validate | render | install | deploy | bootstrap | verify | status | start | stop | shell | plan)
bin/s3-reconcile              bucket diff/repair between two S3 endpoints; --since/--until window, --date-layout
                              partitions, HEAD mode, parallel listing (seed proof, gap repair, failback)
bin/sw-canary                 end-to-end DR canary (one-shot or daemon with Prometheus metrics on :9340)
bin/sw-guard                  detect + --fix: DM stall (E-08/E-10), tiering leftovers (E-03), stale master
                              layout (E-07), unclassed volumes (E-02); reconciles the stall window
bin/sw-failback               plan | start | verify | cutover: snapshot-ON failback with reconcile gate (E-09)
bin/seaweed_admin_api.py      Enterprise Admin REST API client (plugin jobs, Data Movement targets/policies/status)
tests/e2e_enterprise.py       Enterprise suite: tiering policy, worker demotion before/after, EC failure domains,
                              Data Movement seed/lag/faults/backup, DC loss, MIGRATE failback (--screens = Admin UI evidence)
tests/run_lab_enterprise.sh   clean Enterprise lab -> deploy -> bootstrap -> Enterprise suite
tests/dm_selfheal.py          E-08 experiment: kill all workers under load; --tune / --guard variants
tests/tier_remediation.py     E-03/E-07 reproduction + sw-guard repair
tests/load_1m.py              1M files/day design point at 10x peak, PUT+GET latency + DR lag + window check
tests/pg_store_test.py        postgres2 shared filer store: visibility, filer SIGKILL, insert rate, bytes/entry
tests/e2e_test.py             38 acceptance tests (functional, security, HA, tiering/EC, DR, failover/failback)
tests/run_lab_e2e.sh          clean lab -> deploy both DCs -> full E2E run
tests/make_lab_inventory.py   production.yaml -> lab.yaml (IP remap 10.X.Y.Z -> 127.X.Y.Z)
docs/diagrams/                architecture diagrams (SVG + PNG) and their generator
reports/                      latest lab run: validation, deploy timings, e2e-results.json
build/production/             example render of production.yaml (secrets are placeholders, no private keys)
```

Requirements on the control host: Python 3.9+, `pyyaml`, `boto3` (tests), `openssl`, `ssh`.

## Production procedure

```bash
export SW_S3_ARCHIVE_APP_SECRET=... SW_S3_MONITOR_SECRET=... SW_S3_ADMIN_SECRET=...   # from vault
export SW_JWT_KEY_PRIMARY=... SW_JWT_KEY_DR=... SW_ADMIN_PASSWORD_PRIMARY=... SW_ADMIN_PASSWORD_DR=...
INV=inventory/production-1m.yaml
export SW_PG_PASSWORD=...                              # postgres2 filer store (one Patroni cluster per DC)

bin/swctl -i $INV validate --strict                     # 0 FAIL required (fix EC layout first!)
bin/swctl -i $INV render                                # build/production/<cluster>/<host>/root/...
bin/swctl -i $INV install --binary ./weed --cluster primary   # sha256-verified binary, user, sysctl, mount checks
bin/swctl -i $INV deploy  --cluster primary --report reports/deploy-primary.json
bin/swctl -i $INV bootstrap --cluster primary           # bucket(s) + tiering.apply (Enterprise)
bin/swctl -i $INV install --binary ./weed --cluster dr
bin/swctl -i $INV deploy  --cluster dr
bin/swctl -i $INV bootstrap --cluster dr      # bucket + tiering.apply + LANDING GATE (blocks until writes land on SSD/class)
bin/swctl -i $INV dm enable                     # enable data_movement + volume_tiering + apply data_movement_tuning
bin/swctl -i $INV dm apply                      # Data Movement target + policies from inventory (clusters.dr.data_movement)
bin/swctl -i $INV dm status                     # phase / lag / files / bytes; flags stale status
bin/swctl -i $INV verify
python3 tests/e2e_test.py -i $INV            # NON-destructive subset only against production
```

The example `build/production` ships certificates without private keys; delete `build/production/pki` (or drop in your enterprise CA) before rendering for real. Replace the self-signed PKI in `build/<inv>/pki/` with certificates from the enterprise CA before
rendering for production (one CA trusted by both DCs keeps Data Movement mTLS simple).

## Lab (full destructive acceptance run)

```bash
tests/run_lab_e2e.sh          # ~15 min on 2 vCPU / 8 GB; results in reports/e2e-results.json
```

The lab uses the same rendered flags/configs as production; only the transport (local processes
instead of ssh+systemd), sizes, and edition (OSS 4.47, so Enterprise tiering/Data Movement tests are
SKIPPED or use OSS stand-ins `ec.encode` / `filer.sync`) differ.

## Enterprise lab (Volume Tiering + Data Movement on the real Enterprise build)

```bash
# official asset, free for dev/test < 25 TB, no licence key:
curl -LO https://github.com/seaweedfs/artifactory/releases/latest/download/weed-enterprise-linux_amd64_large_disk.tar.gz
curl -LO https://github.com/seaweedfs/artifactory/releases/latest/download/weed-enterprise-linux_amd64_large_disk.tar.gz.md5
md5sum weed-enterprise-linux_amd64_large_disk.tar.gz; cat *.md5          # must match
tar xzf weed-enterprise-linux_amd64_large_disk.tar.gz
WEED=$PWD/weed tests/run_lab_enterprise.sh --screens   # results + screenshots in reports/enterprise/
```

Lab-only settings the generator adds: admin auth off on loopback (REST API without login) and
`-volume.allowUntrustedRemoteEndpoints` (volume servers refuse volume copies from loopback addresses).
Never use either in production. Key Enterprise findings (E-01..E-08) are in the v2.1 design document.

## Day-2 operations and remediation (v2.2)

```bash
# install the toolkit on each admin host as /opt/seaweedfs-deploy; credentials in /etc/seaweedfs/ops.env (0600)
systemctl enable --now sw-canary sw-guard.timer        # units rendered by `swctl render` (monitoring section)
bin/sw-guard -i $INV                                   # report only; exit 2 = something wrong
bin/sw-guard -i $INV --fix                             # heal: restart admin+workers, finish EC, fix layout, reconcile
bin/s3-reconcile --src ... --dst ... --bucket archive --since 2026-10-07T14:00Z --date-layout 'ingest/%Y/%m/%d/' --copy-missing
bin/sw-failback -i $INV plan                           # then start | verify | cutover --confirm
```

sw-guard restarts services through the same transport as swctl (ssh + systemctl), so the admin host
needs the deploy key for the cluster hosts. Never restart all workers at once by hand – that is what
orphans Data Movement jobs (E-08); the guard restarts the admin, which is not in the data path.
