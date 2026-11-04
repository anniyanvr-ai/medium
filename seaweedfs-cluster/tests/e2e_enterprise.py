#!/usr/bin/env python3
"""
Enterprise acceptance suite - Volume Tiering and Data Movement on a real SeaweedFS Enterprise build.

Complements tests/e2e_test.py (which covers deployment, S3, security and HA). Run against the
Enterprise lab inventory after deploy + bootstrap:

    python3 tests/e2e_enterprise.py -i inventory/lab-ent.yaml --out reports/enterprise

Destructive by design (kills processes, whole DC). Never point it at production.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import e2e_test as base  # noqa: E402
from ent_admin import AdminAPI  # noqa: E402

sw, rec = base.sw, base.rec
BUCKET = base.BUCKET
P, D = "primary", "dr"
TB = os.environ.get("TIER_BUCKET", "tiertest")   # separate bucket: a 60 s rung must never govern a live collection (E-07)
QUIET = 60              # lab quiet period for the test tiering profile (production: 7,776,000 s)


def wait(fn, timeout, every=3):
    t0 = time.time()
    last = None
    while time.time() - t0 < timeout:
        ok, last = fn()
        if ok:
            return True, round(time.time() - t0, 1), last
        time.sleep(every)
    return False, round(time.time() - t0, 1), last


class Ent:
    def __init__(self, S):
        self.S, self.inv = S, S.inv
        a = lambda c: f"http://{self.inv.admin_node(c)['ip']}:{self.inv.port('admin', cluster=c)}"  # noqa: E731
        self.adm = {P: AdminAPI(a(P)), D: AdminAPI(a(D))}
        self.pki = self.inv.build_dir / "pki"
        self.ui = None
        self.urls = {P: a(P), D: a(D)}

    def shot(self, cluster, path, step, caption):
        if self.ui:
            try:
                self.ui.capture(self.urls[cluster], path, step, caption, cluster=cluster)
            except Exception as e:  # noqa: BLE001
                print("  (screenshot failed)", e)

    def target(self, name, cluster, path=""):
        f = self.inv.filers(cluster)[0]
        host, port = f.split(":")
        k = self.pki / cluster
        return {"name": name, "kind": "cluster", "cluster": {
            "filerGrpcAddress": f"{host}:{int(port) + 10000}", "path": path, "replication": "", "collection": "",
            "ttlSec": 0, "diskType": "", "filerProxy": False, "securitySection": "",
            "caFile": str(k / "ca.crt"), "certFile": str(k / "client.crt"), "keyFile": str(k / "client.key"),
            "serverName": ""}}

    @staticmethod
    def policy(name, target, src, dst, mode="continuous", deletes=True, snapshot=True, start_ns="0"):
        return {"name": name, "enabled": True, "sourcePath": src, "excludePaths": [], "targetName": target,
                "targetBucket": "", "targetDirectory": dst, "mode": mode, "propagateDeletes": deletes,
                "initialSnapshot": snapshot, "startFromTsNs": start_ns, "incremental": False,
                "retentionDays": 0, "bytesPerSecond": 0, "fileConcurrency": 0, "chunkConcurrency": 0,
                "bidirectional": False, "mirrorBucketOps": False}

    def status(self, cluster, name):
        for s in self.adm[cluster].dm_status().get("statuses", []) or []:
            if s.get("policy") == name:
                return s
        return {}

    def tiering(self, cluster, doc):
        f = self.inv.build_dir / f"tiering-{cluster}-{int(time.time())}.json"
        f.write_text(json.dumps(doc))
        return self.S.sh(cluster, f"tiering.apply -file={f}")


def tier_snapshot(S, cluster, collection):
    """Hot vs cold footprint of one collection: replicated volumes per disk type + EC shards per disk type."""
    import re
    snap = {"ssd_replicated_volumes": 0, "ssd_replicated_mb": 0.0, "hdd_replicated_volumes": 0,
            "hdd_replicated_mb": 0.0, "ec_volumes": 0, "ec_shards_on_hdd": 0, "ec_shards_on_ssd": 0,
            "ec_mb_on_hdd": 0.0, "ec_mb_on_ssd": 0.0, "files": 0}
    seen = set()
    for node, rack, dt, vid, col, size in base.volumes(S.inv, cluster):
        if col != collection or size <= 8:
            continue
        snap[f"{dt}_replicated_volumes"] += 1           # counts replicas (2 per volume with 010)
        snap[f"{dt}_replicated_mb"] += size / 1048576
    disk, ecv = None, set()
    for line in S.sh(cluster, "volume.list").splitlines():
        m = re.search(r"^\s+Disk (\w+)\(", line)
        if m:
            disk = m.group(1)
        m = re.search(r"ec volume id:(\d+) collection:(\S+) shards:\[([\d ]*)\].*?total:([\d.]+) (\w+)", line)
        if m and m.group(2) == collection:
            ecv.add(int(m.group(1)))
            n = len(m.group(3).split())
            mb = float(m.group(4)) * {"KiB": 1 / 1024, "MiB": 1, "GiB": 1024}.get(m.group(5), 1 / 1048576)
            snap[f"ec_shards_on_{disk}"] = snap.get(f"ec_shards_on_{disk}", 0) + n
            snap[f"ec_mb_on_{disk}"] = snap.get(f"ec_mb_on_{disk}", 0) + mb
    snap["ec_volumes"] = len(ecv)
    return {k: (round(v, 1) if isinstance(v, float) else v) for k, v in snap.items()}


def tclient(S, cluster=P, i=0, **kw):
    if TB == "archive":
        return S.s3(S.inv.s3s(cluster)[i], **kw)
    return S.s3(S.inv.s3s(cluster)[i], access="sw_admin", secret_env="SW_S3_ADMIN_SECRET", **kw)


def landing_probe(S, cluster, bucket, key):
    """Write one object and report (disk type, volume class) of the volume it landed in."""
    c = tclient(S, cluster)
    c.put_object(Bucket=bucket, Key=key, Body=os.urandom(4096))
    code, body = sw.http_get(f"http://{S.inv.filers(cluster)[0]}/buckets/{bucket}/{key}?metadata=true")
    fid = (json.loads(body).get("chunks") or [{}])[0].get("file_id", "")
    vid = int(fid.split(",")[0]) if fid else -1
    for m in S.inv.masters(cluster):
        c2, b2 = sw.http_get(f"http://{m}/vol/status")
        if c2 == 200:
            for dc, racks in json.loads(b2)["Volumes"]["DataCenters"].items():
                for rack, nodes in racks.items():
                    for node, vols in nodes.items():
                        for v in vols or []:
                            if v["Id"] == vid:
                                return v["DiskType"] or "hdd", v.get("VolumeClass", "")
    return None, None


def finish_transitions(S, cluster, collection, min_shards):
    """Operator finishing (finding E-03): drop replicated copies of volumes whose EC set is complete."""
    import re
    cnt = {}
    for v_, n_ in re.findall(r"ec volume id:(\d+) collection:" + re.escape(collection) + r" shards:\[([\d ]*)\]",
                             S.sh(cluster, "volume.list")):
        cnt[int(v_)] = cnt.get(int(v_), 0) + len(n_.split())
    full = {v for v, c in cnt.items() if c >= min_shards}
    left = [(v[3], v[0]) for v in base.volumes(S.inv, cluster) if v[4] == collection and v[3] in full]
    if left:
        S.sh(cluster, "\n".join(["lock"] + [f"volume.delete -node={n} -volumeId={v}" for v, n in left] + ["unlock"]))
    return len(left)


PROD_PROFILE = {"name": "archive-hot-cold", "landing": {"diskType": "ssd", "replication": "010"},
                "tiers": [{"name": "cold-ec", "when": {"quietForSeconds": 7776000},
                           "ec": {"diskType": "hdd", "dataShards": 10, "parityShards": 4}}]}


def profile(name, quiet, k, m):
    return {"name": name, "landing": {"diskType": "ssd", "replication": "010"},
            "tiers": [{"name": f"cold-ec-{k}-{m}", "when": {"quietForSeconds": quiet},
                       "ec": {"diskType": "hdd", "dataShards": k, "parityShards": m}}]}


def build(S: base.Suite, E: Ent):
    inv = S.inv
    tests = []
    T = lambda *a, **k: (lambda f: tests.append(S.test(*a, **k)(f)) or f)  # noqa: E731
    s3p = lambda i=0: S.s3(inv.s3s(P)[i])  # noqa: E731
    s3d = lambda i=0: S.s3(inv.s3s(D)[i])  # noqa: E731

    # ------------------------------------------------------------------ edition
    @T("X01", "enterprise", "Enterprise build and default dev/test licence active on every master",
       "masters log the Enterprise licence (25 TB dev/test, no key file)")
    def _():
        out = {}
        for c in (P, D):
            for n in inv.nodes(c, "master"):
                logf = Path(inv.prefix(c, n["host"])) / "log/seaweed-master.log"
                txt = logf.read_text(errors="replace") if logf.exists() else ""
                out[n["host"]] = "25TB licence" if "enterprise license (25TB" in txt else "NOT FOUND"
        ver = subprocess.run([inv.g["weed_binary"], "version"], capture_output=True, text=True).stdout.split("\n")[0]
        ok = all(v == "25TB licence" for v in out.values()) and "enterprise" in ver
        return ("PASS" if ok else "FAIL"), {"version": ver, "masters": out}

    # ------------------------------------------------------------------ tiering
    @T("X02", "tiering", "Rendered production tiering policy validates and applies (tiering.apply / get / status)",
       "policy accepted, archive bound to archive-hot-cold, 7,776,000 s rung")
    def _():
        doc = json.loads((inv.build_dir / "primary/_cluster/tiering.json").read_text())
        out = E.tiering(P, doc)
        got = S.sh(P, "tiering.get")
        ok = "applied tiering config" in out and '"quietForSeconds": "7776000"' in got and "archive-hot-cold" in got
        E.shot(P, "/", "1. Deploy", "Primary Admin dashboard right after swctl deploy + bootstrap (Enterprise 4.48)")
        E.shot(P, "/cluster/volume-servers", "1. Deploy", "6 volume servers: hot (ssd) and cold (hdd) per rack r1-r3")
        E.shot(P, "/storage/tiering", "2. Tiering policy", "Volume Tiering after tiering.apply of the production policy (archive -> archive-hot-cold, 7,776,000 s)")
        E.shot(P, "/plugin/lanes/default", "2. Tiering policy", "Plugin workers registered with the Admin (default lane)")
        return ("PASS" if ok else "FAIL"), {"apply": out.strip()[-160:], "v1_schema_note":
                                            "v1.0 JSON ('pattern'/'class') is rejected: unknown field \"pattern\"; "
                                            "correct protojson is {'match': ['**'], 'volumeClass': ...}"}

    @T("X03", "tiering", f"Worker-driven demotion SSD(010) -> EC 10+4 on HDD with a {QUIET}s test rung",
       "quiet volumes demoted by the volume_tiering worker; shards only on HDD nodes; all objects readable")
    def _():
        if TB != BUCKET:
            S.sh(P, f"s3.bucket.create -name {TB}")
        out = E.tiering(P, {"selectors": [{"match": ["**"], "volumeClass": "archive-data"}],
                            "profiles": [PROD_PROFILE, profile("archive-test", QUIET, 10, 4)],
                            "bindings": [{"collectionPattern": TB, "volumeClass": "archive-data",
                                          "profileName": "archive-test"},
                                         {"collectionPattern": BUCKET, "volumeClass": "archive-data",
                                          "profileName": "archive-hot-cold"}]})
        # the policy reaches filers/S3 asynchronously: probe until a write lands classed on SSD
        t_apply, n, landed = time.time(), 0, (None, None)
        while time.time() - t_apply < 600:
            n += 1
            landed = landing_probe(S, P, TB, f"probe/landing-{n}")
            if landed == ("ssd", "archive-data"):
                break
            time.sleep(10)
        S.ctx["policy_propagation_s"] = round(time.time() - t_apply)
        ds = base.put_dataset(tclient(S, retries=4), "tier/old", 300, [64 * 1024, 512 * 1024], bucket=TB)
        S.ctx["ds_tier"] = ds
        time.sleep(8)                                         # sizes arrive with the next heartbeat
        before = tier_snapshot(S, P, TB)
        S.ctx["tier_before"] = before
        E.shot(P, "/storage/volumes", "3. Hot -> Cold (before)", "BEFORE demotion: archive data on SSD volumes, replication 010")
        E.shot(P, "/storage/collections", "3. Hot -> Cold (before)", "Collections before demotion")
        st0 = S.sh(P, "tiering.status -v")
        E.adm[P].enable_job("volume_tiering", detection_interval_minutes=1)
        t_start = time.time()

        # the volumes that hold the 300 'old' objects (probe objects live in other landing volumes)
        old_vids = set()
        for k in list(ds):
            c2, b2 = sw.http_get(f"http://{inv.filers(P)[0]}/buckets/{TB}/{k}?metadata=true")
            for ch in (json.loads(b2).get("chunks") or []):
                old_vids.add(int(ch["file_id"].split(",")[0]))

        def demoted():
            sh = base.ec_shards(S, P)
            still_hot = {v[3] for v in base.volumes(inv, P) if v[3] in old_vids and v[2] == "ssd"}
            done = old_vids <= set(sh) and not still_hot
            return done, {"old_volumes": sorted(old_vids), "ec": sorted(v for v in sh if v in old_vids),
                          "still_on_ssd": sorted(still_hot)}
        time.sleep(QUIET)
        E.adm[P].detect("volume_tiering")

        def encoded():          # worker has produced a full 14-shard EC set for every old volume
            sh = base.ec_shards(S, P)
            full = {v for v, nodes in sh.items() if sum(nodes.values()) >= 14}
            return old_vids <= full, sorted(full & old_vids)
        enc_ok, enc_s, _ = wait(encoded, 600, 10)
        S.ctx["encode_s"] = enc_s
        ok, dt, last = wait(demoted, 180, 10)       # give the worker time to drop the replicated source
        finishing = None
        if enc_ok and not ok:
            # Observed on 4.48-enterprise in the lab: EC set complete, but one replicated copy of each
            # volume stays on SSD (tiering.status TRANSITIONING; worker log "needs finishing ... no
            # executor"). Operator finishing: drop the leftover replica once the EC set is verified.
            leftovers = [(v[3], v[0]) for v in base.volumes(inv, P) if v[3] in old_vids and v[2] == "ssd"]
            cmds = ["lock"] + [f"volume.delete -node={n} -volumeId={vid}" for vid, n in leftovers] + ["unlock"]
            S.sh(P, "\n".join(cmds))
            finishing = {"leftover_replicas_deleted": len(leftovers),
                         "status_before": S.sh(P, "tiering.status").strip().splitlines()[-1:]}
            ok, dt, last = wait(demoted, 120, 10)
        sh = base.ec_shards(S, P)
        pl = base.ec_placement(sh)
        bad = base.verify_dataset(tclient(S, i=1), ds, bucket=TB)
        S.ctx["ec_dist_104"] = sh
        time.sleep(8)
        after = tier_snapshot(S, P, TB)
        S.ctx["tier_after"] = after
        E.shot(P, "/storage/ec-shards", "3. Hot -> Cold (after)", "AFTER demotion: archive volumes are EC 10+4 shards on the HDD tier")
        E.shot(P, "/storage/volumes", "3. Hot -> Cold (after)", "AFTER demotion: SSD volumes emptied (only new landing volumes remain)")
        E.shot(P, "/storage/tiering", "3. Hot -> Cold (after)", "Volume Tiering overview after the worker demoted the quiet volumes")
        res = {"policy_propagation_s": S.ctx["policy_propagation_s"], "landing_probes": n, "landed": landed,
               "encode_complete_s": S.ctx.get("encode_s"), "operator_finishing": finishing,
               "before": before, "after": after,
               "apply": out.strip()[-80:], "demotion_seconds_after_quiet": round(time.time() - t_start - QUIET),
               "ec_volumes": len(sh), **pl, "unreadable": len(bad), "last": last,
               "tiering_status_before": st0.strip().splitlines()[-1][:160] if st0.strip() else ""}
        if not ok:
            return "FAIL", res
        if pl["shards_on_hot_nodes_pct"] > 0:
            return "FINDING", {**res, "note": "worker left EC shards on SSD nodes"}
        if finishing and not bad:
            return "FINDING", {**res, "note": "EC placement correct, but the replicated source was not dropped "
                                              "automatically; operator finishing (volume.delete of the leftover "
                                              "replica after verifying 14 shards) was needed"}
        return ("PASS" if not bad else "FAIL"), res

    @T("X04", "tiering", "Cold node loss with EC 10+4 on 3 cold nodes (Enterprise placement)",
       "design expectation (v1.0): readable; arithmetic says 5 shards/node > 4 parity")
    def _():
        sh = S.ctx.get("ec_dist_104") or base.ec_shards(S, P)
        per = {}
        for vid, nodes in sh.items():
            for n, k in nodes.items():
                per[n] = max(per.get(n, 0), k)
        cold = [s for s in inv.services(P) if s.kind == "volume_cold"][0]
        S.kill(P, cold.host)
        time.sleep(6)
        ds = S.ctx.get("ds_tier", {})
        bad = base.verify_dataset(tclient(S, retries=1, timeout=3), ds, bucket=TB)
        E.shot(P, "/storage/ec-shards", "4. EC failure domains", "cold-01 stopped: EC 10+4 volumes missing 5 of 14 shards")
        S.start(P, cold.host)
        sw.gate_volume(inv, P, 60)
        time.sleep(5)
        after = base.verify_dataset(tclient(S), ds, bucket=TB)
        ev = {"max_shards_per_node": per, "unreadable_while_down": f"{len(bad)}/{len(ds)}",
              "unreadable_after_restart": len(after)}
        return ("FINDING" if bad and not after else ("PASS" if not bad else "FAIL")), ev

    @T("X05", "tiering", "Remediation: Enterprise custom EC 6+3 survives a cold node loss on 3 racks",
       "max 3 shards per node <= 3 parity: 0 unreadable with one cold node down")
    def _():
        S.sh(P, "s3.bucket.create -name archive63")
        # archive goes back to the 90-day production profile: a 60 s rung would keep demoting the *active*
        # landing volumes, and a mid-transition volume makes replicated writes fail (finding E-07)
        E.tiering(P, {"selectors": [{"match": ["**"], "volumeClass": "archive-data"}],
                      "profiles": [PROD_PROFILE, profile("archive-63", QUIET, 6, 3)],
                      "bindings": [{"collectionPattern": BUCKET, "volumeClass": "archive-data",
                                    "profileName": "archive-hot-cold"},
                                   {"collectionPattern": "archive63", "volumeClass": "archive-data",
                                    "profileName": "archive-63"}]})
        c = S.s3(inv.s3s(P)[0], access="sw_admin", secret_env="SW_S3_ADMIN_SECRET", retries=4)   # archive-app is scoped to 'archive'
        # landing gate for the new binding (E-02): otherwise the writes land unclassed on HDD
        sw.verify_landing(inv, P, "archive63", "ssd", "archive-data", timeout=300)
        ds = {}
        for i in range(200):
            body = os.urandom(256 * 1024)
            c.put_object(Bucket="archive63", Key=f"t/{i:04d}", Body=body)
            ds[f"t/{i:04d}"] = base.md5(body)
        time.sleep(QUIET)
        E.adm[P].detect("volume_tiering")

        def ec63():
            cnt = {}
            for v_, n_ in re.findall(r"ec volume id:(\d+) collection:archive63 shards:\[([\d ]*)\]",
                                     S.sh(P, "volume.list")):
                cnt[int(v_)] = cnt.get(int(v_), 0) + len(n_.split())
            return cnt

        def done():          # every EC'd archive63 volume has 9 shards and no replicated copy left on SSD
            cnt = ec63()
            left = [v for v in base.volumes(inv, P) if v[4] == "archive63" and v[3] in cnt]
            return (len(cnt) >= 3 and all(c >= 9 for c in cnt.values()) and not left), (len(cnt), len(left))
        ok, dt, _ = wait(done, 180, 10)
        finishing = 0
        if not ok:
            full, _, _ = wait(lambda: (len(ec63()) >= 3 and all(c >= 9 for c in ec63().values()), None), 600, 10)
            if full:      # same operator finishing as X03 (finding E-03)
                finishing = finish_transitions(S, P, "archive63", 9)
                ok, dt, _ = wait(done, 120, 10)
        txt = S.sh(P, "volume.list")
        per, node = {}, None
        for line in txt.splitlines():
            m = re.search(r"DataNode (\S+)", line)
            if m:
                node = m.group(1)
            m = re.search(r"ec volume id:(\d+) collection:archive63 .*?shards:\[([\d ]*)\]", line)
            if m and node:
                per.setdefault(int(m.group(1)), {}).setdefault(node, 0)
                per[int(m.group(1))][node] += len(m.group(2).split())
        worst = max((max(v.values()) for v in per.values()), default=0)
        cold = [s for s in inv.services(P) if s.kind == "volume_cold"][0]
        E.shot(P, "/storage/ec-shards", "4. EC failure domains", "archive63 bucket demoted to EC 6+3 (max 3 shards per cold node)")
        S.kill(P, cold.host)
        time.sleep(6)
        bad = []
        for k, h in ds.items():
            try:
                b = S.s3(inv.s3s(P)[0], access="sw_admin", secret_env="SW_S3_ADMIN_SECRET", retries=1,
                         timeout=3).get_object(Bucket="archive63", Key=k)["Body"].read()
                if base.md5(b) != h:
                    bad.append(k)
            except Exception:  # noqa: BLE001
                bad.append(k)
        S.start(P, cold.host)
        sw.gate_volume(inv, P, 60)
        ev = {"demoted": ok, "operator_finishing_replicas_deleted": finishing, "ec_volumes": len(per),
              "worst_shards_on_one_node": worst,
              "unreadable_with_cold01_down": f"{len(bad)}/{len(ds)}"}
        return ("PASS" if ok and per and not bad else "FAIL"), ev

    # ------------------------------------------------------------------ data movement
    @T("X10", "data-movement", "Initial snapshot completeness on a pre-loaded primary (written via all 3 gateways)",
       "policy reaches caught_up and listing+ETag diff Primary vs DR is 0 - no reconcile needed")
    def _():
        gws = [S.s3(inv.s3s(P)[i], retries=4) for i in range(3)]

        def writes_healthy():          # stale volume layout after tiering can fail writes for a while
            okc = 0
            for i in range(20):
                try:
                    gws[i % 3].put_object(Bucket=BUCKET, Key=f"health/{i}", Body=os.urandom(70000))
                    okc += 1
                except Exception:  # noqa: BLE001
                    pass
            return okc == 20, okc
        S.ctx["finished_before_dm"] = finish_transitions(S, P, BUCKET, 14)
        S.ctx["writes_health"] = wait(writes_healthy, 300, 10)[:2]
        for i, g in enumerate(gws):
            base.put_dataset(g, f"seed/gw{i}", 800, [4 * 1024, 32 * 1024, 256 * 1024])
        E.adm[P].put_target(E.target("primary-to-dr", D))
        E.adm[P].enable_job("data_movement", detection_interval_minutes=1)
        E.shot(P, "/data-movement/targets", "5. Data Movement setup", "Target primary-to-dr: DR filer gRPC 127.20.4.11:18888 with mTLS files")
        t0 = time.time()
        E.adm[P].put_policy(E.policy("archive-primary-to-dr", "primary-to-dr", "/buckets/archive", "/buckets/archive"))
        time.sleep(4)
        E.shot(P, "/data-movement", "5. Data Movement setup", "Policy archive-primary-to-dr created: CONTINUOUS, initial snapshot ON, deletes ON")
        seeded_shot = {"done": False}
        phases = []

        def caught():
            st = E.status(P, "archive-primary-to-dr")
            ph = st.get("phase")
            if not phases or phases[-1] != ph:
                phases.append(ph)
            if ph == "seeding" and not seeded_shot["done"]:
                seeded_shot["done"] = True
                E.shot(P, "/data-movement", "6. Initial snapshot", "Policy in SEEDING: walking the existing tree")
            return ph == "caught_up", st
        ok, dt, st = wait(caught, 900, 3)
        E.shot(P, "/data-movement", "6. Initial snapshot", "Policy CAUGHT_UP after the initial snapshot")
        E.shot(P, "/plugin/lanes/movement", "6. Initial snapshot", "Movement lane: data_movement jobs executed by the workers")
        r = rec.reconcile(None, None, BUCKET, src_client=s3p(), dst_client=s3d())
        S.ctx["dm_ok"] = ok
        S.ctx["seed"] = {"objects": r["src_objects"], "seconds": dt, "bytesMoved": st.get("bytesMoved"),
                         "filesMoved": st.get("filesMoved")}
        ev = {"phases": phases, "seconds_to_caught_up": dt, "filesMoved": st.get("filesMoved"),
              "bytesMoved_mb": round((st.get("bytesMoved") or 0) / 1048576, 1),
              "snapshotEntriesWalked": st.get("snapshotEntriesWalked"),
              "src_objects": r["src_objects"], "missing_on_dr": r["missing_on_dst"], "size_mismatch": r["size_mismatch"]}
        return ("PASS" if ok and r["missing_on_dst"] == 0 and r["size_mismatch"] == 0 else "FAIL"), ev

    @T("X11", "data-movement", "CONTINUOUS lag (PUT via all 3 gateways -> visible on DR)", "lag well under RPO")
    def _():
        cps, cd = [s3p(i) for i in range(3)], s3d()
        lags = []
        for i in range(30):
            k, body = f"dr/lag/{i}", os.urandom(32 * 1024)
            cps[i % 3].put_object(Bucket=BUCKET, Key=k, Body=body)
            t0 = time.time()
            while True:
                try:
                    if base.md5(cd.get_object(Bucket=BUCKET, Key=k)["Body"].read()) == base.md5(body):
                        break
                except Exception:  # noqa: BLE001
                    pass
                if time.time() - t0 > 120:
                    return "FAIL", f"{k} not replicated in 120s"
                time.sleep(0.1)
            lags.append(time.time() - t0)
        st = E.status(P, "archive-primary-to-dr")
        return "PASS", {"p50_s": round(statistics.median(lags), 2), "p95_s": round(sorted(lags)[28], 2),
                        "max_s": round(max(lags), 2), "reported_lagSeconds": st.get("lagSeconds"),
                        "phase": st.get("phase")}

    @T("X12", "data-movement", "Delete propagation (mirror)", "delete on Primary removes the object on DR")
    def _():
        s3p().delete_object(Bucket=BUCKET, Key="dr/lag/0")
        t0 = time.time()
        while time.time() - t0 < 120:
            try:
                s3d().head_object(Bucket=BUCKET, Key="dr/lag/0")
                time.sleep(0.2)
            except Exception:  # noqa: BLE001
                return "PASS", {"propagation_s": round(time.time() - t0, 2)}
        return "FAIL", "not propagated in 120 s"

    def gap_case(prefix, fault, restore, hold=20):
        pool = base.GatewayPool(S, P)
        load = base.Load(pool, prefix).start()
        time.sleep(3)
        fault()
        time.sleep(hold / 2)
        E.shot(P, "/data-movement", "7. Replication under failure", f"{prefix}: fault active, policy status during the outage")
        time.sleep(hold / 2)
        restore()
        time.sleep(5)
        load.stop()
        pool.close()
        t0 = time.time()
        missing = list(load.acked)
        while time.time() - t0 < 240:
            missing = base.verify_dataset(s3d(), load.acked, workers=16)
            if not missing:
                break
            time.sleep(5)
        return {"acked": len(load.acked), "missing_on_dr": len(missing),
                "catch_up_s": round(time.time() - t0, 1), "load": load.summary(0)}

    @T("X13", "data-movement", "Primary filer-01 crash while writing (the v1/OSS gap scenario)",
       "every acknowledged write reaches DR after recovery - no reconcile needed")
    def _():
        h = [s for s in inv.services(P) if s.kind == "filer"][0].host
        ev = gap_case("dr/srcfail", lambda: S.kill(P, h), lambda: (S.start(P, h), sw.gate_http(inv, P, "filer", 60)))
        return ("PASS" if ev["missing_on_dr"] == 0 else "FAIL"), ev

    @T("X14", "data-movement", "DR target filer crash (the filer the target points at)",
       "replication pauses, resumes, no gaps")
    def _():
        h = [s for s in inv.services(D) if s.kind == "filer"][0].host
        ev = gap_case("dr/dstfail", lambda: S.kill(D, h), lambda: (S.start(D, h), sw.gate_http(inv, D, "filer", 60)))
        return ("PASS" if ev["missing_on_dr"] == 0 else "FAIL"), ev

    @T("X15", "data-movement", "All Data Movement workers crash for 20 s",
       "leases expire, a restarted worker resumes from the checkpoint, no gaps")
    def _():
        ws = [s for s in inv.services(P) if s.kind == "worker"]
        ev = gap_case("dr/workerfail", lambda: [S.r.stop(w, hard=True) for w in ws],
                      lambda: [S.r.start(w) for w in ws])
        if ev["missing_on_dr"]:
            jobs = E.adm[P].jobs()
            jobs = jobs if isinstance(jobs, list) else jobs.get("jobs", [])
            dm = [{"job": j.get("job_id", "")[-28:], "state": j.get("state"), "worker": j.get("worker_id"),
                   "updated": j.get("updated_at")} for j in jobs if j.get("job_type") == "data_movement"][:3]
            st = E.status(P, "archive-primary-to-dr")
            E.shot(P, "/data-movement", "7. Replication under failure",
                   "After all workers were killed and restarted: status still shows the dead worker's last state")
            E.shot(P, "/plugin/lanes/movement", "7. Replication under failure",
                   "Movement lane: data_movement job still 'running' on the dead worker id")
            ev.update({"dm_jobs": dm, "status_phase": st.get("phase"), "status_worker": st.get("workerId"),
                       "status_age_s": round(time.time() - (st.get("updatedAtNs") or 0) / 1e9),
                       "note": "workers killed mid-job: the data_movement job stays 'running' on the dead worker "
                               "(execution timeout 86400 s), replication stops, status keeps the last good phase"})
            return "FINDING", ev
        return "PASS", ev

    @T("X16", "data-movement", "BACKUP mode keeps objects deleted on Primary",
       "backup copy retains a deleted object (deletes OFF)")
    def _():
        S.sh(D, "s3.bucket.create -name archive-backup")
        E.adm[P].put_target(E.target("dr-backup", D))
        E.adm[P].put_policy(E.policy("archive-backup", "dr-backup", "/buckets/archive/backup-test",
                                     "/buckets/archive-backup/backup-test", mode="backup", deletes=False))
        c = s3p()
        c.put_object(Bucket=BUCKET, Key="backup-test/keep-me.xml", Body=b"<doc/>")
        ok, dt, _ = wait(lambda: (base.http_get_ok(inv.filers(D)[0], "/buckets/archive-backup/backup-test/keep-me.xml"), None), 300, 3)
        c.delete_object(Bucket=BUCKET, Key="backup-test/keep-me.xml")
        time.sleep(20)
        still = base.http_get_ok(inv.filers(D)[0], "/buckets/archive-backup/backup-test/keep-me.xml")
        st = E.status(P, "archive-backup")
        E.shot(P, "/data-movement", "8. Backup policy", "BACKUP policy (deletes OFF) next to the CONTINUOUS mirror")
        ev = {"copied_s": dt, "present_after_primary_delete": still, "phase": st.get("phase")}
        return ("PASS" if ok and still else "FAIL"), ev

    @T("X17", "data-movement", "Where does replicated data land on DR?", "(design note) DR landing tier")
    def _():
        vs = [v for v in base.volumes(inv, D) if v[4] == BUCKET and v[5] > 0]
        ssd = {v[3] for v in vs if v[2] == "ssd"}
        hdd = {v[3] for v in vs if v[2] == "hdd"}
        return ("FINDING" if ssd else "PASS"), {"dr_ssd_volumes": len(ssd), "dr_hdd_volumes": len(hdd),
                                                "note": "seed lands on DR SSD as new data; size DR SSD or use a "
                                                        "short-quiet DR profile during seeding"}

    # ------------------------------------------------------------------ DC loss + failback
    @T("X20", "failover", "Unplanned Primary DC loss under load: RPO with Data Movement",
       "acknowledged writes missing on DR <= RPO window")
    def _():
        pool = base.GatewayPool(S, P)
        load = base.Load(pool, "dr/rpo", interval=0.02).start()
        time.sleep(15)
        t_kill = time.time()
        import concurrent.futures as cf
        with cf.ThreadPoolExecutor(40) as ex:
            list(ex.map(lambda s: S.r.stop(s, hard=True), inv.services(P)))
        load.stop()
        pool.close()
        S.ctx["t_failover"] = t_kill
        time.sleep(10)
        missing = base.verify_dataset(s3d(), load.acked, workers=16)
        lost = round(t_kill - min(load.acked_t[m.split(":")[0]] for m in missing), 2) if missing else 0.0
        S.ctx["rpo_missing"] = [m.split(":")[0] for m in missing]
        return ("PASS" if lost <= (inv.sla.get("rpo_seconds") or 60) else "FAIL"), {
            "acked_puts": len(load.acked), "missing_on_dr": len(missing), "rpo_observed_s": lost}

    @T("X21", "failover", "Activate DR: LIST/GET/PUT with application credentials", "all succeed")
    def _():
        pool = base.GatewayPool(S, D)
        r, _ = pool.call("list_objects_v2", Bucket=BUCKET, Prefix="seed/gw0/", MaxKeys=10)
        ds = base.put_dataset(s3d(2), "dr-test/batch", 50, [8192])
        S.ctx["dr_writes"] = ds
        pool.close()
        E.shot(D, "/", "9. DC failover", "DR Admin dashboard while Primary is down: DR serving LIST/GET/PUT")
        E.shot(D, "/object-store/buckets", "9. DC failover", "DR buckets: archive holds the replicated data plus DR-era writes")
        return ("PASS" if r.get("KeyCount", 0) > 0 else "FAIL"), {
            "list": r.get("KeyCount"), "dr_writes": len(ds), "rto_s": round(time.time() - S.ctx["t_failover"], 1)}

    @T("X22", "failover", "Failback with a MIGRATE policy DR -> Primary (drain fence)",
       "DR-era writes on Primary; policy reports a cut-over-ready phase; forward replication re-established")
    def _():
        for ph in sw.PHASES:
            for s in [x for x in inv.services(P) if x.phase == ph]:
                S.r.start(s)
            if ph in sw.GATES and ph != "sync":
                sw.GATES[ph](inv, S.r, P)
        def admin_up():
            try:
                E.adm[P] = AdminAPI(E.adm[P].base)
                return "policies" in E.adm[P].dm_config(), None
            except Exception as e:  # noqa: BLE001
                return False, str(e)
        wait(admin_up, 180, 3)
        # pause forward mirror while DR is authoritative
        pol = E.policy("archive-primary-to-dr", "primary-to-dr", "/buckets/archive", "/buckets/archive")
        pol["enabled"] = False
        E.adm[P].put_policy(pol)
        E.adm[D].put_target(E.target("dr-to-primary", P))
        E.adm[D].enable_job("data_movement", detection_interval_minutes=1)
        since = str(int((S.ctx["t_failover"] - 60) * 1e9))
        E.adm[D].put_policy(E.policy("failback-dr-to-primary", "dr-to-primary", "/buckets/archive",
                                     "/buckets/archive", mode="migrate", deletes=False, snapshot=False,
                                     start_ns=since))
        phases = []

        def fenced():
            st = E.status(D, "failback-dr-to-primary")
            ph = st.get("phase")
            if not phases or phases[-1] != ph:
                phases.append(ph)
            return ph not in (None, "", "unspecified", "seeding", "catching_up") and \
                not base.verify_dataset(s3p(), S.ctx["dr_writes"]), st
        ok, dt, st = wait(fenced, 600, 5)
        E.shot(D, "/data-movement", "10. Failback", "DR Admin: MIGRATE policy failback-dr-to-primary replaying DR-era writes")
        missing_dr_writes = base.verify_dataset(s3p(), S.ctx["dr_writes"])
        # cut back: re-enable forward mirror
        pol["enabled"] = True
        E.adm[P].put_policy(pol)
        E.adm[D].put_policy({**E.policy("failback-dr-to-primary", "dr-to-primary", "/buckets/archive",
                                        "/buckets/archive", mode="migrate", deletes=False, snapshot=False,
                                        start_ns=since), "enabled": False})
        s3p().put_object(Bucket=BUCKET, Key="dr/after-failback.xml", Body=b"ok")
        ok2, dt2, _ = wait(lambda: (base.http_get_ok(inv.filers(D)[0], "/buckets/archive/dr/after-failback.xml"), None), 180, 2)
        ev = {"migrate_phases": phases, "seconds": dt, "dr_writes_on_primary": f"{len(S.ctx['dr_writes']) - len(missing_dr_writes)}/{len(S.ctx['dr_writes'])}",
              "forward_resumed_s": dt2 if ok2 else None, "last_status": {k: st.get(k) for k in ("phase", "lagSeconds", "postFenceEvents", "lastError")}}
        return ("PASS" if not missing_dr_writes and ok2 else "FAIL"), ev

    @T("X23", "failover", "Post-failback consistency Primary vs DR", "only the RPO-window writes differ")
    def _():
        time.sleep(15)
        r = rec.reconcile(None, None, BUCKET, src_client=s3p(), dst_client=s3d())
        only_p = r["missing_on_dst"]
        E.shot(P, "/data-movement", "10. Failback", "Primary Admin after failback: forward CONTINUOUS mirror re-enabled")
        ev = {**{k: r[k] for k in ("src_objects", "dst_objects", "missing_on_dst", "extra_on_dst", "size_mismatch")},
              "rpo_window_objects": len(S.ctx.get("rpo_missing", []))}
        if r["extra_on_dst"] == 0 and r["size_mismatch"] == 0 and only_p <= len(S.ctx.get("rpo_missing", [])):
            return ("PASS" if only_p == 0 else "FINDING"), ev
        return "FAIL", ev

    return tests


def http_get_ok(filer, path):
    code, _ = sw.http_get(f"http://{filer}{path}")
    return code == 200


base.http_get_ok = http_get_ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--inventory", required=True)
    ap.add_argument("--out", default=str(base.ROOT / "reports/enterprise"))
    ap.add_argument("--only")
    ap.add_argument("--screens", action="store_true", help="capture Admin UI screenshots into <out>/screens")
    a = ap.parse_args()
    inv = sw.Inventory(a.inventory)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    S = base.Suite(inv, out, True)
    E = Ent(S)
    if a.screens:
        from ui_capture import UICapture
        E.ui = UICapture(out / "screens")
    t0 = time.time()
    tests = build(S, E)
    tests = [t for t in tests if t.tid != "X15"] + [t for t in tests if t.tid == "X15"]   # X15 breaks DM (E-08): run last
    for t in tests:
        if a.only and not any(t.tid.startswith(p) for p in a.only.split(",")):
            continue
        t()
    if E.ui:
        E.ui.close()
    summ = {k: sum(1 for r in S.results if r["status"] == k) for k in ("PASS", "FAIL", "FINDING", "SKIP")}
    ver = subprocess.run([inv.g["weed_binary"], "version"], capture_output=True, text=True).stdout.splitlines()[0]
    rep = {"inventory": str(inv.path), "weed_version": ver, "minutes": round((time.time() - t0) / 60, 1),
           "summary": summ, "results": S.results,
           "tiering_before_after": {"before": S.ctx.get("tier_before"), "after": S.ctx.get("tier_after"),
                                    "policy_propagation_s": S.ctx.get("policy_propagation_s")},
           "replication": {"seed": S.ctx.get("seed")}}
    (out / "e2e-enterprise-results.json").write_text(json.dumps(rep, indent=2, default=str))
    print(f"\n==== {summ} ({rep['minutes']} min) -> {out / 'e2e-enterprise-results.json'}")
    return 1 if summ["FAIL"] else 0


if __name__ == "__main__":
    sys.exit(main())
