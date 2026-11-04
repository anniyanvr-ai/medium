#!/usr/bin/env python3
"""
End-to-end acceptance suite for a SeaweedFS Primary + DR deployment.

Runs against any inventory (lab or real). Destructive tests (process kills,
rack loss, primary DC loss) only run with --destructive; never point that at
production.

    python3 tests/e2e_test.py -i inventory/lab.yaml --destructive --out reports/

Result states
    PASS      behaviour matches the design
    FAIL      behaviour does not match the design (defect in process/config)
    FINDING   the platform behaved as the code dictates, but the design
              document's expectation is wrong or a gap exists (action needed)
    SKIP      not applicable (e.g. Enterprise-only feature on OSS build)
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import importlib.machinery
import importlib.util
import io
import json
import os
import random
import re
import statistics
import subprocess
import sys
import threading
import time
import traceback
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

ROOT = Path(__file__).resolve().parents[1]
_loader = importlib.machinery.SourceFileLoader("swctl", str(ROOT / "bin" / "swctl"))
_spec = importlib.util.spec_from_loader("swctl", _loader)
sw = importlib.util.module_from_spec(_spec)
sys.modules["swctl"] = sw
_loader.exec_module(sw)

_l2 = importlib.machinery.SourceFileLoader("s3reconcile", str(ROOT / "bin" / "s3-reconcile"))
_s2 = importlib.util.spec_from_loader("s3reconcile", _l2)
rec = importlib.util.module_from_spec(_s2)
sys.modules["s3reconcile"] = rec
_l2.exec_module(rec)

BUCKET = "archive"


# --------------------------------------------------------------------------- #
# Harness
# --------------------------------------------------------------------------- #
class Suite:
    def __init__(self, inv, out: Path, destructive: bool):
        self.inv, self.out, self.destructive = inv, out, destructive
        self.r = sw.Runner(inv)
        self.results = []
        self.ctx = {}
        self.secrets = {}
        sf = inv.build_dir / "secrets.env"
        if sf.exists():
            for line in sf.read_text().splitlines():
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    self.secrets[k] = v
        for ident in inv.g["s3"]["identities"]:
            self.secrets.setdefault(ident["secret_env"], os.environ.get(ident["secret_env"], ""))

    # ---- recording ------------------------------------------------------ #
    def test(self, tid, category, title, expected, destructive=False):
        def deco(fn):
            def run():
                if destructive and not self.destructive:
                    self.record(tid, category, title, expected, "SKIP", "needs --destructive", 0)
                    return
                t0 = time.time()
                print(f"\n[{tid}] {title}", flush=True)
                try:
                    status, evidence = fn()
                except Exception as e:  # noqa: BLE001
                    status, evidence = "FAIL", f"exception: {e}\n{traceback.format_exc(limit=3)}"
                self.record(tid, category, title, expected, status, evidence, time.time() - t0)
            run.tid = tid
            return run
        return deco

    def record(self, tid, category, title, expected, status, evidence, dt):
        col = {"PASS": "\033[32m", "FAIL": "\033[31m", "FINDING": "\033[33m", "SKIP": "\033[2m"}[status]
        print(f"  {col}{status}\033[0m ({dt:.1f}s) {str(evidence)[:400]}", flush=True)
        self.results.append({"id": tid, "category": category, "title": title, "expected": expected,
                             "status": status, "evidence": evidence, "seconds": round(dt, 1)})

    # ---- clients -------------------------------------------------------- #
    def s3(self, endpoint, access="archive_app", secret_env="SW_S3_ARCHIVE_APP_SECRET", secret=None,
           retries=1, timeout=5):
        return boto3.client(
            "s3", endpoint_url=f"http://{endpoint}", aws_access_key_id=access,
            aws_secret_access_key=secret if secret is not None else self.secrets[secret_env],
            region_name="us-east-1",
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": retries, "mode": "standard"},
                          connect_timeout=timeout, read_timeout=timeout * 3))

    def sh(self, cluster, cmd, timeout=600):
        return sw.weed_shell(self.inv, cluster, cmd, timeout=timeout)

    def svc(self, cluster, host=None, kind=None):
        return [s for s in self.inv.services(cluster)
                if (host is None or s.host == host) and (kind is None or s.kind == kind)]

    def kill(self, cluster, host=None, kind=None, hard=True):
        for s in self.svc(cluster, host, kind):
            self.r.stop(s, hard=hard)

    def start(self, cluster, host=None, kind=None):
        for s in sorted(self.svc(cluster, host, kind), key=lambda s: sw.PHASES.index(s.phase)):
            self.r.start(s)

    def all_up(self, cluster):
        return all(self.r.is_running(s) for s in self.inv.services(cluster) if s.kind != "filer_sync")


class GatewayPool:
    """Emulates the F5 pool: 1s /healthz monitor + round-robin over healthy members,
    plus the SDK's retry on another member after a connection error."""

    def __init__(self, suite: Suite, cluster, deep=False):
        self.suite, self.cluster = suite, cluster
        self.members = suite.inv.s3s(cluster)
        self.clients = {m: suite.s3(m) for m in self.members}
        self.healthy = set(self.members)
        self._stop = threading.Event()
        self._i = 0
        self.lock = threading.Lock()
        self.t = threading.Thread(target=self._monitor, daemon=True)
        self.t.start()

    def _monitor(self):
        while not self._stop.is_set():
            for m in self.members:
                code, _ = sw.http_get(f"http://{m}/healthz", timeout=1)
                with self.lock:
                    (self.healthy.add if code == 200 else self.healthy.discard)(m)
            time.sleep(1)

    def pick(self, exclude=()):
        with self.lock:
            cand = [m for m in self.members if m in self.healthy and m not in exclude] or \
                   [m for m in self.members if m not in exclude] or self.members
            self._i += 1
            return cand[self._i % len(cand)]

    def call(self, op, **kw):
        tried = []
        last = None
        for _ in range(3):
            m = self.pick(tried)
            tried.append(m)
            try:
                return getattr(self.clients[m], op)(**kw), m
            except (EndpointConnectionError, ConnectionError) as e:
                last = e
                with self.lock:
                    self.healthy.discard(m)
            except ClientError as e:
                last = e
                if e.response["ResponseMetadata"]["HTTPStatusCode"] < 500:
                    raise
            except Exception as e:  # noqa: BLE001  (read timeouts etc.)
                last = e
        raise last

    def close(self):
        self._stop.set()


class Load:
    """Continuous PUT+GET(verify) workload through a GatewayPool."""

    def __init__(self, pool: GatewayPool, prefix, size=64 * 1024, interval=0.05):
        self.pool, self.prefix, self.size, self.interval = pool, prefix, size, interval
        self.events = []       # (t, op, ok, err)
        self.acked = {}        # key -> md5 of successfully PUT objects
        self.acked_t = {}      # key -> time the PUT was acknowledged
        self.lat = []          # (t, seconds) successful PUT latency
        self._stop = threading.Event()
        self.t = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        i = 0
        while not self._stop.is_set():
            key = f"{self.prefix}/{i:06d}.bin"
            body = os.urandom(self.size)
            t = time.time()
            try:
                self.pool.call("put_object", Bucket=BUCKET, Key=key, Body=body)
                self.acked[key] = hashlib.md5(body).hexdigest()
                self.acked_t[key] = time.time()
                self.lat.append((t, time.time() - t))
                self.events.append((t, "put", True, ""))
            except Exception as e:  # noqa: BLE001
                self.events.append((t, "put", False, type(e).__name__ + ":" + str(e)[:80]))
            if self.acked:
                k = random.choice(list(self.acked)[-50:])
                t = time.time()
                try:
                    r, _ = self.pool.call("get_object", Bucket=BUCKET, Key=k)
                    ok = hashlib.md5(r["Body"].read()).hexdigest() == self.acked[k]
                    self.events.append((t, "get", ok, "" if ok else "checksum mismatch"))
                except Exception as e:  # noqa: BLE001
                    self.events.append((t, "get", False, type(e).__name__ + ":" + str(e)[:80]))
            i += 1
            time.sleep(self.interval)

    def start(self):
        self.t.start()
        return self

    def stop(self):
        self._stop.set()
        self.t.join(30)

    def summary(self, since=0.0):
        ev = [e for e in self.events if e[0] >= since]
        fails = [e for e in ev if not e[2]]
        win = (max(e[0] for e in fails) - min(e[0] for e in fails)) if fails else 0.0
        errs = sorted({e[3].split(":")[0] for e in fails})
        lat = sorted(x[1] for x in self.lat if x[0] >= since) or [0]
        starts = sorted(e[0] for e in self.events if e[0] >= since - 60)
        gaps = [b - a for a, b in zip(starts, starts[1:]) if b >= since]
        return {"max_stall_s": round(max(gaps, default=0), 1),"ops": len(ev), "failed": len(fails), "error_window_s": round(win, 1), "errors": errs,
                "put_p50_ms": round(lat[len(lat) // 2] * 1000), "put_max_ms": round(lat[-1] * 1000)}


def md5(b):
    return hashlib.md5(b).hexdigest()


def put_dataset(client, prefix, n, sizes, bucket=None):
    bucket = bucket or BUCKET
    out = {}

    def one(i):
        body = os.urandom(random.choice(sizes))
        key = f"{prefix}/{i:05d}.xml"
        client.put_object(Bucket=bucket, Key=key, Body=body)
        return key, md5(body)
    with cf.ThreadPoolExecutor(8) as ex:
        for k, h in ex.map(one, range(n)):
            out[k] = h
    return out


def verify_dataset(client, ds, workers=8, bucket=None):
    bucket = bucket or BUCKET
    bad = []

    def one(item):
        k, h = item
        try:
            r = client.get_object(Bucket=bucket, Key=k)
            return k if md5(r["Body"].read()) != h else None
        except Exception as e:  # noqa: BLE001
            return f"{k}:{type(e).__name__}"
    with cf.ThreadPoolExecutor(workers) as ex:
        bad = [x for x in ex.map(one, ds.items()) if x]
    return bad


def volumes(inv, cluster):
    """[(node_url, rack, disk_type, vid, collection, size)] from master /vol/status."""
    for m in inv.masters(cluster):
        code, body = sw.http_get(f"http://{m}/vol/status")
        if code == 200:
            out = []
            for dc, racks in json.loads(body)["Volumes"]["DataCenters"].items():
                for rack, nodes in racks.items():
                    for node, vols in nodes.items():
                        for v in vols or []:
                            out.append((node, rack, v["DiskType"] or "hdd", v["Id"], v["Collection"], v["Size"]))
            return out
    return []


def ec_shards(suite, cluster):
    """{vid: {node: nshards}} parsed from `volume.list`."""
    txt = suite.sh(cluster, "volume.list")
    res, node = {}, None
    for line in txt.splitlines():
        m = re.search(r"DataNode (\S+)", line)
        if m:
            node = m.group(1)
        m = re.search(r"ec volume id:(\d+).*?shards:\[([\d ]*)\]", line)
        if m and node:
            d = res.setdefault(int(m.group(1)), {})
            d[node] = d.get(node, 0) + len(m.group(2).split())
    return res


def ec_placement(shards):
    hot = sum(n for nodes in shards.values() for node, n in nodes.items() if ":8080" in node)
    tot = sum(n for nodes in shards.values() for n in nodes.values()) or 1
    worst = max((max(nodes.values()) for nodes in shards.values()), default=0)
    incomplete = sorted(v for v, nodes in shards.items() if sum(nodes.values()) != 14)
    return {"shards_total": tot, "shards_on_hot_nodes_pct": round(100 * hot / tot, 1),
            "worst_shards_on_one_node": worst, "volumes_not_14_shards": incomplete[:5]}


def leader(inv, cluster):
    for m in inv.masters(cluster):
        code, body = sw.http_get(f"http://{m}/cluster/status", timeout=2)
        if code == 200:
            ld = json.loads(body).get("Leader", "")
            if ld:
                return ld.rsplit(".", 1)[0]          # "ip:port.grpcport" -> "ip:port"
    return None


# --------------------------------------------------------------------------- #
# Test catalogue
# --------------------------------------------------------------------------- #
def build(S: Suite):
    inv = S.inv
    P, D = "primary", "dr"
    tests = []
    T = lambda *a, **k: (lambda f: tests.append(S.test(*a, **k)(f)) or f)  # noqa: E731

    # ============================ A. DEPLOYMENT PROCESS ======================== #
    @T("A01", "process", "Inventory validation catches design defects before deploy",
       "validator runs; FAIL items are real design defects to fix")
    def _():
        res = sw.validate(sw.Inventory(str(ROOT / "inventory" / "production.yaml")))
        fails = [f"{c}: {m}" for l, c, m in res if l == "FAIL"]
        S.ctx["validate"] = res
        return ("FINDING" if fails else "PASS"), {"fail": fails, "counts": {
            l: sum(1 for r in res if r[0] == l) for l in ("PASS", "WARN", "FAIL")}}

    @T("A02", "process", "v1.0 port plan reproduced on real binary (metricsPort = port+10000)",
       "v1.0 document flags start cleanly", destructive=True)
    def _():
        # run the exact v1.0 master & volume flags on a scratch IP
        ip = "127.99.0.1"
        tmp = S.inv.build_dir / "v1-portcheck"
        tmp.mkdir(parents=True, exist_ok=True)
        weed = S.inv.g["weed_binary"]
        out = {}
        good = subprocess.Popen([weed, "master", "-ip=127.99.0.2", "-port=9333", f"-mdir={tmp}/good",
                                 "-metricsIp=127.99.0.2", "-metricsPort=9324"], cwd=tmp,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        (tmp / "good").mkdir(exist_ok=True)
        time.sleep(4)
        for name, args in {
            "master -metricsPort=19333": ["master", f"-ip={ip}", "-port=9333", f"-mdir={tmp}",
                                          "-metricsPort=19333", f"-metricsIp={ip}"],
            "volume -metricsPort=18080": ["volume", f"-ip={ip}", "-port=8080", f"-dir={tmp}",
                                          "-master=127.99.0.2:9333", "-metricsPort=18080", f"-metricsIp={ip}"],
            "s3 -metricsPort=18333": ["s3", f"-ip.bind={ip}", "-port=8333", f"-filer={ip}:8888",
                                      "-metricsPort=18333", f"-metricsIp={ip}",
                                      f"-localSocket={tmp}/s3.sock"],
        }.items():
            p = subprocess.Popen([weed, *args], cwd=tmp, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            time.sleep(4)
            alive = p.poll() is None
            p.kill()
            log = p.communicate(timeout=10)[0].decode(errors="replace")
            hit = [l for l in log.splitlines() if l.startswith("F") and "in use" in l] or \
                  [l for l in log.splitlines() if "address already in use" in l]
            out[name] = ("still running" if alive else f"exited rc={p.returncode}") + \
                        (f"; {hit[0][-160:]}" if hit else "; no bind error logged")
        good.kill()
        broken = any("already in use" in v or "exited" in v for v in out.values())
        return ("FINDING" if broken else "PASS"), out

    @T("A03", "process", "Phased deploy with health gates (primary + DR)",
       "every phase gate passes; total deploy time recorded")
    def _():
        rep = []
        for c in (P, D):
            f = ROOT / "reports" / f"deploy-{c}.json"
            if f.exists():
                rep += json.loads(f.read_text())
        bad = [r for r in rep if not r["ok"]]
        return ("PASS" if rep and not bad else "FAIL"), rep

    @T("A04", "process", "Deploy is idempotent (re-run on a running cluster)",
       "no restarts, all gates pass again")
    def _():
        pids_before = {s.name + s.host: (S.r._local_paths(s)[0].read_text() if S.inv.local and
                       S.r._local_paths(s)[0].exists() else "") for s in S.inv.services(P)}
        rc = sw.main(["-i", str(S.inv.path), "deploy", "--cluster", P])
        pids_after = {s.name + s.host: (S.r._local_paths(s)[0].read_text() if S.inv.local and
                      S.r._local_paths(s)[0].exists() else "") for s in S.inv.services(P)}
        restarted = [k for k in pids_before if pids_before[k] != pids_after[k]]
        return ("PASS" if rc == 0 and not restarted else "FAIL"), {"rc": rc, "restarted": restarted}

    @T("A05", "process", "Every Prometheus target answers (port plan works end-to-end)",
       "all metrics endpoints return SeaweedFS_* series")
    def _():
        bad, n = [], 0
        for c in (P, D):
            for s in S.inv.services(c):
                if s.metrics_port and s.kind != "filer_sync":
                    n += 1
                    code, body = sw.http_get(f"http://{s.ip}:{s.metrics_port}/metrics")
                    if code != 200 or b"SeaweedFS_" not in body and b"go_goroutines" not in body:
                        bad.append(f"{c}/{s.host}/{s.name}:{s.metrics_port}={code}")
        return ("PASS" if not bad else "FAIL"), {"targets": n, "bad": bad}

    # ============================ B. TOPOLOGY ================================== #
    @T("B01", "topology", "Master Raft quorum: 3 members, 1 leader (each cluster)", "1 leader, 2 followers")
    def _():
        out = {}
        for c in (P, D):
            ok, dt, det = sw.gate_master(inv, c, 10)
            out[c] = det
            if not ok:
                return "FAIL", out
        return "PASS", out

    @T("B02", "topology", "Volume servers registered with correct DC / rack (+ disk type)",
       "each DC: 6 servers, one hot + one cold per rack; disk type confirmed by volume placement (C04/F02)")
    def _():
        out = {}
        ok = True
        for c in (P, D):
            topo = sw.topology(inv, c)
            rows = []
            for dc in topo["DataCenters"]:
                for rk in dc["Racks"]:
                    for dn in rk["DataNodes"]:
                        rows.append(f"{dc['Id']}/{rk['Id']}/{dn['Url']}")
            exp = sorted(f"{inv.clusters[c]['datacenter']}/{n['rack']}/{n['ip']}:{inv.port(r, cluster=c)}"
                         for n in inv.nodes(c) for r in ("volume_hot", "volume_cold") if r in n["roles"])
            out[c] = {"registered": len(rows), "mismatch": sorted(set(exp) ^ set(rows))}
            ok &= not out[c]["mismatch"] and len(rows) == 6
        return ("PASS" if ok else "FAIL"), out

    @T("B03", "topology", "DR is an independent cluster (no shared quorum)", "disjoint master sets & leaders")
    def _():
        lp, ld = leader(inv, P), leader(inv, D)
        disjoint = not (set(inv.masters(P)) & set(inv.masters(D)))
        return ("PASS" if disjoint and lp and ld and lp != ld else "FAIL"), {"primary": lp, "dr": ld}

    # ============================ C. FUNCTIONAL S3 ============================ #
    @T("C01", "functional", "S3 PUT/GET/HEAD/LIST/DELETE integrity (500 objects, 1 KB-2 MB)",
       "all objects round-trip with matching MD5")
    def _():
        c = S.s3(inv.s3s(P)[0])
        t0 = time.time()
        ds = put_dataset(c, "func/set1", 500, [1024, 16 * 1024, 256 * 1024, 2 * 1024 * 1024])
        put_s = time.time() - t0
        bad = verify_dataset(c, ds)
        keys, token = [], None
        while True:
            kw = dict(Bucket=BUCKET, Prefix="func/set1/", MaxKeys=1000)
            if token:
                kw["ContinuationToken"] = token
            r = c.list_objects_v2(**kw)
            keys += [o["Key"] for o in r.get("Contents", [])]
            token = r.get("NextContinuationToken")
            if not token:
                break
        c.head_object(Bucket=BUCKET, Key=next(iter(ds)))
        c.put_object(Bucket=BUCKET, Key="func/delete-me", Body=b"x")
        c.delete_object(Bucket=BUCKET, Key="func/delete-me")
        try:
            c.head_object(Bucket=BUCKET, Key="func/delete-me")
            deleted = False
        except ClientError:
            deleted = True
        S.ctx["ds1"] = ds
        ok = not bad and len(keys) == 500 and deleted
        return ("PASS" if ok else "FAIL"), {"objects": len(ds), "bad": bad[:5], "listed": len(keys),
                                            "delete_ok": deleted, "put_seconds": round(put_s, 1)}

    @T("C02", "functional", "Multipart upload 64 MB (large XML/EDI batch)", "ETag multipart, MD5 matches")
    def _():
        c = S.s3(inv.s3s(P)[1])
        body = os.urandom(64 * 1024 * 1024)
        from boto3.s3.transfer import TransferConfig
        c.upload_fileobj(io.BytesIO(body), BUCKET, "func/multipart.bin",
                         Config=TransferConfig(multipart_threshold=8 * 1024 * 1024, multipart_chunksize=8 * 1024 * 1024))
        h = c.head_object(Bucket=BUCKET, Key="func/multipart.bin")
        got = c.get_object(Bucket=BUCKET, Key="func/multipart.bin")["Body"].read()
        S.ctx["ds1"]["func/multipart.bin"] = md5(body)
        ok = md5(got) == md5(body) and "-" in h["ETag"]
        return ("PASS" if ok else "FAIL"), {"etag": h["ETag"], "size": h["ContentLength"]}

    @T("C03", "functional", "Read-your-write across gateways (different filers)",
       "object written via s3-01 is immediately readable via s3-02/s3-03")
    def _():
        gws = inv.s3s(P)
        lags = []
        for i in range(20):
            k = f"func/xgw/{i}"
            S.s3(gws[0]).put_object(Bucket=BUCKET, Key=k, Body=b"v" * 100)
            t0 = time.time()
            for g in gws[1:]:
                while True:
                    try:
                        S.s3(g).head_object(Bucket=BUCKET, Key=k)
                        break
                    except ClientError:
                        if time.time() - t0 > 10:
                            return "FAIL", f"{k} not visible via {g} after 10s"
                        time.sleep(0.05)
            lags.append(time.time() - t0)
        return "PASS", {"max_visibility_lag_ms": round(max(lags) * 1000), "p50_ms": round(statistics.median(lags) * 1000)}

    @T("C04", "functional", "Landing tier: new bucket data is on SSD with 010 (cross-rack) replicas",
       "all archive volumes ssd; each volume has 2 copies in 2 different racks")
    def _():
        vs = [v for v in volumes(inv, P) if v[4] == BUCKET and v[5] > 0]
        by = {}
        for node, rack, dt, vid, col, size in vs:
            by.setdefault(vid, []).append((rack, dt))
        wrong_disk = sorted({vid for vid, reps in by.items() if any(d != "ssd" for _, d in reps)})
        bad_rep = sorted(vid for vid, reps in by.items() if len(reps) != 2 or len({r for r, _ in reps}) != 2)
        ok = by and not wrong_disk and not bad_rep
        return ("PASS" if ok else "FAIL"), {"volumes_with_data": len(by), "not_ssd": wrong_disk, "bad_replication": bad_rep}

    # ============================ D. SECURITY ================================= #
    @T("D01", "security", "S3 IAM: anonymous / wrong secret / out-of-scope actions denied",
       "403 for anon, bad key, bucket create by app, write by monitor")
    def _():
        gw = inv.s3s(P)[0]
        res = {}
        try:
            urllib.request.urlopen(f"http://{gw}/{BUCKET}/func/xgw/0", timeout=3)
            res["anonymous_get"] = "ALLOWED"
        except urllib.error.HTTPError as e:
            res["anonymous_get"] = e.code
        for name, fn in {
            "bad_secret": lambda: S.s3(gw, secret="wrong").list_objects_v2(Bucket=BUCKET, MaxKeys=1),
            "app_create_bucket": lambda: S.s3(gw).create_bucket(Bucket="not-allowed"),
            "monitor_put": lambda: S.s3(gw, "monitor_probe", "SW_S3_MONITOR_SECRET").put_object(
                Bucket=BUCKET, Key="x", Body=b"x"),
        }.items():
            try:
                fn()
                res[name] = "ALLOWED"
            except ClientError as e:
                res[name] = e.response["ResponseMetadata"]["HTTPStatusCode"]
        S.s3(gw, "monitor_probe", "SW_S3_MONITOR_SECRET").list_objects_v2(Bucket=BUCKET, MaxKeys=1)
        res["monitor_list"] = "allowed (expected)"
        ok = all(v in (401, 403) for k, v in res.items() if k != "monitor_list")
        return ("PASS" if ok else "FAIL"), res

    @T("D02", "security", "gRPC mTLS enforced (weed shell without client cert)",
       "connection refused / times out without certs")
    def _():
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            try:
                r = subprocess.run([inv.g["weed_binary"], "shell", f"-master={inv.masters(P)[0]}"],
                                   input="cluster.ps\n", text=True, capture_output=True, cwd=td, timeout=15)
                ok = "volume servers" not in r.stdout
                return ("PASS" if ok else "FAIL"), (r.stdout + r.stderr)[-300:]
            except subprocess.TimeoutExpired:
                return "PASS", "shell could not establish a gRPC session within 15s (TLS handshake rejected)"

    @T("D03", "security", "Volume write without JWT rejected", "401 Unauthorized")
    def _():
        hot = [s for s in inv.services(P) if s.kind == "volume_hot"][0]
        req = urllib.request.Request(f"http://{hot.addr}/1,0000000001deadbeef", data=b"x", method="POST")
        try:
            urllib.request.urlopen(req, timeout=3)
            return "FAIL", "write accepted without JWT"
        except urllib.error.HTTPError as e:
            return ("PASS" if e.code == 401 else "FAIL"), f"HTTP {e.code}"

    @T("D04", "security", "Filer HTTP port bypasses S3 IAM (unauthenticated read)",
       "application network cannot read objects via filer :8888")
    def _():
        f = inv.filers(P)[0]
        code, body = sw.http_get(f"http://{f}/buckets/{BUCKET}/func/xgw/0")
        if code == 200:
            return "FINDING", ("filer HTTP returned object bytes without credentials. Firewall filer 8888/18888 "
                               "to S3 gateways + ops only, and/or set [jwt.filer_signing(.read)] keys")
        return "PASS", f"HTTP {code}"

    @T("D05", "security", "Master HTTP /dir/assign reachable without auth",
       "application network cannot obtain write tokens from masters")
    def _():
        code, body = sw.http_get(f"http://{inv.masters(P)[0]}/dir/assign")
        if code == 200 and b"fid" in body:
            return "FINDING", ("master issued fid+JWT to an anonymous caller - JWT only protects volume servers "
                               "if master HTTP 9333 is firewalled to cluster members; restrict with -whiteList "
                               "or [guard] white_list")
        return "PASS", f"HTTP {code}"

    # ============================ E. HIGH AVAILABILITY ======================= #
    def ha_case(name, cluster, kill_fn, restore_fn, observe=15, settle=5):
        pool = GatewayPool(S, cluster)
        load = Load(pool, f"ha/{name}").start()
        time.sleep(settle)
        t_fail = time.time()
        kill_fn()
        time.sleep(observe)
        during = load.summary(t_fail)
        t_rec = time.time()
        restore_fn()
        time.sleep(settle + 5)
        after = load.summary(time.time() - 5)
        load.stop()
        pool.close()
        bad_final = verify_dataset(S.s3(inv.s3s(cluster)[1]), dict(list(load.acked.items())))
        S.ctx.setdefault("ha_acked", {}).update(load.acked)
        return during, after, bad_final

    def gw_host(i, kind):
        return [s.host for s in inv.services(P) if s.kind == kind][i]

    @T("E01", "ha", "S3 gateway crash (s3-01) behind F5 pool", "application continues; errors only "
       "until monitor marks member down", destructive=True)
    def _():
        h = gw_host(0, "s3")
        d, a, bad = ha_case("s3", P, lambda: S.kill(P, h), lambda: S.start(P, h))
        ok = d["failed"] <= 3 and a["failed"] == 0 and not bad
        return ("PASS" if ok else "FAIL"), {"during": d, "after_restore": a, "lost_acked": len(bad)}

    @T("E02", "ha", "Filer crash (filer-01)", "S3 gateways fail over to filer-02/03", destructive=True)
    def _():
        h = gw_host(0, "filer")
        d, a, bad = ha_case("filer", P, lambda: S.kill(P, h), lambda: S.start(P, h))
        ok = d["error_window_s"] <= 10 and a["failed"] == 0 and not bad
        return ("PASS" if ok else "FAIL"), {"during": d, "after_restore": a, "lost_acked": len(bad)}

    @T("E03", "ha", "Master LEADER crash", "new leader elected; writes continue", destructive=True)
    def _():
        ld = leader(inv, P)
        host = [n["host"] for n in inv.nodes(P, "master") if ld and ld.startswith(n["ip"] + ":")][0]
        t = {}

        def kill():
            t["k"] = time.time()
            S.kill(P, host)
            while True:
                nl = leader(inv, P)
                if nl and nl != ld:
                    t["elected"] = time.time() - t["k"]
                    t["new"] = nl
                    break
                if time.time() - t["k"] > 60:
                    break
                time.sleep(0.2)
        d, a, bad = ha_case("master", P, kill, lambda: S.start(P, host), observe=10)
        ok = "elected" in t and a["failed"] == 0 and not bad
        return ("PASS" if ok else "FAIL"), {"old": ld, "new": t.get("new"),
                                            "election_s": round(t.get("elected", -1), 1),
                                            "during": d, "after_restore": a, "lost_acked": len(bad)}

    @T("E04", "ha", "Hot volume server crash (hot-01)", "reads served by cross-rack replica; "
       "writes re-routed", destructive=True)
    def _():
        h = gw_host(0, "volume_hot")
        before = verify_dataset(S.s3(inv.s3s(P)[1]), S.ctx.get("ds1", {}))
        res = {}

        def kill():
            S.kill(P, h)
            time.sleep(3)
            res["reads_with_hot01_down_bad"] = len(verify_dataset(S.s3(inv.s3s(P)[1]), S.ctx.get("ds1", {})))
        d, a, bad = ha_case("hot", P, kill, lambda: S.start(P, h), observe=20)
        ok = res["reads_with_hot01_down_bad"] == 0 and a["failed"] == 0 and not bad and not before
        return ("PASS" if ok else "FAIL"), {**res, "during": d, "after_restore": a, "lost_acked": len(bad)}

    @T("E05", "ha", "Full RACK r1 loss (master, filer, s3, hot, cold, admin)",
       "remaining racks keep S3 read/write for replicated (hot) data", destructive=True)
    def _():
        r1 = [n["host"] for n in inv.nodes(P) if n["rack"] == "r1"]
        res = {}

        def kill():
            for h in r1:
                S.kill(P, h)
            time.sleep(5)
            res["hot_reads_bad"] = len(verify_dataset(S.s3(inv.s3s(P)[1]), S.ctx.get("ds1", {})))
            res["leader_after"] = leader(inv, P)

        def restore():
            for h in r1:
                S.start(P, h)
        d, a, bad = ha_case("rack", P, kill, restore, observe=25)
        ok = res["hot_reads_bad"] == 0 and a["failed"] == 0 and not bad
        return ("PASS" if ok else "FAIL"), {**res, "hosts_killed": r1, "during": d, "after_restore": a,
                                            "lost_acked": len(bad)}

    @T("E06", "ha", "Rolling restart of every service (upgrade procedure) under load",
       "zero application-visible errors with one-at-a-time restarts + gates", destructive=True)
    def _():
        pool = GatewayPool(S, P)
        load = Load(pool, "ha/rolling").start()
        time.sleep(3)
        t0 = time.time()
        order = ["volume_cold", "volume_hot", "filer", "s3", "master"]
        for kind in order:
            for s in [x for x in inv.services(P) if x.kind == kind]:
                S.r.stop(s)          # graceful SIGTERM, like systemctl restart
                S.r.start(s)
                gate = {"master": lambda: sw.gate_master(inv, P, 60),
                        "filer": lambda: sw.gate_http(inv, P, "filer", 60),
                        "s3": lambda: sw.gate_http(inv, P, "s3", 60)}.get(kind, lambda: sw.gate_volume(inv, P, 60))
                gate()
                time.sleep(3)        # let heartbeats / F5 monitor converge before the next node
        dur = time.time() - t0
        s = load.summary(t0)
        load.stop()
        pool.close()
        bad = verify_dataset(S.s3(inv.s3s(P)[0]), load.acked)
        ok = s["failed"] == 0 and not bad
        return ("PASS" if ok else ("FINDING" if not bad else "FAIL")), {
            "restarted": sum(1 for x in inv.services(P) if x.kind in order), "minutes": round(dur / 60, 1),
            "load": s, "lost_acked": len(bad)}

    # ============================ F. TIERING / EC ============================= #
    @T("F01", "tiering", "Enterprise Volume Tiering policy (tiering.apply / status)",
       "policy validates and binds archive collection")
    def _():
        if inv.g["edition"] != "enterprise":
            return "SKIP", ("OSS build in lab - tiering.* commands are Enterprise-only. Rendered policy: "
                            f"{inv.build_dir}/primary/_cluster/tiering.json. Run F01 on the Enterprise staging cluster.")
        out = S.sh(P, f"tiering.apply -file={inv.build_dir}/primary/_cluster/tiering.json\ntiering.status -v")
        return ("PASS" if "error" not in out.lower() else "FAIL"), out[-800:]

    @T("F02", "tiering", "Hot(SSD, 010) -> Cold(HDD, EC 10+4) demotion after quiet period",
       "quiet SSD volumes become EC shards on HDD; objects still readable via S3", destructive=True)
    def _():
        c = S.s3(inv.s3s(P)[0])
        ds = put_dataset(c, "tier/old", 300, [64 * 1024, 512 * 1024])
        S.ctx["ds_tier"] = ds
        time.sleep(8)                                        # quiet period (lab: seconds, prod: 7776000s)
        # stop new writes landing in the volumes we are about to seal
        cmd = ("lock\nec.encode -collection=archive -quietFor=5s -fullPercent=0 "
               "-sourceDiskType=ssd -diskType=hdd -force\nunlock")
        t0 = time.time()
        out = S.sh(P, cmd, timeout=900)
        shards = ec_shards(S, P)
        attempts = 1
        while not shards and attempts < 4:          # lock contention with admin plugin workers
            time.sleep(15)
            out = S.sh(P, cmd, timeout=900)
            shards = ec_shards(S, P)
            attempts += 1
        lock_wait = "waiting for lock held by" in out
        ssd_left = [v for v in volumes(inv, P) if v[4] == BUCKET and v[2] == "ssd" and v[5] > 0]
        bad = verify_dataset(c, {**ds, **S.ctx.get("ds1", {})})
        S.ctx["ec_dist"] = shards
        pl = ec_placement(shards)
        S.ctx["ec_placement_after_encode"] = pl
        ok = shards and not bad
        ev = {"ec_volumes": len(shards), **pl, "ssd_volumes_with_data_after": len(ssd_left), "unreadable": bad[:5],
              "encode_attempts": attempts, "shell_lock_contention_with_admin": lock_wait,
              "encode_seconds": round(time.time() - t0, 1), "encode_tail": out.strip()[-240:]}
        if ok and (pl["shards_on_hot_nodes_pct"] > 0 or pl["worst_shards_on_one_node"] > inv.g["tiering"]["ec"]["parity_shards"]):
            return "FINDING", {**ev, "note": "objects readable, but shard placement is NOT on the cold tier / not "
                                             "failure-domain safe - always verify placement after demotion"}
        return ("PASS" if ok else "FAIL"), ev

    @T("F02b", "tiering", "Re-balance EC shards onto the cold (HDD) tier", "ec.balance -diskType=hdd moves "
       "all shards to cold nodes, max shards/node minimised", destructive=True)
    def _():
        out = S.sh(P, "lock\nec.balance -collection=archive -diskType=hdd -apply\nunlock", timeout=900)
        time.sleep(5)
        pl = ec_placement(ec_shards(S, P))
        S.ctx["ec_dist"] = ec_shards(S, P)
        bad = verify_dataset(S.s3(inv.s3s(P)[0]), S.ctx.get("ds_tier", {}))
        ok = pl["shards_on_hot_nodes_pct"] == 0 and not bad
        return ("PASS" if ok else "FINDING"), {**pl, "unreadable": len(bad), "balance_tail": out.strip()[-200:]}

    @T("F03", "tiering", "Cold node loss with EC 10+4 on 3 cold nodes (v1.0 'Test D')",
       "v1.0 expects: EC remains readable", destructive=True)
    def _():
        dist = S.ctx.get("ec_dist") or ec_shards(S, P)
        per_node = {}
        for vid, nodes in dist.items():
            for n, k in nodes.items():
                per_node[n] = max(per_node.get(n, 0), k)
        cold = [s for s in inv.services(P) if s.kind == "volume_cold"][0]
        S.kill(P, cold.host)
        time.sleep(6)
        c = S.s3(inv.s3s(P)[0], retries=1, timeout=3)
        ds = S.ctx.get("ds_tier", {})
        bad = verify_dataset(c, ds)
        S.start(P, cold.host)
        sw.gate_volume(inv, P, 60)
        time.sleep(5)
        bad_after = verify_dataset(c, ds)
        ev = {"max_shards_on_one_node": per_node, "parity": inv.g["tiering"]["ec"]["parity_shards"],
              "unreadable_while_down": f"{len(bad)}/{len(ds)}", "unreadable_after_restart": len(bad_after)}
        if bad and not bad_after:
            return "FINDING", ev   # platform correct, design expectation wrong
        return ("PASS" if not bad else "FAIL"), ev

    @T("F04", "tiering", "New writes after demotion land on SSD again", "fresh volumes on ssd")
    def _():
        c = S.s3(inv.s3s(P)[2])
        S.ctx["ds_new"] = put_dataset(c, "tier/new", 50, [128 * 1024])
        time.sleep(8)                                     # sizes are reported on the next heartbeat
        vs = [v for v in volumes(inv, P) if v[4] == BUCKET and v[5] > 0]
        return ("PASS" if any(v[2] == "ssd" for v in vs) else "FAIL"), {
            "ssd_volumes_with_data": len({v[3] for v in vs if v[2] == "ssd"}),
            "hdd_replicated_volumes": len({v[3] for v in vs if v[2] == "hdd"})}

    # ============================ G. DR REPLICATION =========================== #
    @T("G01", "dr", "DR seed: initial snapshot replicated (incl. EC'd cold data)",
       "every primary object present on DR with same MD5")
    def _():
        sync = [s for s in inv.services(D) if s.kind == "filer_sync"]
        if not sync:
            return "SKIP", "no filer_sync service (Enterprise: Data Movement policy)"
        # bootstrap was done in the run script; start replication now (deploy --phase sync)
        for s in sync:
            S.r.start(s)
        cp, cd = S.s3(inv.s3s(P)[0]), S.s3(inv.s3s(D)[0])
        t0 = time.time()
        last, stable = None, 0
        while time.time() - t0 < 300:               # wait until the DR object count stops moving
            r = rec.reconcile(None, None, BUCKET, src_client=cp, dst_client=cd)
            if r["missing_on_dst"] == 0:
                break
            stable = stable + 1 if last == r["dst_objects"] else 0
            last = r["dst_objects"]
            if stable >= 6:
                break
            time.sleep(5)
        seed_s = time.time() - t0
        before = r
        if before["missing_on_dst"] == 0:
            return "PASS", {"objects": before["src_objects"], "seed_seconds": round(seed_s, 1)}
        # OSS filer.sync replays the metadata LOG; it is not a snapshot. Repair with a tree copy and re-verify.
        fix = rec.reconcile(None, None, BUCKET, copy_missing=True, workers=16, src_client=cp, dst_client=cd)
        after = rec.reconcile(None, None, BUCKET, src_client=cp, dst_client=cd)
        return "FINDING", {"log_replay_result": {k: before[k] for k in ("src_objects", "dst_objects", "missing_on_dst")},
                           "silent_gap_pct": round(100 * before["missing_on_dst"] / max(1, before["src_objects"]), 1),
                           "repair_copied": fix["copied"], "after_repair_missing": after["missing_on_dst"],
                           "note": "seed via metadata-log replay silently missed objects (no error logged). "
                                   "A seed MUST be a snapshot/tree copy (Enterprise DM initial snapshot) and MUST "
                                   "be verified by listing+ETag diff before DR is declared ready"}

    @T("G02", "dr", "Continuous replication lag (PUT on primary -> visible on DR)",
       "lag well under RPO")
    def _():
        cps, cd = [S.s3(g) for g in inv.s3s(P)], S.s3(inv.s3s(D)[0])
        lags = []
        for i in range(30):
            k = f"dr/lag/{i}"
            body = os.urandom(32 * 1024)
            cps[i % len(cps)].put_object(Bucket=BUCKET, Key=k, Body=body)   # all gateways -> all filers
            t0 = time.time()
            while True:
                try:
                    if md5(cd.get_object(Bucket=BUCKET, Key=k)["Body"].read()) == md5(body):
                        break
                except ClientError:
                    pass
                if time.time() - t0 > 60:
                    return "FAIL", f"{k} not replicated in 60s"
                time.sleep(0.1)
            lags.append(time.time() - t0)
        rpo = inv.sla.get("rpo_seconds") or 900
        st = {"p50_s": round(statistics.median(lags), 2), "p95_s": round(sorted(lags)[int(.95 * len(lags)) - 1], 2),
              "max_s": round(max(lags), 2), "rpo_s": rpo}
        return ("PASS" if max(lags) < rpo else "FAIL"), st

    @T("G03", "dr", "Delete propagation (mirror semantics)", "delete on primary removes object on DR")
    def _():
        cp, cd = S.s3(inv.s3s(P)[0]), S.s3(inv.s3s(D)[0])
        cp.delete_object(Bucket=BUCKET, Key="dr/lag/0")
        t0 = time.time()
        while time.time() - t0 < 60:
            try:
                cd.head_object(Bucket=BUCKET, Key="dr/lag/0")
                time.sleep(0.2)
            except ClientError:
                return "PASS", {"propagation_s": round(time.time() - t0, 2),
                                "note": "a mirror is NOT a backup - keep a separate BACKUP policy with deletes OFF"}
        return "FAIL", "delete not propagated in 60s"

    @T("G06", "dr", "Replication source filer (filer-01) crash",
       "replication continues or catches up after restart without gaps", destructive=True)
    def _():
        src = [s for s in inv.services(P) if s.kind == "filer"][0]
        pool = GatewayPool(S, P)
        load = Load(pool, "dr/srcfail").start()
        time.sleep(3)
        S.kill(P, src.host)
        time.sleep(20)
        S.start(P, src.host)
        sw.gate_http(inv, P, "filer", 60)
        time.sleep(5)
        load.stop()
        pool.close()
        cd = S.s3(inv.s3s(D)[0])
        t0 = time.time()
        missing = list(load.acked)
        while time.time() - t0 < 90:
            missing = verify_dataset(cd, load.acked, workers=16)
            if not missing:
                break
            time.sleep(5)
        ev = {"acked": len(load.acked), "missing_on_dr_after_90s": len(missing),
              "catch_up_s": round(time.time() - t0, 1), "load": load.summary(0)}
        if missing:
            fix = rec.reconcile(None, None, BUCKET, prefix="dr/srcfail/", copy_missing=True,
                                src_client=S.s3(inv.s3s(P)[0]), dst_client=cd)
            ev["repaired_by_reconcile"] = fix["copied"]
            return "FINDING", {**ev, "note": "single-source replication stream lost events written while its "
                                              "source filer was down; replication source must be HA and gaps "
                                              "must be detected by periodic reconcile"}
        return "PASS", ev

    @T("G04", "dr", "Where do replicated objects land on DR?", "(design note) DR landing tier")
    def _():
        vs = [v for v in volumes(inv, D) if v[4] == BUCKET and v[5] > 0]
        ssd = {v[3] for v in vs if v[2] == "ssd"}
        hdd = {v[3] for v in vs if v[2] == "hdd"}
        return ("FINDING" if ssd else "PASS"), {
            "dr_ssd_volumes": len(ssd), "dr_hdd_volumes": len(hdd),
            "note": "seeded data (incl. data already EC'd on primary) re-lands on DR SSD and is 'new' for "
                    "quietForSeconds: DR SSD must absorb the whole seed, or seed with a DR-only short-quiet profile"}

    @T("G05", "dr", "Same S3 identities work on DR (failover without app re-config)",
       "archive-app can LIST/GET on DR")
    def _():
        cd = S.s3(inv.s3s(D)[1])
        r = cd.list_objects_v2(Bucket=BUCKET, Prefix="func/set1/", MaxKeys=5)
        return ("PASS" if r.get("KeyCount", 0) > 0 else "FAIL"), {"listed": r.get("KeyCount")}

    # ============================ H. DC FAILOVER / FAILBACK =================== #
    @T("H01", "failover", "Unplanned PRIMARY DC loss under load: measured RPO",
       "acknowledged writes missing on DR <= RPO window", destructive=True)
    def _():
        pool = GatewayPool(S, P)
        load = Load(pool, "dr/rpo", interval=0.02).start()
        time.sleep(15)
        t_kill = time.time()
        with cf.ThreadPoolExecutor(32) as ex:          # whole DC at once (power / network loss)
            list(ex.map(lambda s: S.r.stop(s, hard=True), inv.services(P)))
        S.ctx["kill_s"] = round(time.time() - t_kill, 2)
        load.stop()
        pool.close()
        S.ctx["t_failover"] = t_kill
        acked = {k: v for k, v in load.acked.items()}
        time.sleep(10)
        cd = S.s3(inv.s3s(D)[0])
        missing = verify_dataset(cd, acked, workers=16)
        # which acknowledged writes were lost, and how old were they at the time of the crash?
        lost_window = 0.0
        if missing:
            keys = [m.split(":")[0] for m in missing]
            lost_window = round(t_kill - min(load.acked_t[k] for k in keys), 2)
        late = sum(1 for t in load.acked_t.values() if t > t_kill)
        res = {"acked_puts": len(acked), "acked_after_kill_started": late, "kill_duration_s": S.ctx["kill_s"],
               "missing_on_dr": len(missing), "rpo_observed_s": lost_window,
               "rpo_target_s": inv.sla.get("rpo_seconds")}
        return ("PASS" if lost_window <= (inv.sla.get("rpo_seconds") or 900) else "FAIL"), res

    @T("H02", "failover", "Activate DR (GSLB -> DR VIP): LIST/GET/PUT on DR", "all four operations succeed",
       destructive=True)
    def _():
        t0 = S.ctx.get("t_failover", time.time())
        pool = GatewayPool(S, D)
        r, _ = pool.call("list_objects_v2", Bucket=BUCKET, Prefix="func/set1/", MaxKeys=10)
        k = next(iter(S.ctx["ds1"]))
        g, _ = pool.call("get_object", Bucket=BUCKET, Key=k)
        ok_get = md5(g["Body"].read()) == S.ctx["ds1"][k]
        body = os.urandom(4096)
        pool.call("put_object", Bucket=BUCKET, Key="dr-test/written-during-failover.xml", Body=body)
        ds = put_dataset(S.s3(inv.s3s(D)[2]), "dr-test/batch", 50, [8192])
        ds["dr-test/written-during-failover.xml"] = md5(body)
        S.ctx["dr_writes"] = ds
        l2, _ = pool.call("list_objects_v2", Bucket=BUCKET, Prefix="dr-test/")
        pool.close()
        rto = time.time() - t0
        ok = r.get("KeyCount", 0) > 0 and ok_get and l2.get("KeyCount", 0) == 51
        return ("PASS" if ok else "FAIL"), {"list": r.get("KeyCount"), "get_md5_ok": ok_get,
                                            "dr_put_list": l2.get("KeyCount"),
                                            "technical_rto_s": round(rto, 1),
                                            "note": "lab RTO excludes human decision + DNS TTL"}

    @T("H03", "failover", "Controlled failback: restore primary, replay DR writes, re-establish DR",
       "DR-era writes present on primary; primary->DR replication resumes", destructive=True)
    def _():
        sync = [s for s in inv.services(D) if s.kind == "filer_sync"]
        for s in sync:
            S.r.stop(s)
        # 1. bring primary back (it holds pre-failover data)
        for ph in sw.PHASES:
            for s in [x for x in inv.services(P) if x.phase == ph]:
                S.r.start(s)
            if ph in ("master", "volume", "filer", "s3"):
                sw.GATES[ph](inv, S.r, P)
        # 2. reverse sync DR -> primary only for changes since failover
        adm = [n for n in inv.nodes(D) if "filer_sync" in n["roles"]][0]
        cfg = Path(inv.p(D, adm["host"], inv.g["config_dir"]))
        since_ms = int((S.ctx.get("t_failover", time.time()) - 60) * 1000)
        rev = subprocess.Popen([inv.g["weed_binary"], "filer.sync", f"-a={inv.filers(D)[0]}",
                                f"-b={inv.filers(P)[0]}", "-a.path=/buckets/archive", "-b.path=/buckets/archive",
                                "-isActivePassive", f"-a.fromTsMs={since_ms}",
                                f"-a.security={cfg}/security.toml", f"-b.security={cfg}/sync-source-security.toml"],
                               cwd=cfg, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        cp = S.s3(inv.s3s(P)[0])
        ds = S.ctx.get("dr_writes", {})
        t0 = time.time()
        bad = list(ds)
        while time.time() - t0 < 90:
            bad = verify_dataset(cp, ds)
            if not bad:
                break
            time.sleep(3)
        replay_s = time.time() - t0
        rev.terminate()
        rev.wait(20)
        repaired = None
        if bad:
            fix = rec.reconcile(None, None, BUCKET, prefix="dr-test/", copy_missing=True,
                                src_client=S.s3(inv.s3s(D)[0]), dst_client=cp)
            repaired = {"replayed_by_log": len(ds) - len(bad), "copied_by_reconcile": fix["copied"]}
            bad = verify_dataset(cp, ds)
        # 3. re-establish primary -> DR
        for s in sync:
            S.r.start(s)
        k = "dr/after-failback.xml"
        cp.put_object(Bucket=BUCKET, Key=k, Body=b"ok")
        cd = S.s3(inv.s3s(D)[0])
        t1 = time.time()
        resumed = False
        while time.time() - t1 < 60:
            try:
                cd.head_object(Bucket=BUCKET, Key=k)
                resumed = True
                break
            except ClientError:
                time.sleep(0.3)
        ok = not bad and resumed
        ev = {"dr_writes_on_primary": f"{len(ds) - len(bad)}/{len(ds)}", "replay_s": round(replay_s, 1),
              "forward_replication_resumed": resumed, "repair": repaired}
        if ok and repaired:
            return "FINDING", {**ev, "note": "reverse log replay was incomplete; reconcile (tree copy) closed the gap"}
        return ("PASS" if ok else "FAIL"), ev

    @T("H04", "failover", "Post-failback full consistency check primary vs DR", "no divergence")
    def _():
        cp, cd = S.s3(inv.s3s(P)[0]), S.s3(inv.s3s(D)[0])

        def listing(c):
            out, token = {}, None
            while True:
                kw = dict(Bucket=BUCKET, MaxKeys=1000)
                if token:
                    kw["ContinuationToken"] = token
                r = c.list_objects_v2(**kw)
                for o in r.get("Contents", []):
                    out[o["Key"]] = (o["Size"], o["ETag"])
                token = r.get("NextContinuationToken")
                if not token:
                    return out
        time.sleep(10)
        a, b = listing(cp), listing(cd)
        only_p = sorted(set(a) - set(b))
        only_d = sorted(set(b) - set(a))
        diff = sorted(k for k in set(a) & set(b) if a[k][0] != b[k][0])
        ok = not only_p and not only_d and not diff
        ev = {"primary": len(a), "dr": len(b), "only_primary": only_p[:5], "only_dr": only_d[:5],
              "size_mismatch": diff[:5], "only_primary_n": len(only_p), "only_dr_n": len(only_d)}
        if not ok and all(k.startswith("dr/rpo/") for k in only_p + only_d):
            return "FINDING", {**ev, "note": "divergence limited to writes acknowledged by primary but not yet "
                                              "replicated at crash time (the RPO window). They exist only on the "
                                              "recovered primary; reconcile before re-enabling DR mirror deletes."}
        return ("PASS" if ok else "FAIL"), ev

    return tests


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--inventory", required=True)
    ap.add_argument("--out", default=str(ROOT / "reports"))
    ap.add_argument("--destructive", action="store_true")
    ap.add_argument("--only", help="comma list of test id prefixes")
    a = ap.parse_args()
    inv = sw.Inventory(a.inventory)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    S = Suite(inv, out, a.destructive)
    t0 = time.time()
    for t in build(S):
        if a.only and not any(t.tid.startswith(p) for p in a.only.split(",")):
            continue
        t()
    summ = {k: sum(1 for r in S.results if r["status"] == k) for k in ("PASS", "FAIL", "FINDING", "SKIP")}
    rep = {"inventory": str(inv.path), "edition": inv.g["edition"], "weed_version": subprocess.run(
        [inv.g["weed_binary"], "version"], capture_output=True, text=True).stdout.splitlines()[0],
        "started": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t0)),
        "minutes": round((time.time() - t0) / 60, 1), "summary": summ, "results": S.results}
    (out / "e2e-results.json").write_text(json.dumps(rep, indent=2, default=str))
    print(f"\n==== {summ}  ({rep['minutes']} min)  -> {out / 'e2e-results.json'}")
    return 1 if summ["FAIL"] else 0


if __name__ == "__main__":
    sys.exit(main())
