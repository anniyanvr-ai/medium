#!/usr/bin/env python3
"""
dm_selfheal.py - lab experiment for finding E-08 (Data Movement stops after all workers die).

Writes a steady load through the Primary S3 endpoint, SIGKILLs every Primary worker, restarts
them after --down seconds, and measures:
  * recovery_s      time from worker restart until a canary object reaches DR
  * missing_before  acknowledged writes absent on DR once replication resumed (before reconcile)
  * missing_after   same, after `s3-reconcile --copy-missing` on the outage window (if --reconcile)

  --tune    first applies the production timeouts proposed in the v2.2 design (slice 600 s,
            execution timeout 900 s) so an orphaned job is reaped by the admin within 15 min.
  --guard   instead of waiting, runs `sw-guard --fix` and lets it heal the stall.
"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bin"))
from seaweed_admin_api import AdminAPI  # noqa: E402

TUNE_ADMIN = {"execution_timeout_seconds": 900, "job_type_max_runtime_seconds": 900}
TUNE_WORKER = {"slice_max_runtime_seconds": {"int64_value": "600"}}


def s3(ep):
    return boto3.client("s3", endpoint_url=ep, region_name="us-east-1",
                        config=Config(s3={"addressing_style": "path"}, connect_timeout=3, read_timeout=10,
                                      retries={"max_attempts": 2}))


def sh(*args, timeout=300):
    r = subprocess.run(list(args), cwd=ROOT, capture_output=True, text=True, timeout=timeout)
    return r.returncode, (r.stdout + r.stderr)[-2000:]


def canary(a, timeout=30):
    rc, out = sh(str(ROOT / "bin/sw-canary"), "--src", a.src, "--dst", a.dst, "--bucket", a.bucket,
                 "--timeout", str(timeout))
    return rc == 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--inventory", default="inventory/lab-ent.yaml")
    ap.add_argument("--admin", default="http://127.10.6.11:23646")
    ap.add_argument("--src", default="http://127.10.5.11:8333")
    ap.add_argument("--dst", default="http://127.20.5.11:8333")
    ap.add_argument("--bucket", default="archive")
    ap.add_argument("--rate", type=float, default=5.0)
    ap.add_argument("--down", type=float, default=20)
    ap.add_argument("--max-wait", type=float, default=1500)
    ap.add_argument("--tune", action="store_true")
    ap.add_argument("--guard", action="store_true")
    ap.add_argument("--reconcile", action="store_true")
    ap.add_argument("--out", default="reports/enterprise/remediation/selfheal.json")
    a = ap.parse_args()
    sw = [str(ROOT / "bin/swctl"), "-i", a.inventory]
    adm = AdminAPI(a.admin)
    res = {"tune": a.tune, "guard": a.guard, "rate": a.rate, "down_s": a.down}

    if a.tune:
        adm.enable_job("data_movement", **TUNE_ADMIN)
        adm.set_values("data_movement", worker=TUNE_WORKER)
        res["tuning"] = {"admin": TUNE_ADMIN, "worker": TUNE_WORKER}
        print("tuned data_movement timeouts", flush=True)

    assert canary(a, 60), "replication not healthy before the test"
    run = time.strftime("%H%M%S")
    prefix = f"selfheal/{run}/"
    src = s3(a.src)
    acked, stop = [], threading.Event()

    def writer():
        n = 0
        while not stop.is_set():
            k = f"{prefix}{n:06d}"
            try:
                src.put_object(Bucket=a.bucket, Key=k, Body=os.urandom(2048))
                acked.append((k, time.time()))
            except Exception:  # noqa: BLE001
                pass
            n += 1
            time.sleep(1 / a.rate)

    th = threading.Thread(target=writer, daemon=True)
    th.start()
    time.sleep(10)
    t_kill = time.time()
    print(sh(*sw, "stop", "--cluster", "primary", "--kind", "worker", "--kill")[1][-300:], flush=True)
    time.sleep(a.down)
    print(sh(*sw, "start", "--cluster", "primary", "--kind", "worker")[1][-300:], flush=True)
    t_up = time.time()

    healed_by = "self"
    if a.guard:
        rc, out = sh(str(ROOT / "bin/sw-guard"), "-i", a.inventory, "--cluster", "primary", "--fix",
                     "--since", str(int(t_kill - 30)), timeout=a.max_wait)
        res["guard_rc"], res["guard_log"] = rc, out
        healed_by = "sw-guard"
    recovered = None
    while time.time() - t_up < a.max_wait:
        if canary(a, 30):
            recovered = time.time()
            break
        print(f"  t+{time.time() - t_up:.0f}s still stalled", flush=True)
        time.sleep(15)
    res["recovery_s"] = round(recovered - t_up, 1) if recovered else None
    res["healed_by"] = healed_by if recovered else "not healed"
    time.sleep(30)
    stop.set()
    th.join()
    time.sleep(30)

    def missing():
        dst = s3(a.dst)
        have, tok = set(), None
        while True:
            kw = dict(Bucket=a.bucket, Prefix=prefix, MaxKeys=1000)
            if tok:
                kw["ContinuationToken"] = tok
            r = dst.list_objects_v2(**kw)
            have |= {o["Key"] for o in r.get("Contents", [])}
            tok = r.get("NextContinuationToken")
            if not tok:
                return [k for k, _ in acked if k not in have]

    miss = missing()
    res.update(acked=len(acked), missing_before=len(miss),
               missing_window=[round(min(t for k, t in acked if k in set(miss)) - t_kill, 1),
                               round(max(t for k, t in acked if k in set(miss)) - t_kill, 1)] if miss else None)
    if a.reconcile and miss:
        rc, out = sh(str(ROOT / "bin/s3-reconcile"), "--src", a.src, "--dst", a.dst, "--bucket", a.bucket,
                     "--prefix", prefix, "--copy-missing")
        res["reconcile_log"] = out[-600:]
        res["missing_after"] = len(missing())
    print(json.dumps({k: v for k, v in res.items() if not k.endswith("_log")}, indent=1))
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(json.dumps(res, indent=1))
    return 0 if recovered else 1


if __name__ == "__main__":
    sys.exit(main())
