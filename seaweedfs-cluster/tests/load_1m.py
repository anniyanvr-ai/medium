#!/usr/bin/env python3
"""
load_1m.py - sustained load at the "1 million files per day" design point, with DR lag measured
while the load runs.

  1,000,000 files/day = 11.6 PUT/s on average; the test drives the PEAK rate (default 10x = 116/s)
  plus the same number of GET/s (downloads of already-written objects), for --duration seconds.

Measures: PUT/GET latency p50/p95/p99/max, error counts, achieved rates, Data Movement lag
(a canary every --canary-every s during the load) and DR completeness of the window afterwards
(s3-reconcile --since, HEAD mode - the production way to verify a million-object day).

Keys are date-partitioned (ingest/YYYY/MM/DD/HH/<uuid>) as recommended for 1M+/day.
"""
import argparse
import concurrent.futures as cf
import json
import os
import random
import statistics
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[1]


def s3(ep, pool=128):
    return boto3.client("s3", endpoint_url=ep, region_name="us-east-1",
                        config=Config(s3={"addressing_style": "path"}, max_pool_connections=pool,
                                      connect_timeout=3, read_timeout=30, retries={"max_attempts": 3}))


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(len(xs) * p))] * 1000, 1)


def size_sample(avg_kb):
    # log-normal around the average, clipped 1 KB .. 8 MB
    s = random.lognormvariate(0, 0.9) * avg_kb * 1024 / 1.5
    return int(min(max(s, 1024), 8 * 1024 * 1024))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", nargs="+", default=["http://127.10.5.11:8333", "http://127.10.5.12:8333",
                                                  "http://127.10.5.13:8333"])
    ap.add_argument("--dst", default="http://127.20.5.11:8333")
    ap.add_argument("--bucket", default="archive")
    ap.add_argument("--daily-files", type=float, default=1_000_000)
    ap.add_argument("--peak", type=float, default=10)
    ap.add_argument("--avg-kb", type=float, default=64)
    ap.add_argument("--duration", type=float, default=300)
    ap.add_argument("--threads", type=int, default=64)
    ap.add_argument("--canary-every", type=float, default=20)
    ap.add_argument("--out", default="reports/enterprise/remediation/load-1m.json")
    a = ap.parse_args()

    rate = a.daily_files / 86400 * a.peak
    clients = [s3(e) for e in a.src]
    lat = {"put": [], "get": []}
    err = {"put": 0, "get": 0}
    written, lock, stop = [], threading.Lock(), threading.Event()
    t_start = time.time()
    run = uuid.uuid4().hex[:6]

    def put(i):
        k = time.strftime("ingest/%Y/%m/%d/%H/", time.gmtime()) + f"{run}-{i:08d}"
        body = os.urandom(size_sample(a.avg_kb))
        t0 = time.time()
        try:
            clients[i % len(clients)].put_object(Bucket=a.bucket, Key=k, Body=body)
            with lock:
                lat["put"].append(time.time() - t0)
                written.append((k, len(body)))
        except Exception:  # noqa: BLE001
            with lock:
                err["put"] += 1

    def get(i):
        with lock:
            if not written:
                return
            k, n = random.choice(written)
        t0 = time.time()
        try:
            b = clients[(i + 1) % len(clients)].get_object(Bucket=a.bucket, Key=k)["Body"].read()
            ok = len(b) == n
            with lock:
                lat["get"].append(time.time() - t0)
                err["get"] += 0 if ok else 1
        except Exception:  # noqa: BLE001
            with lock:
                err["get"] += 1

    canaries = []

    def canary_loop():
        while not stop.is_set():
            r = subprocess.run([str(ROOT / "bin/sw-canary"), "--src", a.src[0], "--dst", a.dst, "--bucket",
                                a.bucket, "--timeout", "60", "--json"], capture_output=True, text=True)
            try:
                canaries.append(json.loads(r.stdout.strip().splitlines()[-1])["lag_s"])
            except Exception:  # noqa: BLE001
                canaries.append(None)
            stop.wait(a.canary_every)

    ct = threading.Thread(target=canary_loop, daemon=True)
    ct.start()
    print(f"driving {rate:.0f} PUT/s + {rate:.0f} GET/s for {a.duration:.0f}s "
          f"({a.daily_files:,.0f} files/day x{a.peak} peak, avg {a.avg_kb} KB)", flush=True)
    slots = threading.BoundedSemaphore(a.threads * 2)     # back-pressure: a stalled cluster must not queue 30k requests
    shed = [0]

    def guarded(fn, i):
        try:
            fn(i)
        finally:
            slots.release()

    with cf.ThreadPoolExecutor(a.threads) as ex:
        i, nxt = 0, time.time()
        while time.time() - t_start < a.duration:
            for fn in (put, get):
                if slots.acquire(blocking=False):
                    ex.submit(guarded, fn, i)
                else:
                    shed[0] += 1
            i += 1
            nxt += 1 / rate
            d = nxt - time.time()
            if d > 0:
                time.sleep(d)
            if i % int(rate * 30) == 0:
                print(f"  t+{time.time() - t_start:.0f}s puts={len(lat['put'])} p99={pct(lat['put'], .99)}ms "
                      f"errs={err} last canary lag={canaries[-1] if canaries else '-'}s", flush=True)
    t_end = time.time()
    stop.set()
    ct.join(timeout=90)
    elapsed = t_end - t_start
    # DR completeness of the window, the production way (HEAD per source key of the window)
    time.sleep(30)
    r = subprocess.run([str(ROOT / "bin/s3-reconcile"), "--src", a.src[0], "--dst", a.dst, "--bucket", a.bucket,
                        "--prefix", "ingest/", "--since", str(int(t_start - 5)), "--head", "--workers", "32"],
                       capture_output=True, text=True)
    try:
        rec = json.loads(r.stdout[r.stdout.index("{"):])
    except Exception:  # noqa: BLE001
        rec = {"raw": r.stdout[-300:] + r.stderr[-300:]}
    lags = [c for c in canaries if c is not None]
    res = {
        "target": {"daily_files": a.daily_files, "peak_factor": a.peak, "put_per_s": round(rate, 1),
                   "get_per_s": round(rate, 1), "avg_kb": a.avg_kb, "duration_s": a.duration},
        "achieved": {"put_per_s": round(len(lat["put"]) / elapsed, 1), "get_per_s": round(len(lat["get"]) / elapsed, 1),
                     "objects": len(written), "gb": round(sum(n for _, n in written) / 1e9, 3),
                     "equiv_files_per_day": int(len(lat["put"]) / elapsed * 86400)},
        "put_ms": {"p50": pct(lat["put"], .5), "p95": pct(lat["put"], .95), "p99": pct(lat["put"], .99),
                   "max": pct(lat["put"], 1.0)},
        "get_ms": {"p50": pct(lat["get"], .5), "p95": pct(lat["get"], .95), "p99": pct(lat["get"], .99),
                   "max": pct(lat["get"], 1.0)},
        "errors": err,
        "shed_requests": shed[0],
        "dr_lag_s": {"samples": len(canaries), "failed": canaries.count(None),
                     "p50": statistics.median(lags) if lags else None, "max": max(lags) if lags else None},
        "dr_window_check": {k: rec.get(k) for k in ("src_objects", "dst_objects", "missing_on_dst",
                                                     "size_mismatch", "elapsed_s", "mode")} if "raw" not in rec else rec,
    }
    print(json.dumps(res, indent=1))
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(json.dumps(res, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
