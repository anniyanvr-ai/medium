#!/usr/bin/env python3
"""Integration scenario runner: 3 filter modes x 2 destinations, each with a
hard kill + resume, independent 100 % verification, peak-memory sampling and
dashboard screenshots. Produces integration/results/<run>/REPORT.md.

Two source modes
  --source emulator   in-process S3 emulator (moto) as MinIO; objects are
                      back-dated to real 2025 dates, so the windows are the
                      literal "since 2025-06-01" and "2025-06-01..2025-08-31".
  --source real       a real MinIO (e.g. http://localhost:9000). MinIO sets
                      LastModified itself, so data is uploaded in three time
                      batches (before / inside / after) and the windows are
                      the instants between the batches.

Examples
  # cloud / CI (needs: pip install -r requirements-dev.txt, SeaweedFS on :8333)
  python integration/run_scenarios.py --source emulator --seaweed http://127.0.0.1:8333

  # your machine: real MinIO + real SeaweedFS
  python integration/run_scenarios.py --source real \
      --minio http://localhost:9000 --minio-key minioadmin --minio-secret minioadmin \
      --seaweed http://localhost:8333 --seaweed-key any --seaweed-secret any
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))
os.environ["NO_PROXY"] = os.environ["no_proxy"] = "127.0.0.1,localhost," + os.environ.get("NO_PROXY", "")


from s3mover.config import Endpoint  # noqa: E402
from s3mover.dashboard import serve  # noqa: E402
from s3mover.s3client import make_client  # noqa: E402

try:
    import psutil
except ImportError:  # pragma: no cover
    psutil = None

UTC = timezone.utc
MB = 1024 * 1024


def log(msg: str) -> None:
    print(f"{datetime.now():%H:%M:%S} {msg}", flush=True)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ------------------------------------------------------------------ data set
def body_for(rnd: random.Random, size: int) -> bytes:
    return rnd.randbytes(size) if hasattr(rnd, "randbytes") else os.urandom(size)


def plan_objects(n_small: int, n_large: int, large_mb: int, seed: int = 7):
    """[(key, size, group)] - group = 0 before window, 1 inside, 2 after."""
    rnd = random.Random(seed)
    objs = []
    for i in range(n_small):
        g = (0, 1, 1, 2)[i % 4]  # half of the objects inside the window
        area = ("epcis", "epcis", "reports", "aggregation")[i % 4]
        size = rnd.choice([0, 1, 512, 2048, 8192, 32768, 65536]) if i % 97 else 0
        objs.append((f"{area}/{['old', 'win', 'new'][g]}/{i // 250:03d}/doc-{i:06d}.xml", size, g))
    objs += [("root-level-before.xml", 100, 0), ("root-level-inside.xml", 100, 1), ("root-level-after.xml", 100, 2),
             ("a-c.xml", 10, 1), ("a/b.xml", 10, 1), ("with space/ünïcødé é.xml", 10, 1)]
    for j in range(n_large):
        objs.append((f"large/blob-{j}.bin", large_mb * MB + 4321 * j, (1, 1, 0, 2)[j % 4]))
    return objs


class EmulatorSource:
    """moto S3 server in this process (so LastModified can be back-dated)."""

    DATES = {0: [datetime(2025, 1, 10, tzinfo=UTC), datetime(2025, 5, 31, 23, 59, 59, tzinfo=UTC)],
             1: [datetime(2025, 6, 1, 0, 0, 0, tzinfo=UTC), datetime(2025, 7, 15, tzinfo=UTC),
                 datetime(2025, 8, 31, 23, 59, 59, tzinfo=UTC)],
             2: [datetime(2025, 9, 1, 0, 0, 0, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC)]}

    def __init__(self):
        from moto.server import ThreadedMotoServer
        self.port = free_port()
        self.srv = ThreadedMotoServer(ip_address="127.0.0.1", port=self.port, verbose=False)
        self.srv.start()
        self.endpoint = f"http://127.0.0.1:{self.port}"
        self.key = self.secret = "minioadmin"
        self.kind = "S3 emulator (moto) standing in for MinIO"

    def backend(self):
        from moto.s3.models import s3_backends
        return s3_backends[list(s3_backends)[0]]["global"]

    def seed(self, client, bucket: str, objs) -> dict:
        client.create_bucket(Bucket=bucket)
        rnd = random.Random(1)
        manifest = {}
        be = self.backend()
        for i, (key, size, g) in enumerate(objs):
            data = body_for(rnd, size)
            client.put_object(Bucket=bucket, Key=key, Body=data)
            when = self.DATES[g][i % len(self.DATES[g])]
            be.buckets[bucket].keys[key].last_modified = when.replace(tzinfo=None)
            manifest[key] = {"size": size, "md5": hashlib.md5(data).hexdigest(), "group": g}
        windows = {"since": ("2025-06-01", None), "between": ("2025-06-01", "2025-08-31")}
        return manifest, windows


class RealSource:
    def __init__(self, endpoint, key, secret, batch_gap: float):
        self.endpoint, self.key, self.secret = endpoint, key, secret
        self.gap = batch_gap
        self.kind = f"real MinIO at {endpoint}"

    def seed(self, client, bucket: str, objs) -> dict:
        try:
            client.create_bucket(Bucket=bucket)
        except Exception:
            pass
        manifest = {}
        bounds = []
        for g in (0, 1, 2):
            if g:
                time.sleep(self.gap)
                bounds.append(datetime.now(UTC))
                time.sleep(self.gap)
            batch = [o for o in objs if o[2] == g]

            def put(o):
                key, size, _ = o
                data = body_for(random.Random(key), size)
                client.put_object(Bucket=bucket, Key=key, Body=data)
                return key, {"size": size, "md5": hashlib.md5(data).hexdigest(), "group": g}

            with ThreadPoolExecutor(16) as ex:
                manifest.update(dict(ex.map(put, batch)))
            log(f"  seeded batch {g}: {len(batch)} objects")
        # sanity: server timestamps really fall into the batches
        t1, t2 = bounds
        windows = {"since": (t1.isoformat(), None),
                   "between": (t1.isoformat(), (t2 - timedelta(microseconds=1)).isoformat())}
        return manifest, windows


def expected(manifest: dict, mode: str) -> dict:
    if mode == "full":
        return manifest
    if mode == "since":
        return {k: v for k, v in manifest.items() if v["group"] >= 1}
    return {k: v for k, v in manifest.items() if v["group"] == 1}


# ------------------------------------------------------------------ verification (independent of s3mover)
def independent_verify(client, bucket: str, exp: dict, threads: int = 16) -> dict:
    got = {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for o in page.get("Contents", []) or []:
            got[o["Key"]] = o["Size"]
    missing = sorted(set(exp) - set(got))
    unexpected = sorted(set(got) - set(exp))
    size_bad = [k for k in exp if k in got and got[k] != exp[k]["size"]]

    def check(k):
        data = client.get_object(Bucket=bucket, Key=k)["Body"].read()
        return k if hashlib.md5(data).hexdigest() != exp[k]["md5"] else None

    with ThreadPoolExecutor(threads) as ex:
        md5_bad = [k for k in ex.map(check, [k for k in exp if k in got]) if k]
    ok = len(exp) - len(missing) - len(set(size_bad) | set(md5_bad))
    return {"expected": len(exp), "present": len(got), "ok_md5": ok, "missing": len(missing),
            "unexpected": len(unexpected), "size_mismatch": len(size_bad), "md5_mismatch": len(md5_bad),
            "percent": round(100.0 * ok / len(exp), 4) if exp else 100.0,
            "examples": (missing + unexpected + size_bad + md5_bad)[:10]}


# ------------------------------------------------------------------ memory sampling
class RssSampler(threading.Thread):
    """Peak RSS of the mover process and of local MinIO / SeaweedFS server processes."""

    def __init__(self):
        super().__init__(daemon=True)
        self.pid = None
        self.peak = {"s3mover": 0, "seaweedfs": 0, "minio": 0}
        self.stop = threading.Event()

    def run(self):
        if not psutil:
            return
        while not self.stop.wait(0.5):
            try:
                if self.pid:
                    p = psutil.Process(self.pid)
                    rss = p.memory_info().rss + sum(c.memory_info().rss for c in p.children(recursive=True))
                    self.peak["s3mover"] = max(self.peak["s3mover"], rss)
            except Exception:
                pass
            sw = mi = 0
            for proc in psutil.process_iter(["name"]):
                try:
                    n = (proc.info["name"] or "").lower()
                    if n.startswith("weed"):
                        sw += proc.memory_info().rss
                    elif n.startswith("minio"):
                        mi += proc.memory_info().rss
                except Exception:
                    pass
            self.peak["seaweedfs"] = max(self.peak["seaweedfs"], sw)
            self.peak["minio"] = max(self.peak["minio"], mi)


# ------------------------------------------------------------------ screenshots
class Shooter:
    def __init__(self, out: Path, port: int, enabled: bool):
        self.out, self.port = out, port
        self.page = None
        if not enabled:
            return
        try:
            from playwright.sync_api import sync_playwright
            self._pw = sync_playwright().start()
            self._br = None
            errors = []
            # bundled Chromium first, then the Chrome / Edge already installed on the machine
            for kw in ({}, {"channel": "chrome"}, {"channel": "msedge"}):
                try:
                    self._br = self._pw.chromium.launch(**kw)
                    break
                except Exception as exc:
                    errors.append(str(exc).splitlines()[0])
            if self._br is None:
                raise RuntimeError("; ".join(errors))
            self.page = self._br.new_page(viewport={"width": 1360, "height": 1100}, device_scale_factor=1)
        except Exception as exc:
            log(f"screenshots disabled ({exc}); run `python -m playwright install chromium`")

    def shot(self, job: str, name: str, theme: str = "light") -> str | None:
        if not self.page:
            return None
        try:
            self.page.goto(f"http://127.0.0.1:{self.port}/?job={job}", wait_until="networkidle")
            self.page.evaluate(f"document.documentElement.dataset.theme='{theme}'")
            self.page.wait_for_timeout(2600)  # two polling cycles
            p = self.out / f"{name}.png"
            self.page.screenshot(path=str(p), full_page=True)
            return p.name
        except Exception as exc:
            log(f"screenshot failed: {exc}")
            return None

    def close(self):
        try:
            if self.page:
                self._br.close()
                self._pw.stop()
        except Exception:
            pass


# ------------------------------------------------------------------ one scenario
def run_scenario(a, src, dst: dict, mode: str, windows: dict, manifest: dict, src_bucket: str,
                 state_dir: Path, out: Path, shooter: Shooter, idx: int) -> dict:
    job = f"{dst['name']}-{idx}-{mode}"
    dst_bucket = f"{a.prefix}-{dst['name']}-{mode}-{a.run_id}"[:63].lower()
    exp = expected(manifest, mode)
    flt = {"mode": mode, "timezone": "UTC"}
    if mode != "full":
        flt["from"], to = windows[mode]
        if to:
            flt["to"] = to
    cfg = {
        "job": {"name": job, "state_dir": str(state_dir)},
        "source": {"endpoint": src.endpoint, "access_key": src.key, "secret_key": src.secret, "type": "minio"},
        "destination": {"endpoint": dst["endpoint"], "access_key": dst["key"], "secret_key": dst["secret"],
                        "type": dst["type"]},
        "buckets": [{"source": src_bucket, "destination": dst_bucket}],
        "filter": flt,
        "transfer": {"action": "copy", "workers": a.workers, "multipart_threshold_mb": 16, "part_size_mb": 8,
                     "max_inflight_mb": 128},
        "throttle": {"source_rps": a.rps, "dest_rps": a.rps * 2},
        "dashboard": {"enabled": False},
    }
    cfg_path = out / f"{job}.yaml"
    cfg_path.write_text(json.dumps(cfg, indent=2))  # JSON is valid YAML; no PyYAML needed
    cmd = [sys.executable, "-m", "s3mover", "run", "-c", str(cfg_path), "--no-dashboard"]
    dcl = make_client(Endpoint(dst["endpoint"], dst["key"], dst["secret"]), 32)
    shots = []
    log(f"[{job}] expecting {len(exp)} objects -> {dst['name']}:{dst_bucket}")

    def count_dest():
        n = 0
        try:
            for page in dcl.get_paginator("list_objects_v2").paginate(Bucket=dst_bucket):
                n += len(page.get("Contents", []) or [])
        except Exception:
            pass
        return n

    sampler = RssSampler()
    sampler.start()
    t0 = time.time()
    logf = open(out / f"{job}.run1.log", "w")
    p = subprocess.Popen(cmd, cwd=ROOT, stdout=logf, stderr=subprocess.STDOUT)
    sampler.pid = p.pid
    kill_at = max(1, int(len(exp) * a.kill_at))
    while p.poll() is None and count_dest() < kill_at:
        time.sleep(0.4)
    killed_at = count_dest()
    if p.poll() is None:
        s = shooter.shot(job, f"{job}-1-running-before-kill")
        shots += [s] if s else []
        p.kill()  # SIGKILL / TerminateProcess: no cleanup, no state flush
        p.wait()
        log(f"[{job}] hard-killed at {killed_at}/{len(exp)} objects at destination")
        time.sleep(7)  # dashboard marks a job "interrupted" once progress is >6 s stale
        s = shooter.shot(job, f"{job}-2-after-kill-interrupted")
        shots += [s] if s else []
    logf.close()

    logf = open(out / f"{job}.run2.log", "w")
    p = subprocess.Popen(cmd, cwd=ROOT, stdout=logf, stderr=subprocess.STDOUT)
    sampler.pid = p.pid
    mid_done = False
    while p.poll() is None:
        if not mid_done and count_dest() >= int(len(exp) * 0.7):
            s = shooter.shot(job, f"{job}-3-resumed-in-progress")
            shots += [s] if s else []
            mid_done = True
        time.sleep(0.5)
    rc = p.returncode
    logf.close()
    elapsed = time.time() - t0
    s = shooter.shot(job, f"{job}-4-complete")
    shots += [s] if s else []
    sampler.stop.set()

    status = json.loads(subprocess.run([sys.executable, "-m", "s3mover", "status", "-c", str(cfg_path)],
                                       cwd=ROOT, capture_output=True, text=True).stdout or "{}")
    prog = status.get("progress", {})
    log(f"[{job}] resume exit={rc}; verifying independently (list + MD5 of every object)")
    ind = independent_verify(dcl, dst_bucket, exp)
    res = {
        "job": job, "destination": dst["name"], "dest_type": dst["type"], "mode": mode,
        "window": prog.get("window"), "expected": len(exp), "killed_at_dest_count": killed_at,
        "resume_exit_code": rc, "status": prog.get("status"), "copied": prog.get("copied"),
        "skipped_existing": prog.get("skipped_existing"), "failed": prog.get("failed"),
        "tool_verify": prog.get("verify"), "independent_verify": ind, "elapsed_s": round(elapsed, 1),
        "peak_rss_mb": {k: round(v / MB, 1) for k, v in sampler.peak.items()},
        "screenshots": shots, "dest_bucket": dst_bucket,
    }
    res["pass"] = (rc == 0 and ind["percent"] == 100.0 and ind["missing"] == 0 and ind["unexpected"] == 0
                   and ind["md5_mismatch"] == 0 and (prog.get("failed") or 0) == 0)
    log(f"[{job}] {'PASS' if res['pass'] else 'FAIL'} {ind['ok_md5']}/{ind['expected']} "
        f"({ind['percent']}%), unexpected={ind['unexpected']}")
    return res


# ------------------------------------------------------------------ report
def write_report(out: Path, a, src, results: list, manifest: dict, env: dict) -> Path:
    total = sum(1 for _ in manifest)
    total_bytes = sum(v["size"] for v in manifest.values())
    lines = [
        f"# s3mover integration results — {env['started']}",
        "",
        f"* Source: **{src.kind}**",
        f"* Destinations: **MinIO-compatible** `{env['minio_dst']}` and **SeaweedFS** `{env['seaweed']}`",
        f"* Data set: {total:,} objects, {total_bytes / MB:.1f} MB (incl. {a.large} multipart objects of "
        f"{a.large_mb} MB+, zero-byte objects, unicode/space keys, `a-c` vs `a/b` ordering trap)",
        f"* Each scenario: start → **hard kill (SIGKILL/TerminateProcess) at ~{int(a.kill_at * 100)}%** → resume "
        f"→ s3mover verify → **independent verification** (list + MD5 of every object)",
        f"* Throttle: source {a.rps} req/s, destination {a.rps * 2} req/s, {a.workers} workers",
        f"* Host: {env['host']}",
        "",
        "| # | Destination | Mode | Window (UTC) | Expected | Killed at | Copied | Already present | Failed | "
        "Verified (MD5) | Unexpected | Peak RSS s3mover | Result |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for i, r in enumerate(results, 1):
        iv = r["independent_verify"]
        lines.append(
            f"| {i} | {r['destination']} | {r['mode']} | {r['window'] or 'all'} | {r['expected']:,} | "
            f"{r['killed_at_dest_count']:,} | {r['copied']:,} | {r['skipped_existing']:,} | {r['failed']} | "
            f"{iv['ok_md5']:,}/{iv['expected']:,} ({iv['percent']}%) | {iv['unexpected']} | "
            f"{r['peak_rss_mb']['s3mover']} MB | {'✅ PASS' if r['pass'] else '❌ FAIL'} |")
    sw = max((r["peak_rss_mb"]["seaweedfs"] for r in results), default=0)
    mi = max((r["peak_rss_mb"]["minio"] for r in results), default=0)
    lines += ["", f"Peak RSS of local server processes during the runs: SeaweedFS (all `weed` processes) "
                  f"{sw} MB, MinIO {str(mi) + ' MB' if mi else 'n/a (not a local process)'}.", ""]
    lines += ["## Dashboard screenshots", ""]
    for r in results:
        if r["screenshots"]:
            lines.append(f"### {r['job']}")
            for s in r["screenshots"]:
                lines.append(f"![{s}]({s})")
            lines.append("")
    rp = out / "REPORT.md"
    rp.write_text("\n".join(lines), encoding="utf-8")
    (out / "results.json").write_text(json.dumps({"env": env, "results": results}, indent=2, default=str))
    return rp


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["emulator", "real"], default="emulator")
    ap.add_argument("--minio", default="http://localhost:9000", help="real MinIO endpoint (source)")
    ap.add_argument("--minio-key", default="minioadmin")
    ap.add_argument("--minio-secret", default="minioadmin")
    ap.add_argument("--minio-dst", help="MinIO endpoint for the minio->minio destination "
                                        "(default: same MinIO, separate buckets; emulator: 2nd emulator process)")
    ap.add_argument("--seaweed", default="http://localhost:8333")
    ap.add_argument("--seaweed-key", default="seaweedadmin")
    ap.add_argument("--seaweed-secret", default="seaweedadmin")
    ap.add_argument("--small", type=int, default=3000, help="number of small objects")
    ap.add_argument("--large", type=int, default=4, help="number of multipart-size objects")
    ap.add_argument("--large-mb", type=int, default=24)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--rps", type=float, default=120, help="source requests/s (dest gets 2x)")
    ap.add_argument("--kill-at", type=float, default=0.35)
    ap.add_argument("--batch-gap", type=float, default=2.0, help="seconds between time batches (real source)")
    ap.add_argument("--only", choices=["minio", "seaweedfs"], help="run one destination only")
    ap.add_argument("--modes", default="full,since,between")
    ap.add_argument("--dashboard-port", type=int, default=8799)
    ap.add_argument("--no-screenshots", action="store_true")
    ap.add_argument("--prefix", default="s3mover-it")
    a = ap.parse_args()
    a.run_id = datetime.now().strftime("%m%d%H%M%S")

    out = HERE / "results" / f"{a.source}-{a.run_id}"
    out.mkdir(parents=True, exist_ok=True)
    state_dir = out / "state"
    httpd = serve(state_dir, "127.0.0.1", a.dashboard_port, background=True)
    shooter = Shooter(out, a.dashboard_port, not a.no_screenshots)

    procs = []
    if a.source == "emulator":
        src = EmulatorSource()
        port = free_port()
        procs.append(subprocess.Popen([sys.executable, "-m", "moto.server", "-H", "127.0.0.1", "-p", str(port)],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        time.sleep(3)
        minio_dst = {"name": "minio", "endpoint": f"http://127.0.0.1:{port}", "key": "minioadmin",
                     "secret": "minioadmin", "type": "minio"}
    else:
        src = RealSource(a.minio, a.minio_key, a.minio_secret, a.batch_gap)
        minio_dst = {"name": "minio", "endpoint": a.minio_dst or a.minio, "key": a.minio_key,
                     "secret": a.minio_secret, "type": "minio"}
    seaweed = {"name": "seaweedfs", "endpoint": a.seaweed, "key": a.seaweed_key, "secret": a.seaweed_secret,
               "type": "seaweedfs"}
    dests = [d for d in (minio_dst, seaweed) if not a.only or d["name"] == a.only]

    src_client = make_client(Endpoint(src.endpoint, src.key, src.secret), 32)
    src_bucket = f"{a.prefix}-src-{a.run_id}"
    log(f"seeding {src.kind} bucket {src_bucket} ...")
    objs = plan_objects(a.small, a.large, a.large_mb)
    manifest, windows = src.seed(src_client, src_bucket, objs)
    log(f"seeded {len(manifest)} objects; windows: {windows}")
    env = {"started": datetime.now().isoformat(timespec="seconds"), "source": src.kind,
           "minio_dst": minio_dst["endpoint"], "seaweed": a.seaweed,
           "host": f"{sys.platform} / Python {sys.version.split()[0]}", "windows": windows,
           "args": vars(a)}

    results = []
    try:
        for d in dests:
            for i, mode in enumerate(a.modes.split(","), 1):
                results.append(run_scenario(a, src, d, mode, windows, manifest, src_bucket, state_dir, out,
                                            shooter, i))
        # overview screenshot of the job selector with all six jobs
        if shooter.page and results:
            s = shooter.shot(results[-1]["job"], "dashboard-dark-mode", theme="dark")
            if s:
                results[-1]["screenshots"].append(s)
    finally:
        shooter.close()
        for p in procs:
            p.terminate()
        httpd.shutdown()
    rp = write_report(out, a, src, results, manifest, env)
    ok = all(r["pass"] for r in results)
    log(f"{'ALL PASS' if ok else 'FAILURES'} - report: {rp}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
