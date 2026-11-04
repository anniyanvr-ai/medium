#!/usr/bin/env python3
"""
tier_remediation.py - reproduces the tiering findings E-03 (leftover replicated copy after EC) and,
when it occurs, E-07 (masters keep a demoted volume writable), then lets `sw-guard --fix` repair them.

Uses the lab bucket archive63 (bound to a 60 s -> EC 6+3 test rung) with the sw_admin identity.
A slow writer keeps writing during the demotion - that is what exposes E-07.
"""
import argparse
import hashlib
import importlib.machinery
import importlib.util
import json
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[1]
_l = importlib.machinery.SourceFileLoader("swctl", str(ROOT / "bin/swctl"))
_s = importlib.util.spec_from_loader("swctl", _l)
sw = importlib.util.module_from_spec(_s)
sys.modules["swctl"] = sw
_l.exec_module(sw)
sys.path.insert(0, str(ROOT / "bin"))
from seaweed_admin_api import AdminAPI  # noqa: E402


def guard(inv_path, fix):
    cmd = [str(ROOT / "bin/sw-guard"), "-i", inv_path, "--cluster", "primary", "--check", "tier", "--check", "layout",
           "--json"] + (["--fix"] if fix else [])
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=1200)
    try:
        return json.loads(r.stdout), r.stderr[-1500:]
    except Exception:  # noqa: BLE001
        return {"raw": r.stdout[-500:]}, r.stderr[-1500:]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--inventory", default="inventory/lab-ent.yaml")
    ap.add_argument("--bucket", default="archive63")
    ap.add_argument("--n", type=int, default=150)
    ap.add_argument("--out", default="reports/enterprise/remediation/tier-remediation.json")
    a = ap.parse_args()
    inv = sw.Inventory(a.inventory)
    sec = dict(l.split("=", 1) for l in (inv.build_dir / "secrets.env").read_text().splitlines() if "=" in l)
    ep = f"http://{inv.s3s('primary')[0]}"
    c = boto3.client("s3", endpoint_url=ep, region_name="us-east-1", aws_access_key_id="sw_admin",
                     aws_secret_access_key=sec["SW_S3_ADMIN_SECRET"],
                     config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}, read_timeout=15))
    adm = AdminAPI(f"http://{inv.admin_node('primary')['ip']}:{inv.port('admin', cluster='primary')}")
    res = {"bucket": a.bucket}
    ok, dt_, _n, last = sw.verify_landing(inv, "primary", a.bucket, "ssd", "archive-data", timeout=300)
    res["landing_gate"] = {"ok": ok, "s": round(dt_, 1)}
    ec0 = set(int(v) for v in re.findall(r"ec volume id:(\d+) collection:" + a.bucket,
                                          sw.weed_shell(inv, "primary", "volume.list")))
    res["ec_volumes_before"] = sorted(ec0)
    run = time.strftime("%H%M%S")
    ds = {}
    for i in range(a.n):
        b = os.urandom(256 * 1024)
        k = f"rem/{run}/{i:04d}"
        c.put_object(Bucket=a.bucket, Key=k, Body=b)
        ds[k] = hashlib.md5(b).hexdigest()
    put_err, put_ok, stop = [], [0], threading.Event()

    def slow_writer():
        n = 0
        while not stop.is_set():
            try:
                c.put_object(Bucket=a.bucket, Key=f"rem/{run}/live-{n:05d}", Body=os.urandom(4096))
                put_ok[0] += 1
            except Exception as e:  # noqa: BLE001
                put_err.append((round(time.time()), str(e)[:140]))
            n += 1
            time.sleep(8)          # sparse writes: some writable volumes go quiet and demote while still writable
    th = threading.Thread(target=slow_writer, daemon=True)
    th.start()
    time.sleep(70)
    adm.enable_job("volume_tiering", detection_interval_minutes=1)
    adm.detect("volume_tiering")
    t0 = time.time()
    # wait until the worker has produced complete 6+3 sets (or 15 min)
    report, findings = None, {}
    while time.time() - t0 < 900:
        report, log = guard(a.inventory, False)
        findings = {f["check"] + ":" + f["level"]: f for f in report.get("findings", [])}
        bad_f = [f for f in report.get("findings", []) if f["level"] in ("FAIL", "WARN") and f["check"] in ("tier", "layout")
                 and "being written" not in f["msg"] and "no volume class" not in f["msg"]]
        new_ec = set(int(v) for v in re.findall(r"ec volume id:(\d+) collection:" + a.bucket,
                                                sw.weed_shell(inv, "primary", "volume.list"))) - ec0
        if bad_f:
            break
        if new_ec and time.time() - t0 > 300:      # demoted cleanly, nothing to fix
            break
        time.sleep(30)
    res["detect_after_s"] = round(time.time() - t0)
    res["before_fix"] = [{"check": f["check"], "level": f["level"], "msg": f["msg"]} for f in report.get("findings", [])]
    res["put_errors_before_fix"] = len(put_err)
    res["put_error_sample"] = put_err[:3]
    fixed, flog = guard(a.inventory, True)
    res["fix"] = [{"check": f["check"], "level": f["level"], "msg": f["msg"]} for f in fixed.get("findings", [])]
    res["fix_actions"] = [x["action"] for x in fixed.get("actions", [])]
    time.sleep(20)
    after, _ = guard(a.inventory, False)
    res["after_fix"] = [{"check": f["check"], "level": f["level"], "msg": f["msg"]} for f in after.get("findings", [])]
    n_err_at_fix = len(put_err)
    time.sleep(30)
    stop.set()
    th.join()
    res["put_errors_after_fix"] = len(put_err) - n_err_at_fix
    res["puts_ok"] = put_ok[0]
    bad = 0
    for k, h in ds.items():
        try:
            if hashlib.md5(c.get_object(Bucket=a.bucket, Key=k)["Body"].read()).hexdigest() != h:
                bad += 1
        except Exception:  # noqa: BLE001
            bad += 1
    res["dataset_unreadable_or_corrupt"] = f"{bad}/{len(ds)}"
    txt = sw.weed_shell(inv, "primary", "volume.list")
    res["new_ec_volumes"] = sorted(set(int(v) for v in re.findall(r"ec volume id:(\d+) collection:" + a.bucket, txt)) - ec0)
    res["ec_volumes"] = sorted({int(v) for v in re.findall(r"ec volume id:(\d+) collection:" + a.bucket, txt)})
    res["guard_log"] = flog
    print(json.dumps({k: v for k, v in res.items() if k != "guard_log"}, indent=1))
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(json.dumps(res, indent=1))
    return 0 if bad == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
