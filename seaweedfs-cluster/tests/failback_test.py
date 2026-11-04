#!/usr/bin/env python3
"""
failback_test.py - exercises bin/sw-failback in the Enterprise lab (finding E-09).

Both clusters stay up; the "DR-era" is simulated: the forward policy is disabled by `sw-failback
start`, writes are then made directly on DR (as applications would after a GSLB switch), and the
DR->Primary snapshot-ON policy plus the reconcile gate must bring every one of them to the Primary.
"""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[1]


def s3(ep):
    return boto3.client("s3", endpoint_url=ep, region_name="us-east-1",
                        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}))


def fb(inv, *args, timeout=1800):
    t0 = time.time()
    r = subprocess.run([str(ROOT / "bin/sw-failback"), "-i", inv, *args], capture_output=True, text=True,
                       timeout=timeout, cwd=ROOT)
    return r.returncode, (r.stdout + r.stderr).strip(), round(time.time() - t0)


def main():
    inv = sys.argv[1] if len(sys.argv) > 1 else "inventory/lab-ent.yaml"
    P, D = "http://127.10.5.11:8333", "http://127.20.5.11:8333"
    out = {"steps": []}
    rows = []
    rc, log, dt = fb(inv, "start")
    out["steps"].append({"step": "start", "rc": rc, "s": dt, "log": log})
    rows.append(["1 start", f"forward policy disabled; DR→Primary policy (snapshot ON, deletes OFF) created; rc={rc}, {dt} s: "
                 + " | ".join(l.strip("✔✘! ") for l in log.splitlines()[-2:])])
    d = s3(D)
    run = time.strftime("%H%M%S")
    keys = []
    t_fail = time.time()
    for i in range(200):
        k = f"failback/{run}/{i:04d}"
        d.put_object(Bucket="archive", Key=k, Body=os.urandom(8192))
        keys.append(k)
    rows.append(["2 DR-era writes", f"{len(keys)} objects written on DR after the forward policy was disabled"])
    time.sleep(60)
    p = s3(P)

    def on_primary():
        n = 0
        for k in keys:
            try:
                p.head_object(Bucket="archive", Key=k)
                n += 1
            except Exception:  # noqa: BLE001
                pass
        return n
    via_dm = on_primary()
    rows.append(["3 replicated by Data Movement alone (60 s)", f"{via_dm}/{len(keys)} DR-era objects on the Primary"])
    rc, log, dt = fb(inv, "verify", "--since", str(int(t_fail - 120)))
    out["steps"].append({"step": "verify", "rc": rc, "s": dt, "log": log})
    rows.append(["4 verify (canary + reconcile loop)", f"rc={rc}, {dt} s: " + " | ".join(l.strip("✔✘! ") for l in log.splitlines()[-3:])])
    after_verify = on_primary()
    rows.append(["5 DR-era objects on the Primary after verify", f"{after_verify}/{len(keys)}"])
    rc2, log2, dt2 = fb(inv, "cutover", "--confirm", "--since", str(int(t_fail - 120)))
    out["steps"].append({"step": "cutover", "rc": rc2, "s": dt2, "log": log2})
    rows.append(["6 cutover", f"rc={rc2}, {dt2} s: " + " | ".join(l.strip("✔✘! ") for l in log2.splitlines()[-4:])])
    r = subprocess.run([str(ROOT / "bin/sw-canary"), "--src", P, "--dst", D, "--bucket", "archive", "--timeout", "120"],
                       capture_output=True, text=True)
    rows.append(["7 forward replication after cutover", r.stdout.strip()])
    out["rows"] = rows
    out["verdict"] = (f"{after_verify}/{len(keys)} DR-era writes on the Primary before cut-over "
                      f"({via_dm} by Data Movement alone in 60 s, rest by the reconcile gate); forward mirror "
                      f"{'resumed' if r.returncode == 0 else 'NOT resumed'} after cut-over")
    print(json.dumps(out["rows"], indent=1))
    (ROOT / "reports/enterprise/remediation/failback.json").write_text(json.dumps(out, indent=1))
    return 0 if after_verify == len(keys) and r.returncode == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
