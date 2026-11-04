#!/usr/bin/env python3
"""
pg_store_test.py - validates the v2.2 metadata architecture (shared postgres2 filer store) with the
Enterprise binary on a throw-away mini cluster (1 master, 2 volume servers, 3 filers, 1 S3 gateway
bound to all three filers). Needs a reachable PostgreSQL (lab: 127.0.0.1:5440, db seaweedfs).

  P1  every filer sees every entry immediately (one store, no peer sync to converge)
  P2  SIGKILL of a filer under S3 write load: 0 acknowledged writes lost / unlisted
  P3  metadata insert rate through the filer and PostgreSQL bytes per entry (sizing input)
"""
import argparse
import concurrent.futures as cf
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config

BASE = Path("/home/claude/lab/pgtest")
IP = "127.30.0.{}"


def up(url, t=60):
    t0 = time.time()
    while time.time() - t0 < t:
        try:
            urllib.request.urlopen(url, timeout=2)
            return True
        except Exception as e:  # noqa: BLE001
            if hasattr(e, "code"):
                return True
            time.sleep(0.5)
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--weed", default="/home/claude/lab/bin-ent/weed")
    ap.add_argument("--pg", default="127.0.0.1:5440")
    ap.add_argument("--entries", type=int, default=20000)
    ap.add_argument("--out", default="reports/enterprise/remediation/pg-store.json")
    a = ap.parse_args()
    pg_host, pg_port = a.pg.split(":")
    subprocess.run(["psql", "-h", pg_host, "-p", pg_port, "-U", "postgres", "-c", "drop database if exists seaweedfs",
                    "-c", "create database seaweedfs"], capture_output=True)
    shutil.rmtree(BASE, ignore_errors=True)
    procs = {}

    def start(name, args, cwd=None):
        d = BASE / name
        d.mkdir(parents=True, exist_ok=True)
        lf = open(d / "log", "ab")
        procs[name] = subprocess.Popen([a.weed, *args], cwd=cwd or d, stdout=lf, stderr=subprocess.STDOUT,
                                       start_new_session=True)

    m = f"{IP.format(1)}:9333"
    start("master", ["master", f"-ip={IP.format(1)}", f"-mdir={BASE}/master", "-volumeSizeLimitMB=256",
                     "-defaultReplication=000"])
    assert up(f"http://{m}/cluster/status")
    for i in (1, 2):
        (BASE / f"vol{i}/data").mkdir(parents=True, exist_ok=True)
        start(f"vol{i}", ["volume", f"-ip={IP.format(10 + i)}", "-port=8080", f"-dir={BASE}/vol{i}/data", "-max=50",
                          f"-mserver={m}", f"-rack=r{i}"])
    time.sleep(3)
    toml = f"""[postgres2]
enabled = true
createTable = \"\"\"
  CREATE TABLE IF NOT EXISTS "%s" (dirhash BIGINT, name VARCHAR(65535), directory VARCHAR(65535), meta bytea,
  PRIMARY KEY (dirhash, name));
\"\"\"
hostname = "{pg_host}"
port = {pg_port}
username = "postgres"
password = ""
database = "seaweedfs"
schema = ""
sslmode = "disable"
connection_max_idle = 20
connection_max_open = 50
connection_max_lifetime_seconds = 300
enableUpsert = true
upsertQuery = \"\"\"INSERT INTO "%[1]s" (dirhash,name,directory,meta) VALUES($1,$2,$3,$4)
  ON CONFLICT (dirhash,name) DO UPDATE SET directory=EXCLUDED.directory, meta=EXCLUDED.meta\"\"\"
"""
    filers = []
    for i in (1, 2, 3):
        d = BASE / f"filer{i}"
        d.mkdir(parents=True, exist_ok=True)
        (d / "filer.toml").write_text(toml)
        f = f"{IP.format(20 + i)}:8888"
        filers.append(f)
        start(f"filer{i}", ["filer", f"-ip={IP.format(20 + i)}", f"-master={m}", "-port=8888",
                            f"-localSocket={d}/f.sock"], cwd=d)
        assert up(f"http://{f}/"), f"filer{i} not up"
        time.sleep(2)
    s3ep = f"{IP.format(30)}:8333"
    (BASE / "s3.json").write_text(json.dumps({"identities": [{"name": "t", "credentials": [
        {"accessKey": "pgtest", "secretKey": "pgtest-secret"}], "actions": ["Admin", "Read", "Write", "List"]}]}))
    start("s3", ["s3", f"-config={BASE}/s3.json", f"-ip.bind={IP.format(30)}", "-port=8333", f"-filer={','.join(filers)}",
                 f"-localSocket={BASE}/s3.sock"])
    assert up(f"http://{s3ep}/")
    time.sleep(2)
    c = boto3.client("s3", endpoint_url=f"http://{s3ep}", region_name="us-east-1", aws_access_key_id="pgtest",
                     aws_secret_access_key="pgtest-secret", config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 4},
                                                              connect_timeout=3, read_timeout=15))
    c.create_bucket(Bucket="archive")
    res = {"store": "postgres2", "pg": "PostgreSQL 16 (lab, single instance)"}

    def listed(f, prefix):
        out, last = set(), ""
        while True:
            u = f"http://{f}/buckets/archive/{prefix}?limit=1000&lastFileName={last}"
            r = json.loads(urllib.request.urlopen(urllib.request.Request(u, headers={"Accept": "application/json"}),
                                                  timeout=10).read())
            ents = r.get("Entries") or []
            out |= {e["FullPath"].rsplit("/", 1)[-1] for e in ents}
            if not r.get("ShouldDisplayLoadMore"):
                return out
            last = r.get("LastFileName")

    # P1 ----------------------------------------------------------------------
    keys = [f"{i:05d}" for i in range(300)]
    with cf.ThreadPoolExecutor(16) as ex:
        list(ex.map(lambda k: c.put_object(Bucket="archive", Key=f"p1/{k}", Body=os.urandom(4096)), keys))
    t0 = time.time()
    seen = {f: len(listed(f, "p1")) for f in filers}
    res["P1"] = {"written": len(keys), "listed_per_filer": seen, "check_s": round(time.time() - t0, 2),
                 "status": "PASS" if all(v == len(keys) for v in seen.values()) else "FAIL"}
    print("P1", res["P1"], flush=True)

    # P2 ----------------------------------------------------------------------
    acked, stop = [], threading.Event()

    def writer(w):
        n = 0
        while not stop.is_set():
            k = f"p2/{w}-{n:05d}"
            try:
                c.put_object(Bucket="archive", Key=k, Body=os.urandom(2048))
                acked.append(k.split("/")[1])
            except Exception:  # noqa: BLE001
                pass
            n += 1
    ths = [threading.Thread(target=writer, args=(w,)) for w in range(4)]
    [t.start() for t in ths]
    time.sleep(5)
    os.killpg(procs["filer1"].pid, signal.SIGKILL)
    t_kill = time.time()
    time.sleep(15)
    stop.set()
    [t.join() for t in ths]
    l2, l3 = listed(filers[1], "p2"), listed(filers[2], "p2")
    miss2 = [k for k in acked if k not in l2]
    unread = 0
    for k in acked[::10]:
        try:
            c.get_object(Bucket="archive", Key=f"p2/{k}")["Body"].read()
        except Exception:  # noqa: BLE001
            unread += 1
    res["P2"] = {"acked": len(acked), "missing_on_filer2": len(miss2), "missing_on_filer3": len([k for k in acked if k not in l3]),
                 "unreadable_sampled": unread, "killed": "filer1 (SIGKILL) under 4-thread write load",
                 "status": "PASS" if not miss2 and not unread else "FAIL"}
    print("P2", res["P2"], flush=True)

    # P3 ----------------------------------------------------------------------
    def mk(i):
        req = urllib.request.Request(f"http://{filers[1 + i % 2]}/buckets/archive/p3/{i // 1000:03d}/{i:07d}",
                                     data=b"x", method="PUT")
        urllib.request.urlopen(req, timeout=10).read()
    t0 = time.time()
    with cf.ThreadPoolExecutor(32) as ex:
        list(ex.map(mk, range(a.entries)))
    dt_ = time.time() - t0
    q = subprocess.run(["psql", "-h", pg_host, "-p", pg_port, "-U", "postgres", "-d", "seaweedfs", "-At", "-c",
                        "select sum(pg_total_relation_size(c.oid)), sum(c.reltuples) from pg_class c "
                        "join pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relkind='r'"],
                       capture_output=True, text=True).stdout.strip()
    subprocess.run(["psql", "-h", pg_host, "-p", pg_port, "-U", "postgres", "-d", "seaweedfs", "-c", "analyze"],
                   capture_output=True)
    rows = subprocess.run(["psql", "-h", pg_host, "-p", pg_port, "-U", "postgres", "-d", "seaweedfs", "-At", "-c",
                           "select sum(n_live_tup) from pg_stat_user_tables"], capture_output=True, text=True).stdout.strip()
    size = subprocess.run(["psql", "-h", pg_host, "-p", pg_port, "-U", "postgres", "-d", "seaweedfs", "-At", "-c",
                           "select pg_database_size('seaweedfs')"], capture_output=True, text=True).stdout.strip()
    rows_i, size_i = int(float(rows or 0)), int(size or 0)
    res["P3"] = {"entries_created": a.entries, "seconds": round(dt_, 1), "entries_per_s": round(a.entries / dt_),
                 "pg_rows": rows_i, "pg_db_bytes": size_i,
                 "bytes_per_entry": round(size_i / max(1, rows_i)),
                 "projected_gb_for_383M_entries": round(size_i / max(1, rows_i) * 383e6 / 1e9),
                 "note": "lab: PG + 3 filers + client on 2 vCPU; data-file chunks included in meta size", "raw": q}
    print("P3", res["P3"], flush=True)
    for p in procs.values():
        try:
            os.killpg(p.pid, signal.SIGTERM)
        except Exception:  # noqa: BLE001
            pass
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(json.dumps(res, indent=1))
    return 0 if all(res[k]["status"] == "PASS" for k in ("P1", "P2")) else 1


if __name__ == "__main__":
    sys.exit(main())
