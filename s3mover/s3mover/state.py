"""SQLite job state.

Exactly ONE connection writes (the engine's DB thread), every other reader
(dashboard, `status` command) opens the file read-only. WAL mode lets them
read while the writer commits, which removes the "database is locked"
failures you get when many worker threads write directly.

Resume model
  * shards.last_key is committed in the SAME transaction as the object rows
    of that listing page -> re-listing restarts exactly after the last
    persisted key, no gaps, no duplicates.
  * objects.status is only advanced after the destination write was
    verified, so a crash at any point re-copies at most the in-flight
    objects (and skip_existing makes even that a cheap HEAD).
"""
from __future__ import annotations

import json
import sqlite3
import time
from contextlib import closing
from pathlib import Path
from typing import Iterable, Optional

PENDING, DONE, SKIPPED, FAILED, MOVED = 0, 2, 3, 4, 5
STATUS_NAMES = {PENDING: "pending", DONE: "copied", SKIPPED: "skipped_existing",
                FAILED: "failed", MOVED: "moved"}
COMPLETE = (DONE, SKIPPED, MOVED)

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS shards(
  id INTEGER PRIMARY KEY,
  src_bucket TEXT NOT NULL, dst_bucket TEXT NOT NULL,
  prefix TEXT NOT NULL, delimited INTEGER NOT NULL DEFAULT 0,
  dest_prefix TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'pending',      -- pending|scanning|scanned
  last_key TEXT,
  listed INTEGER NOT NULL DEFAULT 0,
  matched INTEGER NOT NULL DEFAULT 0,
  matched_bytes INTEGER NOT NULL DEFAULT 0,
  UNIQUE(src_bucket, prefix, delimited)
);
CREATE TABLE IF NOT EXISTS objects(
  id INTEGER PRIMARY KEY,
  shard_id INTEGER NOT NULL,
  key TEXT NOT NULL,
  size INTEGER NOT NULL,
  etag TEXT,
  mtime REAL,
  status INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  UNIQUE(shard_id, key)
);
CREATE INDEX IF NOT EXISTS idx_obj_status ON objects(status, id);
CREATE TABLE IF NOT EXISTS progress(id INTEGER PRIMARY KEY CHECK(id=1), json TEXT, updated REAL);
CREATE TABLE IF NOT EXISTS samples(ts REAL PRIMARY KEY, done_objects INTEGER, done_bytes INTEGER,
                                   src_rps REAL, dst_rps REAL);
CREATE TABLE IF NOT EXISTS errors(id INTEGER PRIMARY KEY, ts REAL, bucket TEXT, key TEXT,
                                  attempt INTEGER, msg TEXT);
CREATE TABLE IF NOT EXISTS runs(id INTEGER PRIMARY KEY, started REAL, ended REAL, status TEXT,
                                args TEXT, summary TEXT);
CREATE TABLE IF NOT EXISTS verify(id INTEGER PRIMARY KEY, ts REAL, shard_id INTEGER, expected INTEGER,
                                  ok INTEGER, missing INTEGER, mismatched INTEGER, extra INTEGER,
                                  samples TEXT);
"""


def connect(path: Path | str, readonly: bool = False) -> sqlite3.Connection:
    path = Path(path)
    if readonly:
        # as_uri() gives file:///C:/... on Windows and percent-encodes spaces
        conn = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=30,
                               check_same_thread=False)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path), timeout=60, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    return conn


class StateStore:
    """Writer-side API. Use from a single thread only."""

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.conn = connect(self.path)
        self.conn.executescript(SCHEMA)
        self._migrate()
        self.conn.commit()

    def _migrate(self) -> None:
        """Add columns introduced after a state file was created (old jobs keep working)."""
        have = {r[1] for r in self.conn.execute("PRAGMA table_info(shards)")}
        for col, ddl in (("before_window", "INTEGER NOT NULL DEFAULT 0"),
                         ("after_window", "INTEGER NOT NULL DEFAULT 0"),
                         ("other_excluded", "INTEGER NOT NULL DEFAULT 0"),
                         ("min_mtime", "REAL"), ("max_mtime", "REAL"),
                         ("months", "TEXT")):
            if col not in have:
                self.conn.execute(f"ALTER TABLE shards ADD COLUMN {col} {ddl}")

    # ---- meta / fingerprint ---------------------------------------------
    def get_meta(self, k: str) -> Optional[str]:
        row = self.conn.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
        return row[0] if row else None

    def set_meta(self, k: str, v: str) -> None:
        self.conn.execute("INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, v))

    def has_work_history(self) -> bool:
        return self.conn.execute("SELECT 1 FROM shards LIMIT 1").fetchone() is not None

    # ---- shards ----------------------------------------------------------
    def add_shard(self, src_bucket: str, dst_bucket: str, prefix: str, delimited: bool,
                  dest_prefix: str) -> int:
        self.conn.execute(
            "INSERT INTO shards(src_bucket,dst_bucket,prefix,delimited,dest_prefix) VALUES(?,?,?,?,?) "
            "ON CONFLICT(src_bucket,prefix,delimited) DO NOTHING",
            (src_bucket, dst_bucket, prefix, int(delimited), dest_prefix))
        row = self.conn.execute(
            "SELECT id FROM shards WHERE src_bucket=? AND prefix=? AND delimited=?",
            (src_bucket, prefix, int(delimited))).fetchone()
        return int(row[0])

    def shards(self) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM shards ORDER BY id").fetchall()

    def shard_stats(self, shard_id: int, st: dict) -> None:
        """Why objects were (not) selected: before/after the window, regex/no date, date range, per month."""
        row = self.conn.execute("SELECT months, min_mtime, max_mtime FROM shards WHERE id=?", (shard_id,)).fetchone()
        months = json.loads(row["months"] or "{}")
        for m, n in st["months"].items():
            months[m] = months.get(m, 0) + n
        mn = min(x for x in (row["min_mtime"], st["min"]) if x is not None) if (row["min_mtime"] or st["min"]) else None
        mx = max(x for x in (row["max_mtime"], st["max"]) if x is not None) if (row["max_mtime"] or st["max"]) else None
        self.conn.execute(
            "UPDATE shards SET before_window=before_window+?, after_window=after_window+?, "
            "other_excluded=other_excluded+?, min_mtime=?, max_mtime=?, months=? WHERE id=?",
            (st["before"], st["after"], st["other"], mn, mx, json.dumps(dict(sorted(months.items()))), shard_id))

    def shard_page(self, shard_id: int, rows: list[tuple], last_key: Optional[str], listed: int) -> int:
        """Persist one listing page atomically with its resume cursor. Returns rows inserted."""
        cur = self.conn.executemany(
            "INSERT INTO objects(shard_id,key,size,etag,mtime) VALUES(?,?,?,?,?) "
            "ON CONFLICT(shard_id,key) DO NOTHING",
            [(shard_id, *r) for r in rows])
        inserted = max(cur.rowcount, 0)
        self.conn.execute(
            "UPDATE shards SET status='scanning', last_key=COALESCE(?, last_key), listed=listed+?, "
            "matched=matched+?, matched_bytes=matched_bytes+? WHERE id=?",
            # exact unless duplicates were ignored; shard_done() recomputes exactly anyway
            (last_key, listed, inserted, sum(r[1] for r in rows) if inserted == len(rows) else 0,
             shard_id))
        return inserted

    def shard_done(self, shard_id: int) -> None:
        self.conn.execute(
            "UPDATE shards SET status='scanned', "
            "matched=(SELECT COUNT(*) FROM objects WHERE shard_id=?), "
            "matched_bytes=(SELECT COALESCE(SUM(size),0) FROM objects WHERE shard_id=?) WHERE id=?",
            (shard_id, shard_id, shard_id))

    def reset_scans(self) -> None:
        """--rescan: list everything again (new objects inside the window get added)."""
        self.conn.execute("UPDATE shards SET status='pending', last_key=NULL")

    # ---- objects ---------------------------------------------------------
    def fetch_pending(self, after_id: int, limit: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT o.id, o.shard_id, o.key, o.size, o.etag, o.mtime, o.attempts, s.src_bucket, s.dst_bucket, "
            "s.dest_prefix FROM objects o JOIN shards s ON s.id=o.shard_id "
            "WHERE o.status=? AND o.id>? ORDER BY o.id LIMIT ?",
            (PENDING, after_id, limit)).fetchall()

    def apply_results(self, results: Iterable[tuple]) -> None:
        """results: (id, status, attempts, error, size, etag)"""
        self.conn.executemany(
            "UPDATE objects SET status=?, attempts=?, error=?, size=COALESCE(?,size), "
            "etag=COALESCE(?,etag) WHERE id=?",
            [(st, att, err, size, etag, oid) for (oid, st, att, err, size, etag) in results])

    def reset_failed(self) -> int:
        return self.conn.execute("UPDATE objects SET status=?, attempts=0, error=NULL WHERE status=?",
                                 (PENDING, FAILED)).rowcount

    def counts(self) -> dict:
        out = {name: {"objects": 0, "bytes": 0} for name in STATUS_NAMES.values()}
        for st, n, b in self.conn.execute(
                "SELECT status, COUNT(*), COALESCE(SUM(size),0) FROM objects GROUP BY status"):
            out[STATUS_NAMES.get(st, str(st))] = {"objects": n, "bytes": b}
        return out

    def shard_counts(self) -> dict:
        """{shard_id: [complete, failed]} - computed once per run start."""
        out: dict = {}
        for sid, st, n in self.conn.execute(
                "SELECT shard_id, status, COUNT(*) FROM objects WHERE status<>? GROUP BY shard_id, status",
                (PENDING,)):
            row = out.setdefault(sid, [0, 0])
            if st in COMPLETE:
                row[0] += n
            elif st == FAILED:
                row[1] += n
        return out

    # ---- telemetry -------------------------------------------------------
    def add_errors(self, errs: list[tuple]) -> None:
        self.conn.executemany("INSERT INTO errors(ts,bucket,key,attempt,msg) VALUES(?,?,?,?,?)", errs)
        # keep the table bounded
        self.conn.execute("DELETE FROM errors WHERE id <= (SELECT MAX(id) FROM errors) - 2000")

    def write_progress(self, snap: dict) -> None:
        self.conn.execute(
            "INSERT INTO progress(id,json,updated) VALUES(1,?,?) "
            "ON CONFLICT(id) DO UPDATE SET json=excluded.json, updated=excluded.updated",
            (json.dumps(snap), time.time()))

    def heartbeat(self, phase: str, activity: str = "") -> None:
        """Keep the dashboard's 'running' signal fresh during long planning / verify phases."""
        row = self.conn.execute("SELECT json FROM progress WHERE id=1").fetchone()
        snap = json.loads(row[0]) if row else {}
        now = time.time()
        snap.update({"phase": phase, "activity": activity, "updated": now})
        snap.pop("status", None)
        self.write_progress(snap)
        self.conn.commit()

    def add_sample(self, ts: float, done_objects: int, done_bytes: int, src_rps: float, dst_rps: float) -> None:
        self.conn.execute("INSERT OR REPLACE INTO samples VALUES(?,?,?,?,?)",
                          (ts, done_objects, done_bytes, src_rps, dst_rps))
        self.conn.execute("DELETE FROM samples WHERE ts < ?", (ts - 6 * 3600,))

    def start_run(self, args: dict) -> int:
        cur = self.conn.execute("INSERT INTO runs(started,status,args) VALUES(?,?,?)",
                                (time.time(), "running", json.dumps(args)))
        self.conn.commit()
        return int(cur.lastrowid)

    def end_run(self, run_id: int, status: str, summary: dict) -> None:
        self.conn.execute("UPDATE runs SET ended=?, status=?, summary=? WHERE id=?",
                          (time.time(), status, json.dumps(summary), run_id))
        self.conn.commit()

    def save_verify(self, shard_id: int, expected: int, ok: int, missing: int, mismatched: int,
                    extra: int, samples: list) -> None:
        self.conn.execute("INSERT INTO verify(ts,shard_id,expected,ok,missing,mismatched,extra,samples) "
                          "VALUES(?,?,?,?,?,?,?,?)",
                          (time.time(), shard_id, expected, ok, missing, mismatched, extra,
                           json.dumps(samples[:50])))

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        try:
            self.conn.commit()
        finally:
            self.conn.close()


# --------------------------------------------------------------------------
# read-only snapshot (dashboard / status)
# --------------------------------------------------------------------------
def snapshot(db_path: Path | str, sample_window: float = 3600) -> dict:
    with closing(connect(db_path, readonly=True)) as c:
        prog = c.execute("SELECT json, updated FROM progress WHERE id=1").fetchone()
        meta = {r["k"]: r["v"] for r in c.execute("SELECT k, v FROM meta")}
        shards = [dict(r) for r in c.execute(
            "SELECT id, src_bucket, dst_bucket, prefix, delimited, status, listed, matched, "
            "matched_bytes, before_window, after_window, other_excluded, min_mtime, max_mtime, months "
            "FROM shards ORDER BY id")]
        last = c.execute("SELECT MAX(ts) FROM samples").fetchone()[0] or 0
        samples = [dict(r) for r in c.execute(
            "SELECT ts, done_objects, done_bytes, src_rps, dst_rps FROM samples WHERE ts>=? ORDER BY ts",
            (last - sample_window,))]
        errors = [dict(r) for r in c.execute(
            "SELECT ts, bucket, key, attempt, msg FROM errors ORDER BY id DESC LIMIT 25")]
        runs = [dict(r) for r in c.execute(
            "SELECT id, started, ended, status, args, summary FROM runs ORDER BY id DESC LIMIT 10")]
        ver = [dict(r) for r in c.execute(
            "SELECT v.*, s.src_bucket, s.prefix FROM verify v JOIN shards s ON s.id=v.shard_id "
            "WHERE v.ts >= (SELECT COALESCE(MAX(ts),0) - 600 FROM verify) ORDER BY v.id")]
    return {
        "progress": json.loads(prog["json"]) if prog else {},
        "progress_updated": prog["updated"] if prog else None,
        "meta": meta,
        "shards": shards,
        "samples": samples,
        "errors": errors,
        "runs": runs,
        "verify": ver,
    }
