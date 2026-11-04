"""Transfer engine: scan -> filter -> copy/move -> verify, resumable.

Threads
  main      : the ONLY SQLite writer. Persists listing pages, feeds the work
              queue from the DB, applies results, writes progress/samples.
  scanner xN: paginated ListObjectsV2 per shard (1 page in memory each).
  worker  xN: stream GET -> PUT (single or multipart) -> verify -> [delete src].
  guard     : pause file + optional MinIO memory watchdog.

Memory on the migration host is bounded by:
  work queue (rows) + ByteBudget(max_inflight_mb) for object bodies/parts.
Load on MinIO/SeaweedFS is bounded by the two AIMD rate limiters and the
fixed number of workers/scanners; nothing ever lists a bucket recursively
into memory.
"""
from __future__ import annotations

import base64
import hashlib
import logging
import os
import queue
import random
import re
import signal
import threading
import time
import urllib.request
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Optional

from .config import Config
from .dates import parse_segment
from .filters import ObjectFilter
from .ratelimit import AdaptiveRateLimiter
from .s3client import error_code, is_not_found, is_throttle, make_client
from .state import COMPLETE, DONE, FAILED, MOVED, PENDING, SKIPPED, StateStore

log = logging.getLogger("s3mover")

MB = 1024 * 1024
META_SRC_ETAG = "s3mover-src-etag"
META_SRC_MTIME = "s3mover-src-mtime"


class IntegrityError(Exception):
    """Destination object does not match what was read from the source."""


class FingerprintMismatch(RuntimeError):
    pass


@dataclass
class WorkItem:
    id: int
    key: str
    size: int
    etag: str
    mtime: float
    attempts: int
    src_bucket: str
    dst_bucket: str
    dest_prefix: str


class _Tagged:
    """Client proxy that records which side (src/dst) raised an exception,
    so back-off is applied to the endpoint that is actually overloaded."""

    def __init__(self, client, side: str):
        self._c = client
        self._side = side

    def __getattr__(self, name):
        attr = getattr(self._c, name)
        if not callable(attr):
            return attr

        def call(*a, **kw):
            try:
                return attr(*a, **kw)
            except Exception as exc:
                try:
                    exc.s3mover_side = self._side
                except Exception:
                    pass
                raise
        return call


class ByteBudget:
    """Blocks until `n` bytes fit under the global in-flight budget."""

    def __init__(self, max_bytes: int):
        self.max = max_bytes
        self.used = 0
        self._cv = threading.Condition()

    @contextmanager
    def reserve(self, n: int):
        n = min(max(n, 1), self.max)
        with self._cv:
            while self.used + n > self.max:
                self._cv.wait(0.5)
            self.used += n
        try:
            yield
        finally:
            with self._cv:
                self.used -= n
                self._cv.notify_all()


def _strip(etag: Optional[str]) -> str:
    return (etag or "").strip('"')


def _read_exact(body, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = body.read(n - len(buf))
        if not chunk:
            break
        buf += chunk
    return bytes(buf)


class Engine:
    def __init__(self, cfg: Config, *, fresh: bool = False, rescan: bool = False,
                 retry_failed: bool = True, stop_after: Optional[int] = None,
                 install_signals: bool = True):
        self.cfg = cfg
        self.fresh = fresh
        self.rescan = rescan
        self.retry_failed = retry_failed
        self.stop_after = stop_after
        self.install_signals = install_signals
        self.filter = ObjectFilter(cfg.filter)
        f = cfg.filter
        self._pruning = (f.date_source == "key_pattern" and f.mode != "full" and f.prune_date_folders)
        self.pruned_folders = 0
        fo = cfg.transfer.folder_objects
        self.rename_folder_objects = fo == "rename" or (fo == "auto" and cfg.destination.type == "seaweedfs")
        t, th = cfg.transfer, cfg.throttle
        pool = t.workers + t.scan_workers + 4
        self.src = make_client(cfg.source, pool)
        self.dst = make_client(cfg.destination, pool)
        self.src_rl = AdaptiveRateLimiter(th.source_rps, min_rate=th.min_rps, adaptive=th.adaptive)
        self.dst_rl = AdaptiveRateLimiter(th.dest_rps, min_rate=th.min_rps, adaptive=th.adaptive)
        self.budget = ByteBudget(t.max_inflight_mb * MB)
        self.stop = threading.Event()
        self.unpaused = threading.Event()
        self.unpaused.set()
        self.pause_reason = ""
        self.inbox: "queue.Queue[tuple]" = queue.Queue(maxsize=512)
        self.work_q: "queue.Queue[Optional[WorkItem]]" = queue.Queue(maxsize=max(64, t.workers * 16))
        self.shard_q: "queue.Queue[Optional[int]]" = queue.Queue()
        self.scan_errors = 0
        self.phase = "starting"
        self.store: Optional[StateStore] = None

    @property
    def src(self):
        return self._src

    @src.setter
    def src(self, c):
        self._src = _Tagged(c, "src")

    @property
    def dst(self):
        return self._dst

    @dst.setter
    def dst(self, c):
        self._dst = _Tagged(c, "dst")

    # ------------------------------------------------------------------ setup
    def _open_store(self) -> StateStore:
        db = self.cfg.db_path
        if self.fresh and db.exists():
            for suffix in ("", "-wal", "-shm"):
                p = Path(str(db) + suffix)
                if p.exists():
                    p.unlink()
        store = StateStore(db)
        fp = self.cfg.fingerprint()
        old = store.get_meta("fingerprint")
        if old and old != fp:
            store.close()
            raise FingerprintMismatch(
                f"state {db} belongs to a different job definition (filter/buckets/endpoints/action "
                f"changed). Use a new job.name or --fresh to discard the old state.")
        store.set_meta("fingerprint", fp)
        store.set_meta("job", self.cfg.job_name)
        store.set_meta("window", self.filter.window.describe())
        store.set_meta("mode", self.cfg.filter.mode)
        store.set_meta("action", self.cfg.transfer.action)
        store.set_meta("source", self.cfg.source.endpoint)
        store.set_meta("destination", self.cfg.destination.endpoint)
        store.set_meta("src_type", self.cfg.source.type)
        store.set_meta("dst_type", self.cfg.destination.type)
        store.commit()
        return store

    def _plan_shards(self, store: StateStore) -> None:
        for b in self.cfg.buckets:
            planned = store.get_meta(f"planned:{b.source}") == "1"
            if not planned:
                self._ensure_dest_bucket(b.destination)
            if b.date_prefixes:
                for p in self._expand_date_prefixes(b.date_prefixes):
                    store.add_shard(b.source, b.destination, p, False, b.dest_prefix)
            elif b.prefixes:
                for p in b.prefixes:
                    if self._pruning and p.endswith("/"):
                        for sp, delim in self._walk_date_tree(b.source, p, (), 0):
                            store.add_shard(b.source, b.destination, sp, delim, b.dest_prefix)
                    else:
                        store.add_shard(b.source, b.destination, p, False, b.dest_prefix)
            elif b.auto_shard:
                store.add_shard(b.source, b.destination, "", True, b.dest_prefix)  # root objects
                known = {(r["prefix"], bool(r["delimited"])) for r in store.shards() if r["src_bucket"] == b.source}
                if self._pruning:
                    found = []
                    for p in self._top_level_prefixes(b.source):
                        found += self._walk_date_tree(b.source, p, (), 0)
                    log.info("date-folder pruning on %s: %d folders to list, %d folders outside the window "
                             "skipped without listing", b.source, len(found), self.pruned_folders)
                else:
                    found = [(p, False) for p in self._top_level_prefixes(b.source)]
                for p, delim in found:
                    if (p, delim) not in known:
                        store.add_shard(b.source, b.destination, p, delim, b.dest_prefix)
                        if planned:  # folder created after this job was first planned
                            log.info("new folder %s/%s found -> added to the job", b.source, p)
            else:
                store.add_shard(b.source, b.destination, "", False, b.dest_prefix)
            # marker in the same transaction as the shards: a crash mid-plan re-plans the bucket
            store.set_meta(f"planned:{b.source}", "1")
            store.commit()

    def _heartbeat(self, activity: str, force: bool = False) -> None:
        """Main-thread only (planning / verify): proves to the dashboard that we are alive."""
        now = time.monotonic()
        if self.store is None or (not force and now - getattr(self, "_last_hb", 0.0) < 2.0):
            return
        self._last_hb = now
        try:
            self.store.heartbeat(self.phase, activity)
            if not hasattr(self, "_pid_written"):
                self.store.conn.execute("UPDATE progress SET json=json_set(json,'$.pid',?) WHERE id=1", (os.getpid(),))
                self.store.commit()
                self._pid_written = True
        except Exception as exc:  # never let telemetry break the job
            log.debug("heartbeat failed: %s", exc)

    # ---------------------------------------------------------- date pruning
    _YEAR = re.compile(r"(?:19|20)\d{2}$")

    def _segment_date(self, parts: tuple, seg: str):
        """Extend a partial (y[, m[, d]]) date with a folder name, or None if not a date level.
        Handles 2025/ 11/ 2/, 2025-08-15/, 20250815/, 15-08-2025/, 2025-08/, 202508/ ..."""
        return parse_segment(parts, seg)

    def _partial_in_window(self, parts: tuple) -> bool:
        tz, w = self.filter.tz, self.filter.window
        y = parts[0]
        if len(parts) == 1:
            lo, hi = datetime(y, 1, 1, tzinfo=tz), datetime(y + 1, 1, 1, tzinfo=tz)
        elif len(parts) == 2:
            m = parts[1]
            lo = datetime(y, m, 1, tzinfo=tz)
            hi = datetime(y + (m == 12), m % 12 + 1, 1, tzinfo=tz)
        else:
            lo = datetime(y, parts[1], parts[2], tzinfo=tz)
            hi = lo + timedelta(days=1)
        if w.start is not None and hi <= w.start:
            return False
        if w.end is not None and (lo > w.end if w.end_inclusive else lo >= w.end):
            return False
        return True

    def _walk_date_tree(self, bucket: str, prefix: str, parts: tuple, depth: int) -> list[tuple[str, bool]]:
        """Descend year/month/day folders with delimiter listings. Folders whose date range is
        outside the window are skipped WITHOUT listing their contents. Returns shards:
        (prefix, True)  = only the objects directly inside a walked folder,
        (prefix, False) = full recursive listing (a day folder, or a subtree with no date folders)."""
        if len(parts) == 3:
            return [(prefix, False)]  # day folder: list everything below it
        children, has_objects, token = [], False, None
        while True:
            kw = dict(Bucket=bucket, Prefix=prefix, Delimiter="/", MaxKeys=self.cfg.throttle.list_page_size)
            if token:
                kw["ContinuationToken"] = token
            self._heartbeat(f"finding date folders: {bucket}/{prefix} ({self.pruned_folders:,} out-of-window folders skipped)")
            resp = self._with_retry(lambda: self.src.list_objects_v2(**kw), self.src_rl, what=f"walk {bucket}/{prefix}")
            has_objects = has_objects or bool(resp.get("Contents"))
            children += [cp["Prefix"] for cp in resp.get("CommonPrefixes", []) or []]
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        dated = {c: self._segment_date(parts, c[len(prefix):].rstrip("/")) for c in children}
        if not parts and not any(dated.values()) and (depth >= 2 or len(children) > 50):
            # no year folders here and too deep/wide to keep looking: list the folder normally
            return [(prefix, False)]
        out: list[tuple[str, bool]] = [(prefix, True)] if has_objects else []
        for child in children:
            nparts = dated[child]
            if nparts is None:
                if parts:   # non-date folder inside a dated path (e.g. 2025/misc/): can't prune it
                    out.append((child, False))
                else:       # still looking for the year level (e.g. 1KB/ -> 1KB/2025/)
                    out += self._walk_date_tree(bucket, child, parts, depth + 1)
            elif self._partial_in_window(nparts):
                out += self._walk_date_tree(bucket, child, nparts, depth + 1)
            else:
                self.pruned_folders += 1
        return out

    def _expand_date_prefixes(self, templates: list[str]) -> list[str]:
        """One listing root per calendar day in the window (deduplicated, so a {yyyy}/{mm}/
        template yields one prefix per month). Only these folders are ever listed."""
        from datetime import timedelta
        w, tz = self.filter.window, self.filter.tz
        start = w.start.astimezone(tz).date()
        end_dt = (w.end or datetime.now(timezone.utc))
        if w.end is not None and not w.end_inclusive:
            end_dt = end_dt - timedelta(microseconds=1)
        end_dt = end_dt.astimezone(tz)
        end = end_dt.date()
        out: list[str] = []
        d = start
        while d <= end:
            for tpl in templates:
                p = tpl.format(yyyy=f"{d.year:04d}", mm=f"{d.month:02d}", dd=f"{d.day:02d}",
                               yy=f"{d.year % 100:02d}", m=str(d.month), d=str(d.day))
                if p not in out:
                    out.append(p)
            d += timedelta(days=1)
        return out

    def _top_level_prefixes(self, bucket: str) -> Iterator[str]:
        token = None
        while True:
            kw = dict(Bucket=bucket, Delimiter="/", MaxKeys=self.cfg.throttle.list_page_size)
            if token:
                kw["ContinuationToken"] = token
            self.src_rl.acquire()
            resp = self.src.list_objects_v2(**kw)
            for cp in resp.get("CommonPrefixes", []) or []:
                yield cp["Prefix"]
            if not resp.get("IsTruncated"):
                return
            token = resp.get("NextContinuationToken")

    def _ensure_dest_bucket(self, bucket: str) -> None:
        try:
            self.dst_rl.acquire()
            self.dst.head_bucket(Bucket=bucket)
        except Exception as exc:
            if error_code(exc) in ("404", "NoSuchBucket", "NotFound"):
                self.dst_rl.acquire()
                self.dst.create_bucket(Bucket=bucket)
                log.info("created destination bucket %s", bucket)
            else:
                raise

    # ------------------------------------------------------------------ guard
    def _wait_unpaused(self) -> None:
        while not self.unpaused.wait(0.5):
            if self.stop.is_set():
                return

    def _guard_loop(self) -> None:
        th = self.cfg.throttle
        last_mem_check = 0.0
        mem_paused = False
        while not self.stop.is_set():
            reasons = []
            if th.pause_file and Path(th.pause_file).exists():
                reasons.append(f"pause file {th.pause_file}")
            if th.memory_guard_url and th.memory_guard_max_rss_mb and \
                    time.monotonic() - last_mem_check >= th.memory_guard_interval:
                last_mem_check = time.monotonic()
                rss = self._minio_max_rss_mb()
                if rss is None:
                    mem_paused = True  # fail safe
                elif rss > th.memory_guard_max_rss_mb:
                    mem_paused = True
                elif rss < th.memory_guard_max_rss_mb * 0.85:
                    mem_paused = False
                self.last_rss_mb = rss
            if mem_paused:
                reasons.append(f"MinIO memory guard (max RSS {getattr(self, 'last_rss_mb', None)} MB)")
            self.pause_reason = "; ".join(reasons)
            if reasons:
                self.unpaused.clear()
            else:
                self.unpaused.set()
            self.stop.wait(1.0)

    def _minio_max_rss_mb(self) -> Optional[float]:
        th = self.cfg.throttle
        try:
            req = urllib.request.Request(th.memory_guard_url)
            if th.memory_guard_token:
                req.add_header("Authorization", f"Bearer {th.memory_guard_token}")
            with urllib.request.urlopen(req, timeout=10) as r:
                text = r.read().decode()
            vals = [float(line.rsplit(" ", 1)[1]) for line in text.splitlines()
                    if line.startswith("minio_node_process_resident_memory_bytes")]
            return max(vals) / MB if vals else None
        except Exception as exc:  # pragma: no cover - network dependent
            log.warning("memory guard: metrics unavailable (%s) -> pausing", exc)
            return None

    # ---------------------------------------------------------------- scanner
    def _scanner_loop(self) -> None:
        while not self.stop.is_set():
            try:
                sid = self.shard_q.get_nowait()
            except queue.Empty:
                return
            if sid is None:
                return
            try:
                self._scan_shard(*sid)
            except Exception as exc:
                self.scan_errors += 1
                log.error("scan of shard %s failed: %s", sid[0], exc)
                self.inbox.put(("error", "", f"<scan shard {sid[0]}>", 0, f"scan failed: {exc!r}"))

    def _scan_shard(self, shard_id: int, bucket: str, prefix: str, delimited: bool,
                    last_key: Optional[str]) -> None:
        th = self.cfg.throttle
        token = None
        while not self.stop.is_set():
            self._wait_unpaused()
            kw = dict(Bucket=bucket, Prefix=prefix, MaxKeys=th.list_page_size)
            if delimited:
                kw["Delimiter"] = "/"
            if token:
                kw["ContinuationToken"] = token
            elif last_key:
                kw["StartAfter"] = last_key
            resp = self._with_retry(lambda: self.src.list_objects_v2(**kw), self.src_rl,
                                    what=f"list {bucket}/{prefix}")
            contents = resp.get("Contents", []) or []
            rows = []
            st = {"before": 0, "after": 0, "other": 0, "min": None, "max": None, "months": {}}
            for o in contents:
                lm: datetime = o["LastModified"]
                verdict, ts = self.filter.classify(o["Key"], lm)
                if verdict == "match":
                    rows.append((o["Key"], int(o["Size"]), _strip(o.get("ETag")), lm.timestamp()))
                elif verdict in ("before", "after"):
                    st[verdict] += 1
                else:
                    st["other"] += 1
                if ts is not None:  # date distribution of EVERYTHING listed (the date the filter uses)
                    t = ts.timestamp()
                    st["min"] = t if st["min"] is None else min(st["min"], t)
                    st["max"] = t if st["max"] is None else max(st["max"], t)
                    m = ts.astimezone(self.filter.tz).strftime("%Y-%m")
                    st["months"][m] = st["months"].get(m, 0) + 1
            page_last = contents[-1]["Key"] if contents else None
            self.inbox.put(("page", shard_id, rows, page_last, len(contents), st))
            if not resp.get("IsTruncated"):
                self.inbox.put(("shard_done", shard_id))
                return
            token = resp.get("NextContinuationToken")
            if not token and page_last:  # some servers omit the token -> fall back to StartAfter
                last_key, token = page_last, None

    def _with_retry(self, fn, rl: AdaptiveRateLimiter, what: str, attempts: int = 8):
        for i in range(1, attempts + 1):
            rl.acquire()
            try:
                r = fn()
                rl.on_success()
                return r
            except Exception as exc:
                if is_throttle(exc):
                    rl.on_throttle()
                if i == attempts or self.stop.is_set():
                    raise
                delay = min(60.0, self.cfg.transfer.retry_base_delay * 2 ** (i - 1)) * (0.5 + random.random())
                log.warning("%s failed (%s), retry %d in %.1fs", what, error_code(exc), i, delay)
                self.stop.wait(delay)

    # ----------------------------------------------------------------- worker
    def _worker_loop(self) -> None:
        while True:
            item = self.work_q.get()
            if item is None:
                return
            if self.stop.is_set():
                # leave it pending; it will be picked up on resume
                self.inbox.put(("result", item.id, PENDING, item.attempts, None, None, None, 0, 0))
                continue
            self._wait_unpaused()
            self.inbox.put(("result", *self._process(item)))

    def _process(self, it: WorkItem) -> tuple:
        """Returns (id, status, attempts, error, size, etag, bytes_transferred, size_for_progress)."""
        t = self.cfg.transfer
        attempt = it.attempts
        last_err = None
        while True:
            attempt += 1
            try:
                status, size, etag, moved_bytes = self._copy_one(it)
                return (it.id, status, attempt, None, size, etag, moved_bytes, size)
            except Exception as exc:
                last_err = f"{error_code(exc)}: {exc}"[:500]
                if is_throttle(exc):
                    side = getattr(exc, "s3mover_side", None)
                    if side in (None, "src"):
                        self.src_rl.on_throttle()
                    if side in (None, "dst"):
                        self.dst_rl.on_throttle()
                self.inbox.put(("error", it.src_bucket, it.key, attempt, last_err))
                if attempt >= t.max_retries:
                    return (it.id, FAILED, attempt, last_err, None, None, 0, it.size)
                if self.stop.is_set():
                    return (it.id, PENDING, attempt, last_err, None, None, 0, 0)
                delay = min(60.0, t.retry_base_delay * 2 ** (attempt - 1)) * (0.5 + random.random())
                self.stop.wait(delay)

    def _dest_matches(self, it: WorkItem, dkey: str, size: int, etag: str) -> bool:
        try:
            self.dst_rl.acquire()
            h = self.dst.head_object(Bucket=it.dst_bucket, Key=dkey)
            self.dst_rl.on_success()
        except Exception as exc:
            if is_not_found(exc):
                return False
            raise
        if int(h["ContentLength"]) != size:
            return False
        d_etag = _strip(h.get("ETag"))
        src_tag = (h.get("Metadata") or {}).get(META_SRC_ETAG, "")
        return d_etag == etag or src_tag == etag

    def _delete_source(self, it: WorkItem) -> None:
        self.src_rl.acquire()
        self.src.delete_object(Bucket=it.src_bucket, Key=it.key)
        self.src_rl.on_success()

    def dest_key(self, key: str, size: int, dest_prefix: str = "") -> str:
        """Destination key. A non-empty object whose key ends in '/' cannot be stored under that
        key on SeaweedFS (it becomes a directory, 0 bytes) -> append the folder-object suffix."""
        k = dest_prefix + key
        if self.rename_folder_objects and key.endswith("/") and size > 0:
            k += self.cfg.transfer.folder_object_suffix
        return k

    def _copy_one(self, it: WorkItem) -> tuple[int, int, str, int]:
        t = self.cfg.transfer
        dkey = self.dest_key(it.key, it.size, it.dest_prefix)
        move = t.action == "move"

        if t.skip_existing and self._dest_matches(it, dkey, it.size, it.etag):
            if move:
                self._delete_source(it)
                return MOVED, it.size, it.etag, 0
            return SKIPPED, it.size, it.etag, 0

        self.src_rl.acquire()
        try:
            resp = self.src.get_object(Bucket=it.src_bucket, Key=it.key)
        except Exception as exc:
            if is_not_found(exc) and move and self._dest_matches(it, dkey, it.size, it.etag):
                # previous run copied + deleted but crashed before recording it
                return MOVED, it.size, it.etag, 0
            raise
        self.src_rl.on_success()
        body = resp["Body"]
        size = int(resp["ContentLength"])
        etag = _strip(resp.get("ETag")) or it.etag
        if size != it.size:  # object changed since listing; the folder-object rule may now differ
            dkey = self.dest_key(it.key, size, it.dest_prefix)
        extra: dict = {}
        if t.preserve_metadata:
            for k in ("ContentType", "CacheControl", "ContentDisposition", "ContentEncoding",
                      "ContentLanguage"):
                if resp.get(k):
                    extra[k] = resp[k]
            meta = dict(resp.get("Metadata") or {})
        else:
            meta = {}
        meta[META_SRC_ETAG] = etag
        if dkey != it.dest_prefix + it.key:
            meta["s3mover-original-key"] = it.key
        meta[META_SRC_MTIME] = datetime.fromtimestamp(it.mtime, timezone.utc).isoformat() if it.mtime else ""
        extra["Metadata"] = meta

        try:
            if size <= t.multipart_threshold_mb * MB:
                with self.budget.reserve(size):
                    data = _read_exact(body, size + 1)
                    if len(data) != size:
                        raise IntegrityError(f"short read: got {len(data)} of {size} bytes")
                    md5 = hashlib.md5(data)
                    self.dst_rl.acquire()
                    r = self.dst.put_object(Bucket=it.dst_bucket, Key=dkey, Body=data,
                                            ContentLength=size,
                                            ContentMD5=base64.b64encode(md5.digest()).decode(), **extra)
                    self.dst_rl.on_success()
                    if _strip(r.get("ETag")) not in ("", md5.hexdigest()):
                        raise IntegrityError(f"put ETag {r.get('ETag')} != md5 {md5.hexdigest()}")
                    # if source etag is a plain MD5 it must match what we read
                    if re.fullmatch(r"[0-9a-f]{32}", etag) and etag != md5.hexdigest():
                        raise IntegrityError(f"source ETag {etag} != md5 of bytes read {md5.hexdigest()}")
            else:
                self._multipart(it, dkey, body, size, extra)
        finally:
            try:
                body.close()
            except Exception:
                pass

        if dkey.endswith("/") and size > 0:
            # "keep" policy for a folder object: make sure the server really stored the bytes
            self.dst_rl.acquire()
            h = self.dst.head_object(Bucket=it.dst_bucket, Key=dkey)
            if int(h["ContentLength"]) != size:
                raise IntegrityError(f"destination stored {h['ContentLength']} of {size} bytes for folder "
                                     f"object {dkey!r}; set transfer.folder_objects: rename")

        if move:
            if not self._dest_matches(it, dkey, size, etag):
                raise IntegrityError("post-copy verification failed; source NOT deleted")
            self._delete_source(it)
            return MOVED, size, etag, size
        return DONE, size, etag, size

    def _multipart(self, it: WorkItem, dkey: str, body, size: int, extra: dict) -> None:
        part = self.cfg.transfer.part_size_mb * MB
        self.dst_rl.acquire()
        mpu = self.dst.create_multipart_upload(Bucket=it.dst_bucket, Key=dkey, **extra)
        uid = mpu["UploadId"]
        parts, total, n = [], 0, 1
        try:
            with self.budget.reserve(part):
                while True:
                    if self.stop.is_set():
                        raise InterruptedError("stop requested")
                    chunk = _read_exact(body, part)
                    if not chunk:
                        break
                    md5 = hashlib.md5(chunk)
                    self.dst_rl.acquire()
                    r = self.dst.upload_part(Bucket=it.dst_bucket, Key=dkey, UploadId=uid, PartNumber=n,
                                             Body=chunk, ContentMD5=base64.b64encode(md5.digest()).decode())
                    parts.append({"ETag": r["ETag"], "PartNumber": n})
                    total += len(chunk)
                    n += 1
            if total != size:
                raise IntegrityError(f"multipart short read: {total} of {size} bytes")
            self.dst_rl.acquire()
            self.dst.complete_multipart_upload(Bucket=it.dst_bucket, Key=dkey, UploadId=uid,
                                               MultipartUpload={"Parts": parts})
            self.dst_rl.on_success()
        except BaseException:
            try:
                self.dst.abort_multipart_upload(Bucket=it.dst_bucket, Key=dkey, UploadId=uid)
            except Exception:
                pass
            raise
        self.dst_rl.acquire()
        h = self.dst.head_object(Bucket=it.dst_bucket, Key=dkey)
        if int(h["ContentLength"]) != size:
            raise IntegrityError(f"dest size {h['ContentLength']} != {size}")

    # ------------------------------------------------------------- main loop
    def run(self, *, transfer: bool = True, verify: Optional[bool] = None) -> dict:
        verify = self.cfg.verify.after_run if verify is None else verify
        store = self.store = self._open_store()
        run_id = store.start_run({"transfer": transfer, "verify": verify, "rescan": self.rescan,
                                  "stop_after": self.stop_after, "workers": self.cfg.transfer.workers})
        if self.retry_failed:
            n = store.reset_failed()
            if n:
                log.info("re-queued %d previously failed objects", n)
        if self.rescan:
            store.reset_scans()
        store.commit()
        old_handlers = self._install_signals()
        self.started = time.time()
        status = "failed"
        summary: dict = {}
        try:
            self.phase = "planning"
            self._heartbeat("listing top-level folders", force=True)
            self._plan_shards(store)
            summary = self._transfer_loop(store, transfer=transfer)
            interrupted = self.stop.is_set()
            if verify and not interrupted and transfer:
                self.phase = "verifying"
                self._write_progress(store)
                summary["verify"] = self.verify(store)
            status = self._final_status(summary, interrupted, transfer)
            self.phase = status
            summary["status"] = status
            self._write_progress(store, final=summary)
            return summary
        finally:
            self.stop.set()
            store.end_run(run_id, status, summary)
            store.close()
            self._restore_signals(old_handlers)

    def _final_status(self, s: dict, interrupted: bool, transfer: bool) -> str:
        if interrupted:
            return "interrupted"
        if not transfer:
            return "planned" if s.get("scan_errors", 0) == 0 else "incomplete"
        v = s.get("verify")
        ok = (s["failed"] == 0 and s["pending"] == 0 and s.get("scan_errors", 0) == 0 and
              s["shards_scanned"] == s["shards_total"])
        if v is not None:
            ok = ok and v["missing"] == 0 and v["mismatched"] == 0
        return "complete" if ok else "incomplete"

    def _transfer_loop(self, store: StateStore, *, transfer: bool) -> dict:
        cfg = self.cfg
        shards = store.shards()
        self.shards_total = len(shards)
        self.shards_scanned = sum(1 for s in shards if s["status"] == "scanned")
        for s in shards:
            if s["status"] != "scanned":
                self.shard_q.put((s["id"], s["src_bucket"], s["prefix"], bool(s["delimited"]), s["last_key"]))
        c = store.counts()
        self.n = {k: v["objects"] for k, v in c.items()}
        self.b = {k: v["bytes"] for k, v in c.items()}
        self.matched = sum(self.n.values())
        self.matched_bytes = sum(self.b.values())
        self.listed = sum(s["listed"] for s in shards)
        self.shard_prog = {int(k): v for k, v in store.shard_counts().items()}
        self.item_shard: dict = {}
        self.transferred_bytes = 0
        self.run_completed = 0
        self.rates: deque = deque(maxlen=120)

        threads = []
        guard = threading.Thread(target=self._guard_loop, name="guard", daemon=True)
        guard.start()
        for i in range(min(cfg.transfer.scan_workers, max(1, self.shard_q.qsize()))):
            th = threading.Thread(target=self._scanner_loop, name=f"scan-{i}", daemon=True)
            th.start()
            threads.append(th)
        scanners = list(threads)
        workers = []
        if transfer:
            for i in range(cfg.transfer.workers):
                th = threading.Thread(target=self._worker_loop, name=f"work-{i}", daemon=True)
                th.start()
                workers.append(th)

        self.phase = "scanning" if self.shard_q.qsize() else "transferring"
        dispatched_upto = 0
        self.inflight = 0
        results: list = []
        errs: list = []
        ops, last_commit, last_prog, last_sample = 0, time.monotonic(), 0.0, 0.0
        drained_pending = False

        while True:
            # 1) drain inbox
            deadline = time.monotonic() + 0.25
            while time.monotonic() < deadline:
                try:
                    msg = self.inbox.get(timeout=0.05)
                except queue.Empty:
                    break
                ops += self._handle(store, msg, results, errs)
            # 2) commit by size OR time (never let a slow trickle sit uncommitted)
            now = time.monotonic()
            if results or errs or ops:
                if ops >= 500 or now - last_commit >= 1.0:
                    if results:
                        store.apply_results(results)
                        results.clear()
                    if errs:
                        store.add_errors(errs)
                        errs.clear()
                    store.commit()
                    ops, last_commit = 0, now
            # 3) feed workers
            if transfer and not self.stop.is_set():
                free = self.work_q.maxsize - self.work_q.qsize()
                if free >= self.work_q.maxsize // 2:
                    rows = store.fetch_pending(dispatched_upto, free)
                    for r in rows:
                        self.work_q.put(WorkItem(r["id"], r["key"], r["size"], r["etag"] or "", r["mtime"] or 0,
                                                 r["attempts"], r["src_bucket"], r["dst_bucket"], r["dest_prefix"]))
                        dispatched_upto = r["id"]
                        self.inflight += 1
                        self.item_shard[r["id"]] = r["shard_id"]
                    drained_pending = not rows
            # 4) progress
            if now - last_prog >= 1.0:
                last_prog = now
                if self.phase == "scanning" and self.shards_scanned == self.shards_total:
                    self.phase = "transferring"
                self._write_progress(store)
                if now - last_sample >= 2.0:
                    last_sample = now
                    store.add_sample(time.time(), self.run_completed, self.transferred_bytes,
                                     self.src_rl.rate, self.dst_rl.rate)
            # 5) termination
            scanners_done = all(not t.is_alive() for t in scanners)
            if self.stop.is_set():
                if scanners_done and self.inflight <= 0 and self.inbox.empty():
                    break
                if scanners_done and transfer:
                    # drain queued-but-unstarted items so workers can exit
                    try:
                        while True:
                            itm = self.work_q.get_nowait()
                            if itm is not None:
                                self.inflight -= 1
                                self.item_shard.pop(itm.id, None)
                    except queue.Empty:
                        pass
                continue
            if scanners_done and self.inbox.empty():
                if not transfer:
                    break
                if self.inflight <= 0 and drained_pending and not results:
                    if not store.fetch_pending(dispatched_upto, 1):
                        break
        # shutdown
        for _ in workers:
            self.work_q.put(None)
        for th in workers:
            th.join(timeout=120)
        while not self.inbox.empty():
            ops += self._handle(store, self.inbox.get_nowait(), results, errs)
        if results:
            store.apply_results(results)
        if errs:
            store.add_errors(errs)
        store.add_sample(time.time(), self.run_completed, self.transferred_bytes, self.src_rl.rate, self.dst_rl.rate)
        store.commit()
        return self._summary()

    def _handle(self, store: StateStore, msg: tuple, results: list, errs: list) -> int:
        kind = msg[0]
        if kind == "page":
            _, sid, rows, last_key, listed, st = msg
            ins = store.shard_page(sid, rows, last_key, listed)
            store.shard_stats(sid, st)
            self.listed += listed
            self.matched += ins
            self.n["pending"] += ins
            added = sum(r[1] for r in rows) if ins == len(rows) else 0
            self.matched_bytes += added
            self.b["pending"] += added
            return 1 + len(rows) // 100
        if kind == "shard_done":
            store.shard_done(msg[1])
            self.shards_scanned += 1
            return 1
        if kind == "error":
            _, bucket, key, attempt, text = msg
            errs.append((time.time(), bucket, key, attempt, text))
            return 1
        if kind == "result":
            _, oid, st, attempts, err, size, etag, moved_bytes, psize = msg
            self.inflight -= 1
            sid = self.item_shard.pop(oid, None)
            results.append((oid, st, attempts, err, size, etag))
            if st != PENDING and sid is not None:
                sp = self.shard_prog.setdefault(sid, [0, 0])
                sp[0 if st in COMPLETE else 1] += 1
            if st != PENDING:
                name = {DONE: "copied", SKIPPED: "skipped_existing", MOVED: "moved", FAILED: "failed"}[st]
                self.n["pending"] -= 1
                self.b["pending"] -= psize
                self.n[name] += 1
                self.b[name] += psize
                if st in COMPLETE:
                    self.run_completed += 1
                self.transferred_bytes += moved_bytes
                if self.stop_after and self.run_completed >= self.stop_after and not self.stop.is_set():
                    log.warning("--stop-after %d reached, stopping gracefully", self.stop_after)
                    self.stop.set()
            return 1
        raise ValueError(kind)

    # --------------------------------------------------------------- progress
    def _summary(self) -> dict:
        n, b = self.n, self.b
        complete = n["copied"] + n["skipped_existing"] + n["moved"]
        return {
            "job": self.cfg.job_name,
            "mode": self.cfg.filter.mode,
            "action": self.cfg.transfer.action,
            "window": self.filter.window.describe(),
            "shards_total": self.shards_total,
            "shards_scanned": self.shards_scanned,
            "listed": self.listed,
            "matched": self.matched,
            "matched_bytes": self.matched_bytes,
            "copied": n["copied"],
            "moved": n["moved"],
            "skipped_existing": n["skipped_existing"],
            "failed": n["failed"],
            "pending": n["pending"],
            "complete": complete,
            "complete_bytes": b["copied"] + b["skipped_existing"] + b["moved"],
            "transferred_bytes_this_run": self.transferred_bytes,
            "completed_this_run": self.run_completed,
            "scan_errors": self.scan_errors,
            "elapsed_s": round(time.time() - self.started, 1),
        }

    def _write_progress(self, store: StateStore, final: Optional[dict] = None) -> None:
        s = final or self._summary()
        now = time.time()
        self.rates.append((now, self.run_completed, self.transferred_bytes))
        obj_s = mb_s = 0.0
        if len(self.rates) >= 2:
            t0, o0, b0 = self.rates[0]
            dt = max(now - t0, 1e-6)
            obj_s = (self.run_completed - o0) / dt
            mb_s = (self.transferred_bytes - b0) / dt / MB
        remaining = s["matched"] - s["complete"] - s["failed"]
        snap = dict(s)
        snap.update({
            "phase": self.phase,
            "updated": now,
            "started": self.started,
            "rate_obj_s": round(obj_s, 2),
            "rate_mb_s": round(mb_s, 2),
            "eta_s": round(remaining / obj_s) if obj_s > 0 and remaining > 0 else None,
            "inflight": max(self.inflight, 0) if hasattr(self, "inflight") else 0,
            "src_rps_limit": round(self.src_rl.rate, 1),
            "dst_rps_limit": round(self.dst_rl.rate, 1),
            "throttle_events": self.src_rl.throttle_events + self.dst_rl.throttle_events,
            "paused": not self.unpaused.is_set(),
            "pause_reason": self.pause_reason,
            "mem_budget_used_mb": round(self.budget.used / MB, 1),
            "mem_budget_max_mb": self.cfg.transfer.max_inflight_mb,
            "workers": self.cfg.transfer.workers,
            "source": self.cfg.source.endpoint,
            "destination": self.cfg.destination.endpoint,
            "src_type": self.cfg.source.type,
            "dst_type": self.cfg.destination.type,
            "pid": os.getpid(),
            "shard_progress": {str(k): v for k, v in getattr(self, "shard_prog", {}).items()},
        })
        store.write_progress(snap)
        store.commit()

    # ----------------------------------------------------------------- verify
    def verify(self, store: Optional[StateStore] = None) -> dict:
        """Streamed merge of expected keys (state DB, sorted) vs destination listing.

        O(1) memory regardless of object count: both sides are sorted by key
        (SQLite BINARY collation == S3 UTF-8 byte order).
        """
        own = store is None
        store = store or self._open_store()
        self.store = store
        self.phase = "verifying"
        tot = {"expected": 0, "ok": 0, "missing": 0, "mismatched": 0, "extra": 0, "samples": []}
        try:
            shards = store.shards()
            for i, sh in enumerate(shards, 1):
                self._heartbeat(f"verifying folder {i}/{len(shards)}: {sh['src_bucket']}/{sh['prefix'] or '(root)'}",
                                force=True)
                r = self._verify_shard(store, sh)
                for k in ("expected", "ok", "missing", "mismatched", "extra"):
                    tot[k] += r[k]
                tot["samples"].extend(r["samples"][: max(0, 20 - len(tot["samples"]))])
                store.save_verify(sh["id"], r["expected"], r["ok"], r["missing"], r["mismatched"],
                                  r["extra"], r["samples"])
                store.commit()
            tot["percent"] = round(100.0 * tot["ok"] / tot["expected"], 4) if tot["expected"] else 100.0
            return tot
        finally:
            if own:
                store.close()

    def _dest_keys(self, bucket: str, prefix: str, delimited: bool, strip: str) -> Iterator[tuple[str, int]]:
        token = None
        while True:
            kw = dict(Bucket=bucket, Prefix=prefix, MaxKeys=self.cfg.throttle.list_page_size)
            if delimited:
                kw["Delimiter"] = "/"
            if token:
                kw["ContinuationToken"] = token
            resp = self._with_retry(lambda: self.dst.list_objects_v2(**kw), self.dst_rl, what=f"verify-list {bucket}")
            for o in resp.get("Contents", []) or []:
                yield o["Key"][len(strip):], int(o["Size"])
            if not resp.get("IsTruncated"):
                return
            token = resp.get("NextContinuationToken")

    def _verify_shard(self, store: StateStore, sh) -> dict:
        """Destination listing is spooled to an on-disk table and joined in SQL.

        This does not depend on the destination returning keys in S3 byte order
        (SeaweedFS walks its filer directory tree, so 'a-c' and 'a/b' can come
        back in a different order than MinIO returns them).
        """
        c = store.conn
        sid = sh["id"]
        c.execute("CREATE TABLE IF NOT EXISTS dest_listing(shard_id INTEGER, key TEXT, size INTEGER, "
                  "PRIMARY KEY(shard_id, key)) WITHOUT ROWID")
        c.execute("DELETE FROM dest_listing WHERE shard_id=?", (sid,))
        dp = sh["dest_prefix"]
        batch = []
        seen = 0
        for key, size in self._dest_keys(sh["dst_bucket"], dp + sh["prefix"], bool(sh["delimited"]), dp):
            batch.append((sid, key, size))
            seen += 1
            if seen % 1000 == 0:
                self._heartbeat(f"verifying {sh['src_bucket']}/{sh['prefix'] or '(root)'}: "
                                f"{seen:,} destination keys listed")
            if len(batch) >= 5000:
                c.executemany("INSERT OR REPLACE INTO dest_listing VALUES(?,?,?)", batch)
                batch.clear()
        if batch:
            c.executemany("INSERT OR REPLACE INTO dest_listing VALUES(?,?,?)", batch)
        store.commit()
        q = lambda sql: c.execute(sql, (sid,)).fetchone()[0]
        expected = q("SELECT COUNT(*) FROM objects WHERE shard_id=?")
        # expected destination key (folder objects may carry the suffix on SeaweedFS)
        if self.rename_folder_objects:
            sfx = self.cfg.transfer.folder_object_suffix.replace("'", "''")
            n = len(self.cfg.transfer.folder_object_suffix)
            # expressions are on the *driving* side so the (shard_id, key) indexes stay usable
            ek = f"(CASE WHEN substr(o.key, -1)='/' AND o.size>0 THEN o.key || '{sfx}' ELSE o.key END)"
            rk = (f"(CASE WHEN substr(d.key, -{n + 1})='/{sfx}' THEN substr(d.key, 1, length(d.key)-{n}) "
                  f"ELSE d.key END)")
        else:
            ek, rk = "o.key", "d.key"
        ok = q("SELECT COUNT(*) FROM objects o JOIN dest_listing d ON d.shard_id=o.shard_id AND d.key=" + ek + " "
               "WHERE o.shard_id=? AND d.size=o.size")
        J = ("FROM objects o LEFT JOIN dest_listing d ON d.shard_id=o.shard_id AND d.key=" + ek + " "
             "WHERE o.shard_id=? AND ")
        missing = q("SELECT COUNT(*) " + J + "d.key IS NULL")
        mismatched = q("SELECT COUNT(*) " + J + "d.key IS NOT NULL AND d.size<>o.size")
        extra = q("SELECT COUNT(*) FROM dest_listing d LEFT JOIN objects o ON o.shard_id=d.shard_id "
                  "AND o.key=" + rk + " WHERE d.shard_id=? AND o.key IS NULL")
        samples = [{"key": r[0], "problem": "missing at destination"}
                   for r in c.execute("SELECT o.key " + J + "d.key IS NULL LIMIT 30", (sid,))]
        samples += [{"key": r[0], "problem": f"size {r[2]} != {r[1]}"}
                    for r in c.execute("SELECT o.key, o.size, d.size " + J +
                                       "d.key IS NOT NULL AND d.size<>o.size LIMIT 20", (sid,))]
        c.execute("DELETE FROM dest_listing WHERE shard_id=?", (sid,))
        return {"expected": expected, "ok": ok, "missing": missing,
                "mismatched": mismatched, "extra": extra, "samples": samples}

    # ---------------------------------------------------------------- signals
    def _install_signals(self):
        if not self.install_signals or threading.current_thread() is not threading.main_thread():
            return None
        old = {}

        def handler(signum, frame):
            if self.stop.is_set():
                log.error("second interrupt -> exiting immediately (state is still consistent)")
                os._exit(130)
            log.warning("interrupt received: finishing in-flight objects, then saving state "
                        "(Ctrl+C again to abort immediately)")
            self.stop.set()

        for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None), getattr(signal, "SIGBREAK", None)):
            if sig is not None:
                try:
                    old[sig] = signal.signal(sig, handler)
                except (ValueError, OSError):
                    pass
        return old

    def _restore_signals(self, old) -> None:
        for sig, h in (old or {}).items():
            try:
                signal.signal(sig, h)
            except (ValueError, OSError):
                pass
