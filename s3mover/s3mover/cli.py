"""Command line interface.

  python -m s3mover run       -c config.yaml [--mode full|since|between --from D --to D]
  python -m s3mover plan      -c config.yaml      # list + filter only (dry run), shows counts/bytes
  python -m s3mover verify    -c config.yaml      # re-verify destination against the job state
  python -m s3mover status    -c config.yaml      # print progress of a job (JSON)
  python -m s3mover dashboard -c config.yaml | --state-dir DIR

Exit codes: 0 complete/verified, 2 incomplete (failed or missing objects),
            3 interrupted (resume by running the same command again), 1 error.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from . import __version__
from .config import ConfigError, load_config
from .dashboard import serve
from .engine import Engine, FingerprintMismatch
from .state import snapshot

EXIT = {"complete": 0, "planned": 0, "incomplete": 2, "interrupted": 3}


def _human(n: float) -> str:
    for u in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or u == "TB":
            return f"{n:.1f} {u}" if u != "B" else f"{int(n)} B"
        n /= 1024
    return str(n)


def _setup_logging(cfg, verbose: bool) -> None:
    Path(cfg.state_dir).mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(threadName)-9s %(message)s")
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    sh = logging.StreamHandler(sys.stderr)
    sh.setFormatter(fmt)
    fh = logging.FileHandler(Path(cfg.state_dir) / f"{cfg.job_name}.log", encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(sh)
    root.addHandler(fh)
    for noisy in ("botocore", "boto3", "urllib3", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def _print_summary(s: dict) -> None:
    v = s.get("verify")
    lines = [
        f"job            : {s['job']}  ({s['action']}, mode={s['mode']})",
        f"window         : {s['window']}",
        f"status         : {s.get('status', '?').upper()}",
        f"shards scanned : {s['shards_scanned']}/{s['shards_total']}",
        f"listed         : {s['listed']:,}",
        f"matched filter : {s['matched']:,} objects, {_human(s['matched_bytes'])}",
        f"copied         : {s['copied']:,}",
        f"moved          : {s['moved']:,}",
        f"already present: {s['skipped_existing']:,}",
        f"failed         : {s['failed']:,}",
        f"pending        : {s['pending']:,}",
        f"this run       : {s['completed_this_run']:,} objects, {_human(s['transferred_bytes_this_run'])} "
        f"in {s['elapsed_s']}s",
    ]
    if v:
        lines.append(f"verify         : {v['ok']:,}/{v['expected']:,} OK ({v['percent']}%), "
                     f"missing={v['missing']}, size-mismatch={v['mismatched']}, other-at-dest={v['extra']}")
    print("\n".join(lines))


def _print_shards(db_path) -> None:
    """Per folder: how many objects were listed, matched, and why the rest were not."""
    from datetime import datetime, timezone
    day = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d") if t else "-"
    snap = snapshot(db_path, sample_window=0)
    print(f"\n{'folder':<40} {'listed':>10} {'matched':>9} {'before':>9} {'after':>9}  dates found")
    for r in snap["shards"]:
        name = f"{r['src_bucket']}/{r['prefix'] or '(root)'}"
        print(f"{name[:40]:<40} {r['listed']:>10,} {r['matched']:>9,} {r.get('before_window') or 0:>9,} "
              f"{r.get('after_window') or 0:>9,}  {day(r.get('min_mtime'))} -> {day(r.get('max_mtime'))}")
        try:
            months = json.loads(r.get("months") or "{}")
        except ValueError:
            months = {}
        if months and r["matched"] == 0 and r["listed"]:
            print("    0 matched; objects per month:", ", ".join(f"{k}: {v:,}" for k, v in months.items()))


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="s3mover", description="MinIO -> MinIO / SeaweedFS mover with date filters")
    ap.add_argument("--version", action="version", version=__version__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p, needs_cfg=True):
        p.add_argument("-c", "--config", required=needs_cfg, help="YAML config file")
        p.add_argument("--job-name", help="override job.name (separate state per job)")
        p.add_argument("--mode", choices=["full", "since", "between"])
        p.add_argument("--from", dest="from_", metavar="DATE", help="start (inclusive), e.g. 2025-06-01")
        p.add_argument("--to", metavar="DATE", help="end (date = whole day inclusive), e.g. 2025-08-31")
        p.add_argument("--action", choices=["copy", "move"])
        p.add_argument("--workers", type=int)
        p.add_argument("--src-bucket", help="override: copy only this source bucket")
        p.add_argument("--dest-bucket", help="override: destination bucket name (created if missing)")
        p.add_argument("--prefix", action="append", dest="prefixes", metavar="FOLDER",
                       help="only this folder, e.g. --prefix 1KB/ --prefix inbound-files-Archive/ (repeatable)")
        p.add_argument("--include", dest="include_regex", metavar="REGEX", help="only keys matching REGEX")
        p.add_argument("--exclude", dest="exclude_regex", metavar="REGEX", help="skip keys matching REGEX")
        p.add_argument("-v", "--verbose", action="store_true")

    for name in ("run", "plan"):
        p = sub.add_parser(name)
        common(p)
        p.add_argument("--fresh", action="store_true", help="discard previous state of this job")
        p.add_argument("--rescan", action="store_true", help="re-list source to pick up new objects")
        p.add_argument("--no-retry-failed", action="store_true")
        p.add_argument("--no-dashboard", action="store_true")
        p.add_argument("--dashboard-port", type=int)
        if name == "run":
            p.add_argument("--no-verify", action="store_true")
            p.add_argument("--stop-after", type=int, help="stop gracefully after N objects (testing)")
            p.add_argument("--keep-dashboard", action="store_true", help="keep serving after the run ends")

    p = sub.add_parser("verify")
    common(p)
    p = sub.add_parser("status")
    common(p)
    p = sub.add_parser("dashboard")
    common(p, needs_cfg=False)
    p.add_argument("--state-dir")
    p.add_argument("--host", default=None)
    p.add_argument("--port", type=int, default=None)
    return ap


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if args.cmd == "dashboard" and not args.config:
        if not args.state_dir:
            print("dashboard needs -c CONFIG or --state-dir", file=sys.stderr)
            return 1
        logging.basicConfig(level=logging.INFO)
        serve(args.state_dir, args.host or "127.0.0.1", args.port or 8765)
        return 0

    overrides = {"mode": args.mode, "from": args.from_, "to": args.to, "action": args.action,
                 "workers": args.workers, "job_name": args.job_name,
                 "src_bucket": args.src_bucket, "dest_bucket": args.dest_bucket,
                 "prefixes": args.prefixes, "include_regex": args.include_regex,
                 "exclude_regex": args.exclude_regex}
    try:
        cfg = load_config(args.config, overrides)
    except (ConfigError, OSError) as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 1

    if args.cmd == "dashboard":
        logging.basicConfig(level=logging.INFO)
        serve(args.state_dir or cfg.state_dir, args.host or cfg.dashboard.host, args.port or cfg.dashboard.port)
        return 0
    if args.cmd == "status":
        if not cfg.db_path.exists():
            print(f"no state for job {cfg.job_name} at {cfg.db_path}", file=sys.stderr)
            return 1
        snap = snapshot(cfg.db_path, sample_window=0)
        print(json.dumps({"meta": snap["meta"], "progress": snap["progress"]}, indent=2, default=str))
        return 0

    _setup_logging(cfg, args.verbose)
    log = logging.getLogger("s3mover")
    log.info("s3mover %s job=%s action=%s mode=%s window=%s", __version__, cfg.job_name,
             cfg.transfer.action, cfg.filter.mode, cfg.filter.window().describe())
    log.info("source=%s (%s) destination=%s (%s)", cfg.source.endpoint, cfg.source.type,
             cfg.destination.endpoint, cfg.destination.type)

    if args.cmd == "verify":
        eng = Engine(cfg)
        v = eng.verify()
        print(json.dumps({k: v[k] for k in ("expected", "ok", "missing", "mismatched", "extra", "percent")}))
        for smp in v["samples"][:20]:
            print(f"  {smp['problem']}: {smp['key']}")
        return 0 if v["missing"] == 0 and v["mismatched"] == 0 else 2

    httpd = None
    if cfg.dashboard.enabled and not args.no_dashboard:
        try:
            httpd = serve(cfg.state_dir, cfg.dashboard.host, args.dashboard_port or cfg.dashboard.port,
                          background=True)
            log.info("dashboard: http://%s:%d/?job=%s", cfg.dashboard.host,
                     args.dashboard_port or cfg.dashboard.port, cfg.job_name)
        except OSError as exc:
            log.warning("dashboard not started (%s) - run `s3mover dashboard` separately", exc)

    try:
        eng = Engine(cfg, fresh=args.fresh, rescan=args.rescan, retry_failed=not args.no_retry_failed,
                     stop_after=getattr(args, "stop_after", None))
        if args.cmd == "plan":
            summary = eng.run(transfer=False, verify=False)
        else:
            summary = eng.run(transfer=True, verify=not args.no_verify)
    except FingerprintMismatch as exc:
        log.error("%s", exc)
        return 1
    except Exception as exc:  # unreachable endpoint, bad credentials, missing bucket ...
        from botocore.exceptions import BotoCoreError, ClientError
        if isinstance(exc, (BotoCoreError, ClientError)):
            log.error("S3 error: %s", exc)
            log.error("check the endpoint / access_key / secret_key / bucket names in %s; "
                      "progress so far is saved - run the same command again to resume", args.config)
            return 1
        raise
    _print_summary(summary)
    try:
        _print_shards(cfg.db_path)
    except Exception:
        pass
    if httpd and getattr(args, "keep_dashboard", False):
        log.info("run finished; dashboard still serving (Ctrl+C to exit)")
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            pass
    return EXIT.get(summary.get("status"), 1)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
