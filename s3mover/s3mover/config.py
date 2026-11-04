"""Configuration loading, validation and date-window parsing.

All settings live in one YAML file; a few can be overridden from the CLI
(mode / from / to / action / workers). Validation is strict: a bad config
fails fast before any request is sent to MinIO or SeaweedFS.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any, Optional

try:  # PyYAML when available; the built-in subset reader otherwise (e.g. Python 3.15, no wheels yet)
    import yaml as _yaml
except ImportError:  # pragma: no cover - depends on environment
    from . import miniyaml as _yaml

FILTER_MODES = ("full", "since", "between")
ACTIONS = ("copy", "move")
DATE_SOURCES = ("last_modified", "key_pattern")
ENDPOINT_TYPES = ("minio", "seaweedfs", "s3")


class ConfigError(ValueError):
    pass


# --------------------------------------------------------------------------
# time helpers
# --------------------------------------------------------------------------
_OFFSET_RE = re.compile(r"^(?:UTC)?([+-])(\d{2}):?(\d{2})$")


def parse_tz(name: str) -> tzinfo:
    """'UTC', '+05:30', 'UTC+05:30' or an IANA name like 'Asia/Kolkata'."""
    if not name or name.upper() in ("UTC", "Z"):
        return timezone.utc
    m = _OFFSET_RE.match(name.strip())
    if m:
        sign = 1 if m.group(1) == "+" else -1
        return timezone(sign * timedelta(hours=int(m.group(2)), minutes=int(m.group(3))))
    try:
        from zoneinfo import ZoneInfo  # stdlib; needs `tzdata` pkg on Windows
        return ZoneInfo(name)
    except Exception as exc:  # pragma: no cover - platform dependent message
        raise ConfigError(
            f"unknown timezone {name!r} ({exc}); use an offset like '+05:30' "
            "or `pip install tzdata` on Windows"
        ) from exc


def _parse_point(value: Any, tz: tzinfo, *, is_end: bool) -> datetime:
    """Parse a bound. Date-only END bounds cover the whole day (exclusive next midnight)."""
    if isinstance(value, datetime):
        dt = value
        date_only = False
    elif isinstance(value, date):
        dt = datetime(value.year, value.month, value.day)
        date_only = True
    else:
        s = str(value).strip()
        date_only = bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", s))
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ConfigError(f"invalid date {value!r}; use YYYY-MM-DD or ISO-8601") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    if is_end and date_only:
        dt = dt + timedelta(days=1)
    return dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class DateWindow:
    """Half-open UTC window [start, end). None = unbounded on that side."""

    start: Optional[datetime] = None
    end: Optional[datetime] = None
    end_inclusive: bool = False  # True when user gave an exact end timestamp

    def contains(self, ts: datetime) -> bool:
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if self.start is not None and ts < self.start:
            return False
        if self.end is not None:
            if self.end_inclusive:
                return ts <= self.end
            return ts < self.end
        return True

    def describe(self) -> str:
        if self.start is None and self.end is None:
            return "all objects"
        s = self.start.isoformat() if self.start else "-inf"
        e = self.end.isoformat() if self.end else "+inf"
        return f"[{s}, {e}{']' if self.end_inclusive else ')'} UTC"


# --------------------------------------------------------------------------
# config dataclasses
# --------------------------------------------------------------------------
@dataclass
class Endpoint:
    endpoint: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"
    type: str = "minio"
    verify_tls: bool = True
    connect_timeout: int = 10
    read_timeout: int = 300  # MinIO can take ~2 min for one listing page


@dataclass
class BucketMap:
    source: str
    destination: str = ""
    prefixes: list[str] = field(default_factory=list)  # explicit shard roots
    dest_prefix: str = ""  # prepended to every destination key
    auto_shard: bool = True  # split by top-level "folders" when prefixes empty
    # Date-organised folders: list ONLY the folders inside the date window, e.g.
    # "EPCIS/{yyyy}/{mm}/{dd}/" -> EPCIS/2025/06/01/ ... EPCIS/2025/08/31/ (since/between modes)
    date_prefixes: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.destination:
            self.destination = self.source


@dataclass
class FilterCfg:
    mode: str = "full"
    from_: Optional[str] = None
    to: Optional[str] = None
    timezone: str = "UTC"
    date_source: str = "last_modified"
    # "auto" = 2025/11/2/, 2025-08-15, 2025_08_15, 20250815, 15-08-2025 ... (see s3mover/dates.py)
    # or a custom regex with named groups y, m, d
    key_pattern: str = "auto"
    # key_pattern: what to do with objects whose key has NO date (e.g. CHINA/x.xml)
    key_date_fallback: str = "last_modified"     # last_modified | exclude
    # key_pattern + since/between: walk year/month/day folders and never list folders outside the window
    prune_date_folders: bool = True
    include_regex: Optional[str] = None
    exclude_regex: Optional[str] = None

    def window(self) -> DateWindow:
        tz = parse_tz(self.timezone)
        if self.mode == "full":
            return DateWindow()
        start = _parse_point(self.from_, tz, is_end=False)
        if self.mode == "since":
            return DateWindow(start=start)
        end_is_date = bool(re.fullmatch(r"\s*\d{4}-\d{2}-\d{2}\s*", str(self.to))) or (
            isinstance(self.to, date) and not isinstance(self.to, datetime)
        )
        end = _parse_point(self.to, tz, is_end=True)
        return DateWindow(start=start, end=end, end_inclusive=not end_is_date)


@dataclass
class TransferCfg:
    action: str = "copy"
    workers: int = 8
    scan_workers: int = 2
    multipart_threshold_mb: int = 16
    part_size_mb: int = 8
    max_inflight_mb: int = 256
    skip_existing: bool = True
    preserve_metadata: bool = True
    max_retries: int = 5
    retry_base_delay: float = 1.0
    # Objects whose key ends with "/" but contain data (e.g. "inbound-files-archive/", 1 KB).
    # SeaweedFS turns such a key into a directory and silently stores 0 bytes, so by default
    # (auto) they are written to <key> + folder_object_suffix on SeaweedFS and kept as-is on MinIO.
    folder_objects: str = "auto"            # auto | keep | rename
    folder_object_suffix: str = ".folder-object"


@dataclass
class ThrottleCfg:
    source_rps: float = 100.0
    dest_rps: float = 100.0
    min_rps: float = 5.0
    adaptive: bool = True
    list_page_size: int = 1000
    pause_file: Optional[str] = None
    memory_guard_url: Optional[str] = None  # MinIO /minio/v2/metrics/cluster
    memory_guard_token: Optional[str] = None
    memory_guard_max_rss_mb: Optional[int] = None
    memory_guard_interval: int = 15


@dataclass
class VerifyCfg:
    after_run: bool = True


@dataclass
class DashboardCfg:
    enabled: bool = True
    host: str = "127.0.0.1"
    port: int = 8765


@dataclass
class Config:
    job_name: str
    state_dir: str
    source: Endpoint
    destination: Endpoint
    buckets: list[BucketMap]
    filter: FilterCfg = field(default_factory=FilterCfg)
    transfer: TransferCfg = field(default_factory=TransferCfg)
    throttle: ThrottleCfg = field(default_factory=ThrottleCfg)
    verify: VerifyCfg = field(default_factory=VerifyCfg)
    dashboard: DashboardCfg = field(default_factory=DashboardCfg)

    # ---- derived ---------------------------------------------------------
    @property
    def db_path(self) -> Path:
        return Path(self.state_dir) / f"{self.job_name}.db"

    def fingerprint(self) -> str:
        """Hash of everything that defines WHICH objects go WHERE.

        Resuming with a different fingerprint would mix two jobs in one
        state DB, so the engine refuses unless --fresh is given.
        """
        payload = {
            "src": self.source.endpoint,
            "dst": self.destination.endpoint,
            "buckets": [asdict(b) for b in self.buckets],
            "filter": asdict(self.filter),
            "action": self.transfer.action,
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:16]

    def validate(self) -> "Config":
        f, t = self.filter, self.transfer
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", self.job_name or ""):
            raise ConfigError("job.name must match [A-Za-z0-9_.-]+")
        if f.mode not in FILTER_MODES:
            raise ConfigError(f"filter.mode must be one of {FILTER_MODES}")
        if f.mode == "since":
            if not f.from_:
                raise ConfigError("filter.mode=since requires filter.from")
            if f.to:
                raise ConfigError("filter.mode=since must not set filter.to (use mode=between)")
        if f.mode == "between" and not (f.from_ and f.to):
            raise ConfigError("filter.mode=between requires filter.from and filter.to")
        if f.mode == "full" and (f.from_ or f.to):
            raise ConfigError("filter.mode=full must not set from/to")
        w = f.window()  # raises on bad dates / tz
        if w.start and w.end and w.start >= w.end:
            raise ConfigError(f"filter.from must be before filter.to ({w.describe()})")
        if f.date_source not in DATE_SOURCES:
            raise ConfigError(f"filter.date_source must be one of {DATE_SOURCES}")
        if f.key_date_fallback not in ("last_modified", "exclude"):
            raise ConfigError("filter.key_date_fallback must be last_modified or exclude")
        if f.date_source == "key_pattern" and f.key_pattern != "auto":
            rx = re.compile(f.key_pattern)
            if not {"y", "m", "d"} <= set(rx.groupindex):
                raise ConfigError("filter.key_pattern needs named groups y, m, d")
        for rx in (f.include_regex, f.exclude_regex):
            if rx:
                re.compile(rx)
        if t.action not in ACTIONS:
            raise ConfigError(f"transfer.action must be one of {ACTIONS}")
        if t.folder_objects not in ("auto", "keep", "rename"):
            raise ConfigError("transfer.folder_objects must be auto, keep or rename")
        if not t.folder_object_suffix or t.folder_object_suffix.endswith("/") or "/" in t.folder_object_suffix:
            raise ConfigError("transfer.folder_object_suffix must be a non-empty name without '/'")
        if not 1 <= t.workers <= 256:
            raise ConfigError("transfer.workers must be 1..256")
        if t.part_size_mb < 5:
            raise ConfigError("transfer.part_size_mb must be >= 5 (S3 minimum part size)")
        if t.multipart_threshold_mb < t.part_size_mb:
            raise ConfigError("transfer.multipart_threshold_mb must be >= part_size_mb")
        need = max(t.multipart_threshold_mb, t.part_size_mb)
        if t.max_inflight_mb < need:
            raise ConfigError(f"transfer.max_inflight_mb must be >= {need}")
        for ep in (self.source, self.destination):
            if ep.type not in ENDPOINT_TYPES:
                raise ConfigError(f"endpoint type must be one of {ENDPOINT_TYPES}")
            if not ep.endpoint.startswith(("http://", "https://")):
                raise ConfigError(f"endpoint must start with http(s):// ({ep.endpoint})")
        if not self.buckets:
            raise ConfigError("at least one bucket mapping is required")
        for b in self.buckets:
            if isinstance(b.date_prefixes, str):
                b.date_prefixes = [b.date_prefixes]
            for tpl in b.date_prefixes:
                if "{yyyy}" not in tpl:
                    raise ConfigError(f"date_prefixes template needs {{yyyy}}: {tpl!r}")
                if f.mode == "full":
                    raise ConfigError("date_prefixes needs mode since or between")
                if b.prefixes:
                    raise ConfigError("use either prefixes or date_prefixes for a bucket, not both")
        same_server = self.source.endpoint.rstrip("/") == self.destination.endpoint.rstrip("/")
        for b in self.buckets:
            if same_server and b.source == b.destination and not b.dest_prefix:
                raise ConfigError(
                    f"bucket {b.source}: source and destination are identical "
                    "(same endpoint, bucket and no dest_prefix)"
                )
        return self


# --------------------------------------------------------------------------
# loading
# --------------------------------------------------------------------------
def _build(cls, data: dict | None, path: str):
    data = dict(data or {})
    if cls is FilterCfg and "from" in data:
        data["from_"] = data.pop("from")
    known = set(cls.__dataclass_fields__)
    unknown = set(data) - known
    if unknown:
        raise ConfigError(f"unknown key(s) in {path}: {sorted(unknown)}")
    return cls(**data)


def load_config(path: str | Path, overrides: dict | None = None) -> Config:
    raw = _yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    return config_from_dict(raw, overrides, base_dir=Path(path).resolve().parent)


def config_from_dict(raw: dict, overrides: dict | None = None, base_dir: Path | None = None) -> Config:
    raw = json.loads(json.dumps(raw, default=str))  # deep copy, dates -> str
    overrides = {k: v for k, v in (overrides or {}).items() if v is not None}
    job = raw.get("job", {})
    flt = raw.setdefault("filter", {})
    trn = raw.setdefault("transfer", {})
    if "mode" in overrides:
        flt["mode"] = overrides["mode"]
        if overrides["mode"] == "full":
            flt.pop("from", None), flt.pop("to", None)
        if overrides["mode"] == "since":
            flt.pop("to", None)
    if "from" in overrides:
        flt["from"] = overrides["from"]
    if "to" in overrides:
        flt["to"] = overrides["to"]
    for k in ("action", "workers"):
        if k in overrides:
            trn[k] = overrides[k]
    for k in ("include_regex", "exclude_regex"):
        if k in overrides:
            flt[k] = overrides[k]
    if overrides.get("prefixes"):
        # only these folders of the (first / --src-bucket) bucket
        first = dict((raw.get("buckets") or [{}])[0])
        first["prefixes"] = [p if p.endswith("/") or not p else p + "/" for p in overrides["prefixes"]]
        first.pop("date_prefixes", None)
        raw["buckets"] = [first]
    if "src_bucket" in overrides or "dest_bucket" in overrides:
        # single-bucket shortcut from the CLI: replaces the buckets list
        first = (raw.get("buckets") or [{}])[0]
        src_b = overrides.get("src_bucket") or first.get("source")
        if not src_b:
            raise ConfigError("--dest-bucket needs --src-bucket (or a bucket in the config)")
        keep = {k: v for k, v in first.items() if k in ("prefixes", "dest_prefix", "auto_shard")}
        raw["buckets"] = [{**keep, "source": src_b, "destination": overrides.get("dest_bucket") or src_b}]
    state_dir = job.get("state_dir", "./state")
    if base_dir is not None and not Path(state_dir).is_absolute():
        state_dir = str((base_dir / state_dir).resolve())
    thr = raw.get("throttle") or {}
    if base_dir is not None and thr.get("pause_file") and not Path(thr["pause_file"]).is_absolute():
        thr["pause_file"] = str((base_dir / thr["pause_file"]).resolve())
        raw["throttle"] = thr
    name = overrides.get("job_name") or job.get("name")
    if not name:
        raise ConfigError("job.name is required")
    try:
        cfg = Config(
            job_name=name,
            state_dir=state_dir,
            source=_build(Endpoint, raw.get("source"), "source"),
            destination=_build(Endpoint, raw.get("destination"), "destination"),
            buckets=[_build(BucketMap, b, "buckets[]") for b in raw.get("buckets", [])],
            filter=_build(FilterCfg, flt, "filter"),
            transfer=_build(TransferCfg, trn, "transfer"),
            throttle=_build(ThrottleCfg, raw.get("throttle"), "throttle"),
            verify=_build(VerifyCfg, raw.get("verify"), "verify"),
            dashboard=_build(DashboardCfg, raw.get("dashboard"), "dashboard"),
        )
    except TypeError as exc:
        raise ConfigError(str(exc)) from exc
    return cfg.validate()
