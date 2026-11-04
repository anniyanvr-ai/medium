"""Object selection: date window + optional include/exclude regex."""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional

from .dates import find_key_date
from .config import DateWindow, FilterCfg, parse_tz


class ObjectFilter:
    def __init__(self, cfg: FilterCfg):
        self.cfg = cfg
        self.window: DateWindow = cfg.window()
        self.tz = parse_tz(cfg.timezone)
        self._inc = re.compile(cfg.include_regex) if cfg.include_regex else None
        self._exc = re.compile(cfg.exclude_regex) if cfg.exclude_regex else None
        self._key_rx = None
        if cfg.date_source == "key_pattern":
            self._key_rx = "auto" if cfg.key_pattern == "auto" else re.compile(cfg.key_pattern)

    @property
    def is_full(self) -> bool:
        return self.window.start is None and self.window.end is None

    def object_date(self, key: str, last_modified: datetime) -> Optional[datetime]:
        """The timestamp the window is applied to (None = undeterminable)."""
        if self._key_rx is None:
            return last_modified
        d = None
        if self._key_rx == "auto":
            ymd = find_key_date(key)
            if ymd:
                d = datetime(*ymd, tzinfo=self.tz)
        else:
            m = self._key_rx.search(key)
            if m:
                try:
                    d = datetime(int(m["y"]), int(m["m"]), int(m["d"]), tzinfo=self.tz)
                except ValueError:
                    d = None
        if d is None:
            return last_modified if self.cfg.key_date_fallback == "last_modified" else None
        return d.astimezone(timezone.utc)

    def classify(self, key: str, last_modified: datetime) -> tuple[str, Optional[datetime]]:
        """('match'|'before'|'after'|'regex'|'nodate', date used)."""
        if self._inc and not self._inc.search(key):
            return "regex", None
        if self._exc and self._exc.search(key):
            return "regex", None
        ts = last_modified if self.is_full and self._key_rx is None else self.object_date(key, last_modified)
        if self.is_full:
            return "match", ts
        if ts is None:
            return "nodate", None
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        w = self.window
        if w.start is not None and ts < w.start:
            return "before", ts
        if w.end is not None and (ts > w.end if w.end_inclusive else ts >= w.end):
            return "after", ts
        return "match", ts

    def matches(self, key: str, last_modified: datetime) -> bool:
        # keys ending in "/" ("folder" objects, with or without data) are filtered exactly
        # like any other object: by date / regex. Nothing is skipped silently.
        if self._inc and not self._inc.search(key):
            return False
        if self._exc and self._exc.search(key):
            return False
        if self.is_full:
            return True
        ts = self.object_date(key, last_modified)
        if ts is None:
            return False
        return self.window.contains(ts)
