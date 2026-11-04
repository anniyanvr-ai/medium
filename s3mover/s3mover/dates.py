"""Date detection in object keys and folder names.

Recognised (key_pattern: auto), in any folder or file name:
  2025/11/2/...      2025/08/15/...     (year / month / day folders, 1- or 2-digit)
  2025-08-15  2025_08_15  2025.08.15  20250815
  15-08-2025  15_08_2025  15.08.2025   (day first, 4-digit year last)
  month-level folders for pruning: 2025-08  2025_08  202508  08-2025
"""
from __future__ import annotations

import re
from datetime import date
from typing import Optional

_Y = r"(?P<y>(?:19|20)\d{2})"
AUTO_PATTERNS = [
    re.compile(rf"(?<!\d){_Y}/(?P<m>\d{{1,2}})/(?P<d>\d{{1,2}})(?!\d)"),
    re.compile(rf"(?<!\d){_Y}[-_.](?P<m>\d{{1,2}})[-_.](?P<d>\d{{1,2}})(?!\d)"),
    re.compile(rf"(?<!\d){_Y}(?P<m>0[1-9]|1[0-2])(?P<d>0[1-9]|[12]\d|3[01])(?!\d)"),
    re.compile(rf"(?<!\d)(?P<d>\d{{1,2}})[-_.](?P<m>\d{{1,2}})[-_.]{_Y}(?!\d)"),
]

_SEG_FULL = [re.compile(rf"^{_Y}[-_.](?P<m>\d{{1,2}})[-_.](?P<d>\d{{1,2}})$"),
             re.compile(rf"^{_Y}(?P<m>0[1-9]|1[0-2])(?P<d>0[1-9]|[12]\d|3[01])$"),
             re.compile(rf"^(?P<d>\d{{1,2}})[-_.](?P<m>\d{{1,2}})[-_.]{_Y}$")]
_SEG_MONTH = [re.compile(rf"^{_Y}[-_.](?P<m>\d{{1,2}})$"),
              re.compile(rf"^{_Y}(?P<m>0[1-9]|1[0-2])$"),
              re.compile(rf"^(?P<m>\d{{1,2}})[-_.]{_Y}$")]
_SEG_YEAR = re.compile(r"^(?:19|20)\d{2}$")


def _valid(y: int, m: int, d: int) -> bool:
    try:
        date(y, m, d)
        return True
    except ValueError:
        return False


def find_key_date(key: str, patterns=None) -> Optional[tuple[int, int, int]]:
    """Left-most valid date in the key (folders come before file names)."""
    best = None
    for rx in patterns or AUTO_PATTERNS:
        for m in rx.finditer(key):
            y, mo, d = int(m["y"]), int(m["m"]), int(m["d"])
            if _valid(y, mo, d):
                if best is None or m.start() < best[0]:
                    best = (m.start(), (y, mo, d))
                break
    return best[1] if best else None


def parse_segment(parts: tuple, seg: str) -> Optional[tuple]:
    """Extend a partial date (y[, m[, d]]) with one folder name; None if it is not a date level."""
    for rx in _SEG_FULL:
        m = rx.match(seg)
        if m:
            y, mo, d = int(m["y"]), int(m["m"]), int(m["d"])
            if _valid(y, mo, d) and tuple(parts) == (y, mo, d)[: len(parts)]:
                return (y, mo, d)
            return None
    if not parts:
        for rx in _SEG_MONTH:
            m = rx.match(seg)
            if m and 1 <= int(m["m"]) <= 12:
                return (int(m["y"]), int(m["m"]))
        if _SEG_YEAR.match(seg):
            return (int(seg),)
        return None
    if not seg.isdigit() or len(seg) > 2:
        return None
    n = int(seg)
    if len(parts) == 1 and 1 <= n <= 12:
        return parts + (n,)
    if len(parts) == 2 and _valid(parts[0], parts[1], n):
        return parts + (n,)
    return None
