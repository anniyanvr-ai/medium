"""Tiny YAML-subset reader used when PyYAML is not installed.

PyYAML has no pre-built wheels for brand-new Python releases (e.g. 3.15),
and building it needs a C compiler on Windows. s3mover config files only use
plain YAML: nested mappings, lists, comments, quoted / plain scalars and
simple flow lists like ["a", "b"]. This module parses exactly that, so the
tool runs on any Python 3.10+ with zero compiled dependencies.

Not supported (and not used by s3mover configs): anchors/aliases, multi-line
block scalars (| and >), multiple documents, tags.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any


class YAMLError(ValueError):
    pass


@dataclass
class _Line:
    no: int
    indent: int
    text: str


_INT = re.compile(r"[-+]?(0|[1-9][0-9_]*)$")
_FLOAT = re.compile(r"[-+]?([0-9][0-9_]*)?\.[0-9_]+([eE][-+]?[0-9]+)?$|[-+]?[0-9][0-9_]*[eE][-+]?[0-9]+$")
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}$")
_TS = re.compile(r"\d{4}-\d{1,2}-\d{1,2}([Tt]|[ \t]+)\d{1,2}:\d{2}:\d{2}(\.\d*)?([ \t]*(Z|[-+]\d{1,2}(:?\d{2})?))?$")


def _strip_comment(s: str) -> str:
    q = None
    i = 0
    while i < len(s):
        ch = s[i]
        if q:
            if q == "'" and ch == "'" and i + 1 < len(s) and s[i + 1] == "'":
                i += 2
                continue
            if q == '"' and ch == "\\":
                i += 2
                continue
            if ch == q:
                q = None
        elif ch in "'\"" and (i == 0 or s[i - 1] in " \t:[{,-"):
            q = ch
        elif ch == "#" and (i == 0 or s[i - 1] in " \t"):
            return s[:i].rstrip()
        i += 1
    return s.rstrip()


def _split_top(s: str, sep: str) -> list[str]:
    """Split on `sep` outside quotes and brackets."""
    parts, depth, q, cur = [], 0, None, []
    for ch in s:
        if q:
            cur.append(ch)
            if ch == q:
                q = None
            continue
        if ch in "'\"":
            q = ch
        elif ch in "[{":
            depth += 1
        elif ch in "]}":
            depth -= 1
        if ch == sep and depth == 0:
            parts.append("".join(cur))
            cur = []
            continue
        cur.append(ch)
    parts.append("".join(cur))
    return parts


def _key_split(text: str):
    """'key: value' -> (key, value) ; 'key:' -> (key, '') ; otherwise None."""
    q, depth = None, 0
    for i, ch in enumerate(text):
        if q:
            if ch == q:
                q = None
            continue
        if ch in "'\"" and i == 0:
            q = ch
        elif ch in "[{":
            depth += 1
        elif ch in "]}":
            depth -= 1
        elif ch == ":" and depth == 0 and (i + 1 == len(text) or text[i + 1] in " \t"):
            return _scalar(text[:i].strip()), text[i + 1:].strip()
    return None


def _unquote_double(s: str) -> str:
    try:
        return json.loads(s)
    except ValueError:
        return s[1:-1].encode().decode("unicode_escape")


def _scalar(s: str) -> Any:
    s = s.strip()
    if s == "" or s in ("~", "null", "Null", "NULL"):
        return None
    if s[0] == "'" and s.endswith("'") and len(s) >= 2:
        return s[1:-1].replace("''", "'")
    if s[0] == '"' and s.endswith('"') and len(s) >= 2:
        return _unquote_double(s)
    if s[0] == "[" and s.endswith("]"):
        inner = s[1:-1].strip()
        return [] if not inner else [_scalar(p) for p in _split_top(inner, ",")]
    if s[0] == "{" and s.endswith("}"):
        inner = s[1:-1].strip()
        out = {}
        for p in (_split_top(inner, ",") if inner else []):
            kv = _key_split(p.strip())
            if kv is None:
                raise YAMLError(f"bad flow mapping entry: {p!r}")
            out[kv[0]] = _scalar(kv[1])
        return out
    low = s.lower()
    if low in ("true", "yes", "on"):
        return True
    if low in ("false", "no", "off"):
        return False
    if _INT.match(s):
        return int(s.replace("_", ""))
    if _FLOAT.match(s):
        return float(s.replace("_", ""))
    if _DATE.match(s):
        try:
            return date.fromisoformat(s)
        except ValueError:
            return s
    if _TS.match(s):
        try:
            return datetime.fromisoformat(re.sub(r"[ \t]+", " ", s).replace("Z", "+00:00"))
        except ValueError:
            return s
    return s


def _check_supported(rest: str, no: int) -> None:
    if rest[:1] in ("|", ">", "&", "*", "!"):
        raise YAMLError(f"line {no}: unsupported YAML feature: {rest[:20]!r}")


class _Parser:
    def __init__(self, text: str):
        self.lines: list[_Line] = []
        for no, raw in enumerate(text.splitlines(), 1):
            if "\t" in raw[: len(raw) - len(raw.lstrip())]:
                raise YAMLError(f"line {no}: tabs are not allowed for indentation")
            body = _strip_comment(raw)
            if not body.strip() or body.strip() in ("---", "..."):
                continue
            stripped = body.lstrip(" ")
            if stripped[:1] in ("|", ">", "&", "*", "!"):
                raise YAMLError(f"line {no}: unsupported YAML feature: {stripped[:20]!r}")
            self.lines.append(_Line(no, len(body) - len(stripped), stripped))

    def parse(self) -> Any:
        if not self.lines:
            return None
        val, i = self._block(0, self.lines[0].indent)
        if i != len(self.lines):
            ln = self.lines[i]
            raise YAMLError(f"line {ln.no}: unexpected indentation")
        return val

    def _is_item(self, ln: _Line) -> bool:
        return ln.text == "-" or ln.text.startswith("- ")

    def _block(self, i: int, indent: int):
        return self._list(i, indent) if self._is_item(self.lines[i]) else self._map(i, indent)

    def _child(self, i: int, parent_indent: int, allow_same_indent_list: bool):
        """Value for a 'key:' with nothing after the colon."""
        if i < len(self.lines):
            nxt = self.lines[i]
            if nxt.indent > parent_indent or (allow_same_indent_list and nxt.indent == parent_indent
                                               and self._is_item(nxt)):
                return self._block(i, nxt.indent)
        return None, i

    def _map(self, i: int, indent: int):
        out: dict = {}
        while i < len(self.lines):
            ln = self.lines[i]
            if ln.indent < indent or (ln.indent == indent and self._is_item(ln)):
                break
            if ln.indent > indent:
                raise YAMLError(f"line {ln.no}: unexpected indentation")
            kv = _key_split(ln.text)
            if kv is None:
                raise YAMLError(f"line {ln.no}: expected 'key: value', got {ln.text!r}")
            key, rest = kv
            if key in out:
                raise YAMLError(f"line {ln.no}: duplicate key {key!r}")
            if rest == "":
                out[key], i = self._child(i + 1, indent, allow_same_indent_list=True)
            else:
                _check_supported(rest, ln.no)
                out[key] = _scalar(rest)
                i += 1
        return out, i

    def _list(self, i: int, indent: int):
        out: list = []
        while i < len(self.lines):
            ln = self.lines[i]
            if ln.indent != indent or not self._is_item(ln):
                if ln.indent > indent:
                    raise YAMLError(f"line {ln.no}: unexpected indentation")
                break
            rest = ln.text[1:].lstrip(" ")
            if rest == "":
                val, i = self._child(i + 1, indent, allow_same_indent_list=False)
                out.append(val)
                continue
            item_indent = indent + (len(ln.text) - len(rest))
            if _key_split(rest) is not None and rest[0] not in "[{":
                # "- key: value" starts a mapping whose keys align with `key`
                self.lines[i] = _Line(ln.no, item_indent, rest)
                val, i = self._map(i, item_indent)
                out.append(val)
            else:
                _check_supported(rest, ln.no)
                out.append(_scalar(rest))
                i += 1
        return out, i


def safe_load(text: str) -> Any:
    if hasattr(text, "read"):
        text = text.read()
    if text.lstrip()[:1] in ("{", "["):
        try:
            return json.loads(text)  # JSON is a subset of YAML
        except ValueError:
            pass
    return _Parser(text).parse()
