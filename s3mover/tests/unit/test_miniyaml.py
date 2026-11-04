"""The built-in YAML reader must parse every shipped config exactly like PyYAML."""
from pathlib import Path

import pytest

from s3mover import miniyaml

yaml = pytest.importorskip("yaml")
ROOT = Path(__file__).resolve().parents[2]
FILES = [ROOT / "config.example.yaml", *sorted((ROOT / "configs").glob("*.yaml"))]


@pytest.mark.parametrize("path", FILES, ids=lambda p: p.name)
def test_shipped_configs_match_pyyaml(path):
    text = path.read_text(encoding="utf-8")
    assert miniyaml.safe_load(text) == yaml.safe_load(text)


SAMPLES = [
    "a: 1\nb: -2.5\nc: true\nd: off\ne: ~\nf:\ng: 2025-06-01\nh: '2025-06-01'\n",
    "key_pattern: '(?P<y>\\d{4})/(?P<m>\\d{2})'\nq: \"tab\\tx\"\ns: 'it''s'\n",
    "url: http://localhost:9000   # comment\nhash: a#b\n",
    "buckets:\n  - source: rts\n    destination: rts2\n    prefixes: [\"epcis/\", 'reports/']\n  - source: x\n",
    "buckets:\n- source: rts\n  dest_prefix: ''\n- source: y\n",
    "l: []\nm: {}\nn: {a: 1, b: x}\nnested:\n  deeper:\n    k: v\n  list:\n    - 1\n    - two\n",
    "ts: 2025-08-31T18:00:00\nplain: hello world\n",
]


@pytest.mark.parametrize("text", SAMPLES)
def test_samples_match_pyyaml(text):
    assert miniyaml.safe_load(text) == yaml.safe_load(text)


def test_json_is_accepted():
    assert miniyaml.safe_load('{"job": {"name": "x"}, "n": [1, 2]}') == {"job": {"name": "x"}, "n": [1, 2]}


@pytest.mark.parametrize("bad", ["a: 1\n   b: 2\n", "a: |\n  text\n", "x: &anchor 1\n", "a: 1\na: 2\n"])
def test_rejects_unsupported_or_invalid(bad):
    with pytest.raises(miniyaml.YAMLError):
        miniyaml.safe_load(bad)
