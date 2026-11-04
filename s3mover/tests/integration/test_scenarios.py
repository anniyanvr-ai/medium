"""End-to-end scenarios against real S3 servers.

    dest = MinIO-compatible (moto server #2)  |  SeaweedFS (real weed server)
    mode = full | since 2025-06-01 | between 2025-06-01 .. 2025-08-31

Every scenario asserts 100 % transfer: the destination key set equals the
independently computed expected set, every object's bytes are identical
(MD5 of full content), and nothing outside the window was copied.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone

import pytest
import yaml

from conftest import ROOT, list_all, make_cfg, md5
from s3mover.config import config_from_dict
from s3mover.engine import Engine, FingerprintMismatch

pytestmark = pytest.mark.integration

JUNE1 = datetime(2025, 6, 1, tzinfo=timezone.utc)
SEP1 = datetime(2025, 9, 1, tzinfo=timezone.utc)

DATES = [
    datetime(2025, 1, 15, 10, 0, tzinfo=timezone.utc),
    datetime(2025, 5, 31, 23, 59, 59, tzinfo=timezone.utc),   # 1 s before since/between start
    datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc),       # exactly the start -> included
    datetime(2025, 6, 1, 0, 0, 1, tzinfo=timezone.utc),
    datetime(2025, 7, 15, 12, 0, tzinfo=timezone.utc),
    datetime(2025, 8, 31, 23, 59, 59, tzinfo=timezone.utc),   # last second of Aug 31 -> included
    datetime(2025, 9, 1, 0, 0, 0, tzinfo=timezone.utc),       # first second after -> excluded (between)
    datetime(2025, 12, 25, tzinfo=timezone.utc),
    datetime(2026, 3, 1, tzinfo=timezone.utc),
]

SPECIAL_KEYS = ["a-c.xml", "a/b.xml", "with space/ünïcødé é.xml", "deep/1/2/3/4/5/leaf.xml",
                "zero-byte.txt", "root-level.xml"]


def expected_for(mode: str, manifest: dict) -> dict:
    if mode == "full":
        return dict(manifest)
    if mode == "since":
        return {k: v for k, v in manifest.items() if v[1] >= JUNE1}
    return {k: v for k, v in manifest.items() if JUNE1 <= v[1] < SEP1}


def seed(src, bucket: str, n: int = 180, large: int = 2) -> dict:
    """Returns {key: (bytes, LastModified)}."""
    src.bucket(bucket)
    manifest = {}
    rnd = __import__("random").Random(bucket)
    for i in range(n):
        when = DATES[i % len(DATES)]
        key = f"epcis/{when:%Y/%m}/doc-{i:05d}.xml" if i % 4 else f"reports/{i:05d}.json"
        body = os.urandom(rnd.randint(1, 4096))
        src.put(bucket, key, body, when)
        manifest[key] = (body, when)
    for j, key in enumerate(SPECIAL_KEYS):
        when = DATES[(j * 2 + 3) % len(DATES)]
        body = b"" if key == "zero-byte.txt" else os.urandom(500 + j)
        src.put(bucket, key, body, when)
        manifest[key] = (body, when)
    for j in range(large):  # > multipart threshold (8 MB in tests) -> multipart path
        when = DATES[4 + j * 2]  # 2025-07-15 (in window), 2025-09-01 (outside "between")
        body = os.urandom(12 * 1024 * 1024 + 12345 * (j + 1))
        key = f"large/blob-{j}.bin"
        src.put(bucket, key, body, when)
        manifest[key] = (body, when)
    return manifest


def assert_100_percent(dst_client, dst_bucket: str, expected: dict) -> None:
    got = list_all(dst_client, dst_bucket)
    missing = set(expected) - set(got)
    unexpected = set(got) - set(expected)
    assert not missing, f"missing at destination: {sorted(missing)[:10]}"
    assert not unexpected, f"copied but outside the window: {sorted(unexpected)[:10]}"
    for key, (body, _) in expected.items():
        assert got[key] == len(body), key
        data = dst_client.get_object(Bucket=dst_bucket, Key=key)["Body"].read()
        assert md5(data) == md5(body), f"content differs: {key}"


def uniq(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


# ------------------------------------------------------------------ 6 scenarios
@pytest.mark.parametrize("mode,frm,to", [
    ("full", None, None),
    ("since", "2025-06-01", None),
    ("between", "2025-06-01", "2025-08-31"),
], ids=["1-full", "2-since-2025-06-01", "3-between-jun1-aug31"])
def test_scenario(tmp_path, moto_src, dest, mode, frm, to):
    sb, db = uniq("rts-src"), uniq("rts-dst")
    manifest = seed(moto_src, sb)
    exp = expected_for(mode, manifest)
    assert 0 < len(exp) <= len(manifest)
    cfg = config_from_dict(make_cfg(tmp_path, moto_src, dest, sb, db, mode=mode, frm=frm, to=to))
    s = Engine(cfg, install_signals=False).run()
    assert s["status"] == "complete", s
    assert s["matched"] == len(exp) and s["copied"] == len(exp) and s["failed"] == 0
    assert s["verify"]["percent"] == 100.0 and s["verify"]["missing"] == 0
    assert_100_percent(dest.client, db, exp)
    # source untouched in copy mode
    assert len(list_all(moto_src.client, sb)) == len(manifest)


# ------------------------------------------------------------------ resume
@pytest.mark.parametrize("mode,frm,to", [("full", None, None), ("since", "2025-06-01", None),
                                         ("between", "2025-06-01", "2025-08-31")])
def test_graceful_stop_and_resume_no_duplicates(tmp_path, moto_src, dest, mode, frm, to):
    sb, db = uniq("res-src"), uniq("res-dst")
    exp = expected_for(mode, seed(moto_src, sb))
    raw = make_cfg(tmp_path, moto_src, dest, sb, db, mode=mode, frm=frm, to=to, job="resume", workers=2)
    s1 = Engine(config_from_dict(raw), stop_after=25, install_signals=False).run()
    assert s1["status"] == "interrupted"
    assert 25 <= s1["completed_this_run"] < len(exp)
    s2 = Engine(config_from_dict(raw), install_signals=False).run()
    assert s2["status"] == "complete"
    # every object transferred exactly once across both runs
    assert s1["completed_this_run"] + s2["completed_this_run"] == len(exp)
    assert s2["skipped_existing"] == 0
    assert_100_percent(dest.client, db, exp)


def _write_yaml(tmp_path, raw) -> str:
    p = tmp_path / "cfg.yaml"
    p.write_text(yaml.safe_dump(raw))
    return str(p)


@pytest.mark.parametrize("mode,frm,to", [("full", None, None), ("between", "2025-06-01", "2025-08-31")])
def test_hard_kill_minus_9_then_resume(tmp_path, moto_src, dest, mode, frm, to):
    """SIGKILL in the middle of a run (no cleanup at all), then resume via CLI."""
    if os.name == "nt":
        pytest.skip("SIGKILL semantics test is POSIX-only")
    sb, db = uniq("kill-src"), uniq("kill-dst")
    exp = expected_for(mode, seed(moto_src, sb, n=400))
    raw = make_cfg(tmp_path, moto_src, dest, sb, db, mode=mode, frm=frm, to=to, job="kill", workers=2)
    raw["throttle"] = {"source_rps": 40, "dest_rps": 40}
    cfgp = _write_yaml(tmp_path, raw)
    cmd = [sys.executable, "-m", "s3mover", "run", "-c", cfgp, "--no-dashboard"]
    p = subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    dc = dest.client
    end = time.time() + 120
    while time.time() < end and len(list_all(dc, db)) < 30:
        time.sleep(0.3)
    p.send_signal(signal.SIGKILL)
    p.wait()
    partial = len(list_all(dc, db))
    assert 0 < partial < len(exp), "kill should land mid-transfer"
    raw["throttle"] = {"source_rps": 500, "dest_rps": 500}
    cfgp = _write_yaml(tmp_path, raw)
    r = subprocess.run(cmd[:5] + [cfgp, "--no-dashboard"], cwd=ROOT, capture_output=True, text=True, timeout=600)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "COMPLETE" in r.stdout
    assert_100_percent(dc, db, exp)


# ------------------------------------------------------------------ move
@pytest.mark.parametrize("mode,frm,to", [("since", "2025-06-01", None), ("between", "2025-06-01", "2025-08-31")])
def test_move_removes_only_moved_objects_from_source(tmp_path, moto_src, dest, mode, frm, to):
    sb, db = uniq("mv-src"), uniq("mv-dst")
    manifest = seed(moto_src, sb, n=90, large=1)
    exp = expected_for(mode, manifest)
    cfg = config_from_dict(make_cfg(tmp_path, moto_src, dest, sb, db, mode=mode, frm=frm, to=to, action="move"))
    s = Engine(cfg, install_signals=False).run()
    assert s["status"] == "complete" and s["moved"] == len(exp)
    assert_100_percent(dest.client, db, exp)
    left = list_all(moto_src.client, sb)
    assert set(left) == set(manifest) - set(exp)  # outside-window objects stay in MinIO


# ------------------------------------------------------------------ misc
def test_rescan_picks_up_only_new_objects(tmp_path, moto_src, moto_dst):
    sb, db = uniq("re-src"), uniq("re-dst")
    seed(moto_src, sb, n=40, large=0)
    raw = make_cfg(tmp_path, moto_src, moto_dst, sb, db, mode="since", frm="2025-06-01")
    s1 = Engine(config_from_dict(raw), install_signals=False).run()
    moto_src.put(sb, "epcis/2025/10/late-arrival.xml", b"new", datetime(2025, 10, 1, tzinfo=timezone.utc))
    moto_src.put(sb, "epcis/2025/04/old.xml", b"old", datetime(2025, 4, 1, tzinfo=timezone.utc))
    s2 = Engine(config_from_dict(raw), rescan=True, install_signals=False).run()
    assert s2["status"] == "complete"
    assert s2["completed_this_run"] == 1 and s2["matched"] == s1["matched"] + 1
    assert "epcis/2025/10/late-arrival.xml" in list_all(moto_dst.client, db)
    assert "epcis/2025/04/old.xml" not in list_all(moto_dst.client, db)


def test_existing_identical_objects_are_skipped(tmp_path, moto_src, moto_dst):
    sb, db = uniq("sk-src"), uniq("sk-dst")
    seed(moto_src, sb, n=30, large=0)
    raw = make_cfg(tmp_path, moto_src, moto_dst, sb, db)
    Engine(config_from_dict(raw), install_signals=False).run()
    raw["job"]["name"] = "second-job"  # new job, same data already at destination
    s = Engine(config_from_dict(raw), install_signals=False).run()
    assert s["status"] == "complete" and s["copied"] == 0 and s["skipped_existing"] == s["matched"]


def test_changed_filter_requires_new_job(tmp_path, moto_src, moto_dst):
    sb = uniq("fp-src")
    seed(moto_src, sb, n=5, large=0)
    raw = make_cfg(tmp_path, moto_src, moto_dst, sb, uniq("fp-dst"), mode="since", frm="2025-06-01")
    Engine(config_from_dict(raw), install_signals=False).run(transfer=False)
    raw["filter"]["from"] = "2025-07-01"
    with pytest.raises(FingerprintMismatch):
        Engine(config_from_dict(raw), install_signals=False).run()
    s = Engine(config_from_dict(raw), fresh=True, install_signals=False).run()
    assert s["status"] == "complete"


def test_key_pattern_date_source(tmp_path, moto_src, moto_dst):
    sb, db = uniq("kp-src"), uniq("kp-dst")
    moto_src.bucket(sb)
    now = datetime(2026, 10, 1, tzinfo=timezone.utc)  # all uploaded "today"
    for k in ["epcis/2025/05/31/a.xml", "epcis/2025/06/01/b.xml", "epcis/2025/08/31/c.xml",
              "epcis/2025/09/01/d.xml", "epcis/nodate/e.xml"]:
        moto_src.put(sb, k, b"x", now)
    raw = make_cfg(tmp_path, moto_src, moto_dst, sb, db, mode="between", frm="2025-06-01", to="2025-08-31")
    raw["filter"]["date_source"] = "key_pattern"
    s = Engine(config_from_dict(raw), install_signals=False).run()
    assert s["status"] == "complete"
    assert set(list_all(moto_dst.client, db)) == {"epcis/2025/06/01/b.xml", "epcis/2025/08/31/c.xml"}


def test_plan_is_a_dry_run(tmp_path, moto_src, moto_dst):
    sb, db = uniq("pl-src"), uniq("pl-dst")
    m = seed(moto_src, sb, n=50, large=0)
    raw = make_cfg(tmp_path, moto_src, moto_dst, sb, db, mode="between", frm="2025-06-01", to="2025-08-31")
    s = Engine(config_from_dict(raw), install_signals=False).run(transfer=False)
    assert s["status"] == "planned" and s["matched"] == len(expected_for("between", m))
    assert list_all(moto_dst.client, db) == {}


def test_cli_status_and_verify(tmp_path, moto_src, moto_dst):
    sb, db = uniq("cli-src"), uniq("cli-dst")
    exp = expected_for("since", seed(moto_src, sb, n=20, large=0))
    cfgp = _write_yaml(tmp_path, make_cfg(tmp_path, moto_src, moto_dst, sb, db, mode="since", frm="2025-06-01"))
    run = lambda *a: subprocess.run([sys.executable, "-m", "s3mover", *a, "-c", cfgp], cwd=ROOT,
                                    capture_output=True, text=True, timeout=300)
    assert run("run", "--no-dashboard").returncode == 0
    v = run("verify")
    assert v.returncode == 0 and '"percent": 100.0' in v.stdout
    st = run("status")
    assert st.returncode == 0 and f'"complete": {len(exp)}' in st.stdout
    # delete one object at the destination -> verify must catch it
    moto_dst.client.delete_object(Bucket=db, Key=sorted(exp)[0])
    v = run("verify")
    assert v.returncode == 2 and "missing at destination" in v.stdout


# ------------------------------------------------------------------ folder objects
@pytest.mark.parametrize("mode,frm,to", [("full", None, None), ("since", "2025-06-01", None),
                                         ("between", "2025-06-01", "2025-08-31")])
def test_folder_objects_and_folders_created_later(tmp_path, moto_src, dest, mode, frm, to):
    """'inbound-files-archive/' holding 1 KB of data, an empty folder marker, and a top-level
    folder that only appears after the first run - all must arrive, byte-exact."""
    sb, db = uniq("fo-src"), uniq("fo-dst")
    moto_src.bucket(sb)
    july = datetime(2025, 7, 1, tzinfo=timezone.utc)
    data = os.urandom(1024)
    moto_src.put(sb, "inbound-files-archive/", data, july)            # folder key WITH 1 KB data
    moto_src.put(sb, "inbound-files-archive/f1.xml", b"a" * 300, july)
    moto_src.put(sb, "empty-folder/", b"", july)                       # plain zero-byte marker
    moto_src.put(sb, "old/x.xml", b"o", datetime(2025, 1, 1, tzinfo=timezone.utc))
    raw = make_cfg(tmp_path, moto_src, dest, sb, db, mode=mode, frm=frm, to=to)
    s1 = Engine(config_from_dict(raw), install_signals=False).run()
    assert s1["status"] == "complete", s1

    renamed = dest.type == "seaweedfs"
    fo_key = "inbound-files-archive/" + (".folder-object" if renamed else "")
    got = dest.client.get_object(Bucket=db, Key=fo_key)["Body"].read()
    assert got == data, "folder-object bytes must survive (SeaweedFS would store 0 bytes under 'x/')"
    listing = list_all(dest.client, db)
    assert "inbound-files-archive/f1.xml" in listing and "empty-folder/" in listing
    assert ("old/x.xml" in listing) == (mode == "full")

    # a new top-level folder appears after the job was planned -> --rescan must pick it up
    moto_src.put(sb, "inbound-files-new/doc.xml", b"n" * 50, datetime(2025, 7, 2, tzinfo=timezone.utc))
    s2 = Engine(config_from_dict(raw), rescan=True, install_signals=False).run()
    assert s2["status"] == "complete" and s2["completed_this_run"] == 1
    assert "inbound-files-new/doc.xml" in list_all(dest.client, db)


# ------------------------------------------------------------------ diagnostics + date folders
def test_zero_match_folders_explain_why(tmp_path, moto_src, moto_dst):
    """A folder that matches 0 objects must show they were all outside the window (and when)."""
    sb = uniq("why-src")
    moto_src.bucket(sb)
    for i in range(30):  # like "1KB/": everything written AFTER the window
        moto_src.put(sb, f"1KB/f{i}.bin", b"x" * 1024, datetime(2026, 10, 1, tzinfo=timezone.utc))
    for i in range(5):
        moto_src.put(sb, f"EPCIS/e{i}.xml", b"e", datetime(2025, 7, 1, tzinfo=timezone.utc))
    moto_src.put(sb, "EPCIS/old.xml", b"o", datetime(2025, 1, 1, tzinfo=timezone.utc))
    raw = make_cfg(tmp_path, moto_src, moto_dst, sb, uniq("why-dst"), mode="between", frm="2025-06-01", to="2025-08-31")
    Engine(config_from_dict(raw), install_signals=False).run(transfer=False)
    from s3mover.state import snapshot
    sh = {r["prefix"]: r for r in snapshot(tmp_path / "state" / "t.db")["shards"]}
    assert sh["1KB/"]["listed"] == 30 and sh["1KB/"]["matched"] == 0 and sh["1KB/"]["after_window"] == 30
    assert json.loads(sh["1KB/"]["months"]) == {"2026-10": 30}
    assert sh["EPCIS/"]["matched"] == 5 and sh["EPCIS/"]["before_window"] == 1


def test_date_prefixes_list_only_folders_inside_window(tmp_path, moto_src, moto_dst):
    """EPCIS/{yyyy}/{mm}/{dd}/ layout: only day folders inside the window are listed at all."""
    sb, db = uniq("dp-src"), uniq("dp-dst")
    moto_src.bucket(sb)
    now = datetime(2026, 10, 1, tzinfo=timezone.utc)  # upload time is irrelevant here
    days = ["2025/05/31", "2025/06/01", "2025/07/15", "2025/08/31", "2025/09/01"]
    for d in days:
        for i in range(3):
            moto_src.put(sb, f"EPCIS/{d}/doc{i}.xml", b"d", now)
    raw = make_cfg(tmp_path, moto_src, moto_dst, sb, db, mode="between", frm="2025-06-01", to="2025-08-31")
    raw["filter"]["date_source"] = "key_pattern"
    raw["buckets"][0]["date_prefixes"] = ["EPCIS/{yyyy}/{mm}/{dd}/"]
    s = Engine(config_from_dict(raw), install_signals=False).run()
    assert s["status"] == "complete" and s["shards_total"] == 92  # Jun 1 .. Aug 31 = 92 day folders
    assert s["listed"] == 9, "only the 3 in-window day folders contain objects; nothing else is listed"
    assert set(list_all(moto_dst.client, db)) == {f"EPCIS/{d}/doc{i}.xml" for d in days[1:4] for i in range(3)}


def test_key_date_folders_are_pruned_and_unpadded_days_match(tmp_path, moto_src, dest):
    """Real layout from the chpd bucket: <folder>/YYYY/MM/D/ with 1-digit days, a 1 KB folder
    object '17/', undated folders, everything uploaded long after the business date."""
    sb, db = uniq("chpd"), uniq("chpd-dst")
    moto_src.bucket(sb)
    uploaded = datetime(2026, 10, 1, tzinfo=timezone.utc)   # LastModified = load date, not business date
    exp = set()
    for month, days in ((10, range(1, 32)), (11, range(1, 31))):
        for d in days:
            k = f"1KB/2025/{month}/{d}/file-{d}.bin"
            moto_src.put(sb, k, b"k" * 1024, uploaded)
            if month == 11 and d <= 20:
                exp.add(k)
    for d in (2, 10, 25):
        k = f"inbound-files-Archive/2025/11/{d}/in-{d}.xml"
        moto_src.put(sb, k, b"a" * 200, uploaded)
        if d <= 20:
            exp.add(k)
    moto_src.put(sb, "inbound-files-Archive/2025/11/17/", b"f" * 1024, uploaded)  # folder object with data
    exp.add("inbound-files-Archive/2025/11/17/")
    moto_src.put(sb, "EPCIS/undated-in.xml", b"e", datetime(2025, 11, 5, tzinfo=timezone.utc))   # no key date -> LastModified
    exp.add("EPCIS/undated-in.xml")
    moto_src.put(sb, "EPCIS/undated-old.xml", b"e", datetime(2024, 1, 1, tzinfo=timezone.utc))
    moto_src.put(sb, "CHINA/x.xml", b"c", datetime(2024, 1, 1, tzinfo=timezone.utc))

    raw = make_cfg(tmp_path, moto_src, dest, sb, db, mode="between", frm="2025-11-01", to="2025-11-20")
    raw["filter"]["date_source"] = "key_pattern"
    eng = Engine(config_from_dict(raw), install_signals=False)
    s = eng.run()
    assert s["status"] == "complete", s
    assert eng.pruned_folders >= 2                  # 1KB/2025/10/ and 11/21..30 never listed
    assert s["listed"] < 40, f"October folders must not be listed (listed={s['listed']})"
    got = set(list_all(dest.client, db))
    if dest.type == "seaweedfs":
        exp = {k + ".folder-object" if k.endswith("/") else k for k in exp}
    assert got == exp


def test_only_selected_folders_with_date_pruning(tmp_path, moto_src, moto_dst):
    """--prefix 1KB/ + key dates: only 1KB/ in-window day folders are listed and copied."""
    sb, db = uniq("pf-src"), uniq("pf-dst")
    moto_src.bucket(sb)
    up = datetime(2026, 10, 1, tzinfo=timezone.utc)
    for d in (1, 2, 30):
        moto_src.put(sb, f"1KB/2025/11/{d}/a.bin", b"x", up)
        moto_src.put(sb, f"inbound-files-Archive/2025/11/{d}/a.xml", b"y", up)
    raw = make_cfg(tmp_path, moto_src, moto_dst, sb, db, mode="between", frm="2025-11-01", to="2025-11-10")
    raw["filter"]["date_source"] = "key_pattern"
    cfg = config_from_dict(raw, {"prefixes": ["1KB"]})
    s = Engine(cfg, install_signals=False).run()
    assert s["status"] == "complete" and s["listed"] == 2
    assert set(list_all(moto_dst.client, db)) == {"1KB/2025/11/1/a.bin", "1KB/2025/11/2/a.bin"}


@pytest.mark.parametrize("date_source", ["last_modified", "key_pattern"])
def test_files_directly_in_undated_folders(tmp_path, moto_src, moto_dst, date_source):
    """EPCIS/<name>.xml and inbound-files/<name>.xml (no date folders) are selected by LastModified,
    also when the job uses key_pattern for its date folders."""
    sb, db = uniq("ud-src"), uniq("ud-dst")
    moto_src.bucket(sb)
    aug, oct_ = datetime(2025, 8, 10, tzinfo=timezone.utc), datetime(2025, 10, 1, tzinfo=timezone.utc)
    exp = set()
    for folder in ("EPCIS/", "inbound-files/"):
        for i in range(5):
            k = f"{folder}urn-epc-{i:04d}-0614141.xml"
            moto_src.put(sb, k, b"x", aug)
            exp.add(k)
            moto_src.put(sb, f"{folder}late-{i}.xml", b"y", oct_)
    moto_src.put(sb, "1KB/2025/8/3/a.bin", b"z", oct_)        # dated folder, not selected by --prefix
    raw = make_cfg(tmp_path, moto_src, moto_dst, sb, db, mode="between", frm="2025-08-01", to="2025-08-31")
    raw["filter"]["date_source"] = date_source
    s = Engine(config_from_dict(raw, {"prefixes": ["EPCIS/", "inbound-files/"]}), install_signals=False).run()
    assert s["status"] == "complete" and s["matched"] == 10
    assert set(list_all(moto_dst.client, db)) == exp
