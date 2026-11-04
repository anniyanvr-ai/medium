"""State store + engine logic with fake S3 clients (no network)."""
import io
import threading

import pytest
from botocore.exceptions import ClientError

from s3mover.config import config_from_dict
from s3mover.engine import ByteBudget, Engine, WorkItem
from s3mover.state import DONE, FAILED, MOVED, PENDING, SKIPPED, StateStore, snapshot


def test_shard_page_and_resume_cursor(tmp_path):
    st = StateStore(tmp_path / "j.db")
    sid = st.add_shard("b", "b2", "p/", False, "")
    assert st.add_shard("b", "b2", "p/", False, "") == sid  # idempotent
    rows = [("p/a", 10, "e1", 1.0), ("p/b", 20, "e2", 2.0)]
    assert st.shard_page(sid, rows, "p/c", 3) == 2
    assert st.shard_page(sid, rows, "p/c", 3) == 0  # duplicates ignored
    st.commit()
    sh = st.shards()[0]
    assert sh["last_key"] == "p/c" and sh["listed"] == 6 and sh["matched"] == 2
    st.shard_done(sid)
    assert st.shards()[0]["status"] == "scanned"
    st.close()


def test_uncommitted_page_is_not_resumed_from(tmp_path):
    """A crash before commit must not advance the cursor (no gaps)."""
    st = StateStore(tmp_path / "j.db")
    sid = st.add_shard("b", "b", "", False, "")
    st.commit()
    st.shard_page(sid, [("k1", 1, "e", 0)], "k1", 1)
    st.conn.close()  # simulated crash: no commit
    st2 = StateStore(tmp_path / "j.db")
    assert st2.shards()[0]["last_key"] is None
    assert st2.fetch_pending(0, 10) == []
    st2.close()


def test_results_counts_and_reset_failed(tmp_path):
    st = StateStore(tmp_path / "j.db")
    sid = st.add_shard("b", "b", "", False, "")
    st.shard_page(sid, [(f"k{i}", i, "e", 0) for i in range(5)], "k4", 5)
    ids = [r["id"] for r in st.fetch_pending(0, 10)]
    st.apply_results([(ids[0], DONE, 1, None, None, None), (ids[1], SKIPPED, 1, None, None, None),
                      (ids[2], FAILED, 5, "boom", None, None), (ids[3], MOVED, 1, None, None, None)])
    st.commit()
    c = st.counts()
    assert c["copied"]["objects"] == 1 and c["failed"]["objects"] == 1 and c["pending"]["objects"] == 1
    assert st.shard_counts()[sid] == [3, 1]
    assert st.reset_failed() == 1
    assert len(st.fetch_pending(0, 10)) == 2
    st.close()


def test_snapshot_is_readonly(tmp_path):
    st = StateStore(tmp_path / "j.db")
    st.write_progress({"phase": "transferring", "matched": 3})
    st.commit()
    snap = snapshot(tmp_path / "j.db")
    assert snap["progress"]["matched"] == 3
    st.close()


def test_byte_budget_blocks_until_released():
    b = ByteBudget(100)
    entered = threading.Event()
    with b.reserve(80):
        t = threading.Thread(target=lambda: (b.reserve(50).__enter__(), entered.set()))
        t.start()
        assert not entered.wait(0.3)  # 80 + 50 > 100 -> must wait
    assert entered.wait(2)


# ------------------------------------------------------------- fake clients
def _err(code, status=503):
    return ClientError({"Error": {"Code": code, "Message": code},
                        "ResponseMetadata": {"HTTPStatusCode": status}}, "Op")


class FakeSrc:
    def __init__(self, data: dict):
        self.data = data
        self.deleted = []

    def get_object(self, Bucket, Key):
        if Key not in self.data:
            raise _err("NoSuchKey", 404)
        import hashlib
        b = self.data[Key]
        return {"Body": io.BytesIO(b), "ContentLength": len(b), "ETag": f'"{hashlib.md5(b).hexdigest()}"',
                "ContentType": "application/xml", "Metadata": {"orig": "1"}}

    def delete_object(self, Bucket, Key):
        self.deleted.append(Key)
        self.data.pop(Key, None)


class FakeDst:
    def __init__(self, fail_puts=0):
        self.objs = {}
        self.fail_puts = fail_puts
        self.puts = 0

    def head_object(self, Bucket, Key):
        if Key not in self.objs:
            raise _err("404", 404)
        b, meta, etag = self.objs[Key]
        return {"ContentLength": len(b), "ETag": f'"{etag}"', "Metadata": meta}

    def put_object(self, Bucket, Key, Body, Metadata=None, **kw):
        import hashlib
        self.puts += 1
        if self.fail_puts:
            self.fail_puts -= 1
            raise _err("SlowDown")
        self.objs[Key] = (Body, Metadata or {}, hashlib.md5(Body).hexdigest())
        return {"ETag": f'"{hashlib.md5(Body).hexdigest()}"'}


def _engine(tmp_path, action="copy"):
    cfg = config_from_dict({
        "job": {"name": "u", "state_dir": str(tmp_path)},
        "source": {"endpoint": "http://s:9000", "access_key": "a", "secret_key": "b"},
        "destination": {"endpoint": "http://d:9000", "access_key": "a", "secret_key": "b"},
        "buckets": [{"source": "b"}], "filter": {"mode": "full"},
        "transfer": {"action": action, "max_retries": 4, "retry_base_delay": 0.01},
        "throttle": {"source_rps": 1000, "dest_rps": 1000}})
    return Engine(cfg, install_signals=False)


def _item(key, size, etag):
    return WorkItem(1, key, size, etag, 0.0, 0, "b", "b", "")


def test_retry_on_slowdown_then_success_and_backoff(tmp_path):
    import hashlib
    e = _engine(tmp_path)
    e.src, e.dst = FakeSrc({"k": b"hello"}), FakeDst(fail_puts=2)
    oid, st, attempts, err, size, etag, moved, _ = e._process(_item("k", 5, hashlib.md5(b"hello").hexdigest()))
    assert st == DONE and attempts == 3 and err is None
    assert e.dst_rl.throttle_events == 2  # SlowDown triggered AIMD back-off
    assert e.dst.objs["k"][1]["s3mover-src-etag"] == hashlib.md5(b"hello").hexdigest()
    assert e.dst.objs["k"][1]["orig"] == "1"  # user metadata preserved


def test_gives_up_after_max_retries(tmp_path):
    e = _engine(tmp_path)
    e.src, e.dst = FakeSrc({"k": b"x"}), FakeDst(fail_puts=99)
    _, st, attempts, err, *_ = e._process(_item("k", 1, ""))
    assert st == FAILED and attempts == 4 and "SlowDown" in err


def test_skip_existing_when_dest_identical(tmp_path):
    import hashlib
    e = _engine(tmp_path)
    e.src, e.dst = FakeSrc({"k": b"abc"}), FakeDst()
    md = hashlib.md5(b"abc").hexdigest()
    e.dst.objs["k"] = (b"abc", {}, md)
    _, st, *_ = e._process(_item("k", 3, md))
    assert st == SKIPPED and e.dst.puts == 0


def test_move_deletes_source_only_after_verified_copy(tmp_path):
    import hashlib
    e = _engine(tmp_path, action="move")
    e.src, e.dst = FakeSrc({"k": b"abc"}), FakeDst()
    _, st, *_ = e._process(_item("k", 3, hashlib.md5(b"abc").hexdigest()))
    assert st == MOVED and e.src.deleted == ["k"] and "k" in e.dst.objs


def test_move_never_deletes_source_when_copy_fails(tmp_path):
    e = _engine(tmp_path, action="move")
    e.src, e.dst = FakeSrc({"k": b"abc"}), FakeDst(fail_puts=99)
    _, st, *_ = e._process(_item("k", 3, ""))
    assert st == FAILED and e.src.deleted == []


def test_move_resume_after_crash_between_delete_and_commit(tmp_path):
    """Source already deleted, destination has it -> recorded as MOVED, not FAILED."""
    import hashlib
    e = _engine(tmp_path, action="move")
    md = hashlib.md5(b"abc").hexdigest()
    e.src, e.dst = FakeSrc({}), FakeDst()
    e.dst.objs["k"] = (b"abc", {"s3mover-src-etag": md}, md)
    e.cfg.transfer.skip_existing = False
    _, st, *_ = e._process(_item("k", 3, md))
    assert st == MOVED


def test_corrupted_source_read_is_detected(tmp_path):
    e = _engine(tmp_path)

    class Liar(FakeSrc):
        def get_object(self, Bucket, Key):
            r = super().get_object(Bucket, Key)
            r["ETag"] = '"' + "0" * 32 + '"'  # server claims different content
            return r

    e.src, e.dst = Liar({"k": b"abc"}), FakeDst()
    e.cfg.transfer.max_retries = 1
    _, st, _, err, *_ = e._process(_item("k", 3, ""))
    assert st == FAILED and ("IntegrityError" in err or "md5" in err)
