"""Shared fixtures.

Source "MinIO"  : in-process moto S3 server, so tests can back-date
                  LastModified (real MinIO cannot) and hit the exact
                  June-1-2025 / Aug-31-2025 boundaries.
Dest "MinIO #2" : a separate moto server process (isolated storage).
Dest SeaweedFS  : real `weed server` (pip install seaweedfs-bin) or an
                  existing endpoint via S3MOVER_SEAWEED_ENDPOINT / _KEY / _SECRET.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import logging

import boto3
import pytest

logging.getLogger("werkzeug").setLevel(logging.WARNING)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# never route test traffic to localhost through a corporate proxy
os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost")
os.environ["no_proxy"] = os.environ["NO_PROXY"] + ",127.0.0.1,localhost"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_http(url: str, timeout: float = 60) -> None:
    import urllib.error
    import urllib.request
    end = time.time() + timeout
    while time.time() < end:
        try:
            urllib.request.urlopen(url, timeout=2)
            return
        except urllib.error.HTTPError:
            return  # any HTTP answer means it is up
        except Exception:
            time.sleep(0.3)
    raise RuntimeError(f"{url} did not come up")


def client(endpoint: str, key: str, secret: str):
    from s3mover.config import Endpoint
    from s3mover.s3client import make_client
    return make_client(Endpoint(endpoint=endpoint, access_key=key, secret_key=secret), 32)


@dataclass
class S3Target:
    name: str
    endpoint: str
    key: str
    secret: str
    type: str

    @property
    def client(self):
        return client(self.endpoint, self.key, self.secret)

    def as_cfg(self) -> dict:
        return {"endpoint": self.endpoint, "access_key": self.key, "secret_key": self.secret, "type": self.type}


# --------------------------------------------------------------- source
class MotoSource(S3Target):
    """moto server in this process; lets tests set LastModified."""

    def backend(self):
        from moto.s3.models import s3_backends
        return s3_backends[list(s3_backends)[0]]["global"]

    def put(self, bucket: str, key: str, body: bytes, when: datetime) -> None:
        c = self._c
        c.put_object(Bucket=bucket, Key=key, Body=body)
        k = self.backend().buckets[bucket].keys[key]
        k.last_modified = when.astimezone(timezone.utc).replace(tzinfo=None)

    def bucket(self, name: str) -> None:
        self._c.create_bucket(Bucket=name)


@pytest.fixture(scope="session")
def moto_src():
    from moto.server import ThreadedMotoServer
    port = free_port()
    srv = ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    srv.start()
    src = MotoSource("minio-src", f"http://127.0.0.1:{port}", "minioadmin", "minioadmin", "minio")
    src._c = src.client
    yield src
    srv.stop()


@pytest.fixture(scope="session")
def moto_dst(tmp_path_factory):
    """Second, isolated S3 server playing 'MinIO #2'."""
    port = free_port()
    proc = subprocess.Popen([sys.executable, "-m", "moto.server", "-H", "127.0.0.1", "-p", str(port)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait_http(f"http://127.0.0.1:{port}/")
    yield S3Target("minio-dst", f"http://127.0.0.1:{port}", "minioadmin", "minioadmin", "minio")
    proc.terminate()
    proc.wait(10)


@pytest.fixture(scope="session")
def seaweed(tmp_path_factory):
    ep = os.environ.get("S3MOVER_SEAWEED_ENDPOINT")
    if ep:
        yield S3Target("seaweedfs", ep, os.environ.get("S3MOVER_SEAWEED_KEY", "seaweedadmin"),
                       os.environ.get("S3MOVER_SEAWEED_SECRET", "seaweedadmin"), "seaweedfs")
        return
    weed = shutil.which("weed")
    if not weed:
        pytest.skip("SeaweedFS not available (pip install seaweedfs-bin or set S3MOVER_SEAWEED_ENDPOINT)")
    d = tmp_path_factory.mktemp("weed")
    (d / "s3.json").write_text(json.dumps({"identities": [{
        "name": "admin", "credentials": [{"accessKey": "seaweedadmin", "secretKey": "seaweedadmin"}],
        "actions": ["Admin", "Read", "Write", "List", "Tagging"]}]}))
    base = 20000 + (os.getpid() % 200) * 10
    ports = dict(master=base, volume=base + 1, filer=base + 2, s3=base + 3)
    proc = subprocess.Popen(
        [weed, "server", "-ip=127.0.0.1", f"-dir={d}", "-s3", f"-s3.config={d / 's3.json'}",
         f"-master.port={ports['master']}", f"-volume.port={ports['volume']}",
         f"-filer.port={ports['filer']}", f"-s3.port={ports['s3']}",
         f"-master.port.grpc={ports['master'] + 10000}", f"-volume.port.grpc={ports['volume'] + 10000}",
         f"-filer.port.grpc={ports['filer'] + 10000}", "-volume.max=50"],
        cwd=d, stdout=open(d / "weed.log", "w"), stderr=subprocess.STDOUT)
    ep = f"http://127.0.0.1:{ports['s3']}"
    target = S3Target("seaweedfs", ep, "seaweedadmin", "seaweedadmin", "seaweedfs")
    # wait until a real write succeeds (volume servers register a bit later than the S3 port)
    end = time.time() + 90
    c = target.client
    while True:
        try:
            c.create_bucket(Bucket="readiness-probe")
            c.put_object(Bucket="readiness-probe", Key="x", Body=b"x")
            break
        except Exception:
            if time.time() > end or proc.poll() is not None:
                proc.kill()
                pytest.skip("SeaweedFS failed to start: see " + str(d / "weed.log"))
            time.sleep(1)
    yield target
    proc.terminate()
    try:
        proc.wait(15)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.fixture(params=["minio", "seaweedfs"])
def dest(request, moto_dst):
    if request.param == "minio":
        return moto_dst
    return request.getfixturevalue("seaweed")


# --------------------------------------------------------------- helpers
def md5(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def list_all(c, bucket: str, prefix: str = "") -> dict[str, int]:
    out = {}
    p = c.get_paginator("list_objects_v2")
    try:
        for page in p.paginate(Bucket=bucket, Prefix=prefix):
            for o in page.get("Contents", []) or []:
                out[o["Key"]] = o["Size"]
    except c.exceptions.NoSuchBucket:
        return {}
    return out


def make_cfg(tmp_path, src: S3Target, dst: S3Target, src_bucket: str, dst_bucket: str, *,
             mode="full", frm=None, to=None, action="copy", job="t", tz="UTC", **transfer) -> dict:
    flt = {"mode": mode, "timezone": tz}
    if frm:
        flt["from"] = frm
    if to:
        flt["to"] = to
    tr = {"workers": 4, "multipart_threshold_mb": 8, "part_size_mb": 5, "max_inflight_mb": 64,
          "retry_base_delay": 0.05, "action": action}
    tr.update(transfer)
    return {
        "job": {"name": job, "state_dir": str(tmp_path / "state")},
        "source": src.as_cfg(),
        "destination": dst.as_cfg(),
        "buckets": [{"source": src_bucket, "destination": dst_bucket}],
        "filter": flt,
        "transfer": tr,
        "throttle": {"source_rps": 500, "dest_rps": 500},
        "dashboard": {"enabled": False},
    }
