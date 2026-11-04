"""boto3 client factory tuned for MinIO / SeaweedFS."""
from __future__ import annotations

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, ConnectionError as BotoConnErr, ReadTimeoutError, EndpointConnectionError

from .config import Endpoint

THROTTLE_CODES = {"SlowDown", "ServiceUnavailable", "RequestTimeout", "InternalError",
                  "XMinioServerNotInitialized", "XMinioStorageFull", "503", "500", "TooManyRequests"}
NOT_FOUND_CODES = {"404", "NoSuchKey", "NotFound"}


def make_client(ep: Endpoint, pool: int):
    kwargs = dict(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
        retries={"max_attempts": 3, "mode": "standard"},
        connect_timeout=ep.connect_timeout,
        read_timeout=ep.read_timeout,
        max_pool_connections=max(10, pool),
        tcp_keepalive=True,
    )
    # boto3 >= 1.36 adds CRC32 trailers by default; older MinIO and
    # SeaweedFS releases reject/ignore them. Only send checksums when required.
    try:
        cfg = BotoConfig(request_checksum_calculation="when_required",
                         response_checksum_validation="when_required", **kwargs)
    except TypeError:  # older botocore
        cfg = BotoConfig(**kwargs)
    return boto3.client(
        "s3",
        endpoint_url=ep.endpoint,
        aws_access_key_id=ep.access_key,
        aws_secret_access_key=ep.secret_key,
        region_name=ep.region,
        verify=ep.verify_tls,
        config=cfg,
    )


def error_code(exc: BaseException) -> str:
    if isinstance(exc, ClientError):
        err = exc.response.get("Error", {})
        return str(err.get("Code") or exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", ""))
    return type(exc).__name__


def is_throttle(exc: BaseException) -> bool:
    if isinstance(exc, (BotoConnErr, ReadTimeoutError, EndpointConnectionError, ConnectionError, TimeoutError)):
        return True
    return error_code(exc) in THROTTLE_CODES


def is_not_found(exc: BaseException) -> bool:
    return isinstance(exc, ClientError) and error_code(exc) in NOT_FOUND_CODES
