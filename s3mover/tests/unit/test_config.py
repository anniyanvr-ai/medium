from datetime import date, datetime, timezone

import pytest

from s3mover.config import ConfigError, config_from_dict, parse_tz


def base(**flt):
    return {
        "job": {"name": "j1", "state_dir": "/tmp/x"},
        "source": {"endpoint": "http://minio:9000", "access_key": "a", "secret_key": "b"},
        "destination": {"endpoint": "http://sw:8333", "access_key": "a", "secret_key": "b", "type": "seaweedfs"},
        "buckets": [{"source": "rts"}],
        "filter": flt,
    }


def utc(*a):
    return datetime(*a, tzinfo=timezone.utc)


# ---------------------------------------------------------------- modes
def test_full_mode_has_no_window():
    w = config_from_dict(base(mode="full")).filter.window()
    assert w.start is None and w.end is None
    assert w.contains(utc(1999, 1, 1)) and w.contains(utc(2100, 1, 1))


def test_since_june_1_2025_is_inclusive_start():
    w = config_from_dict(base(**{"mode": "since", "from": "2025-06-01"})).filter.window()
    assert not w.contains(utc(2025, 5, 31, 23, 59, 59, 999999))
    assert w.contains(utc(2025, 6, 1, 0, 0, 0))
    assert w.contains(utc(2030, 1, 1))


def test_between_june_and_aug_31_includes_whole_last_day():
    w = config_from_dict(base(**{"mode": "between", "from": "2025-06-01", "to": "2025-08-31"})).filter.window()
    assert not w.contains(utc(2025, 5, 31, 23, 59, 59))
    assert w.contains(utc(2025, 6, 1))
    assert w.contains(utc(2025, 8, 31, 23, 59, 59, 999999))
    assert not w.contains(utc(2025, 9, 1, 0, 0, 0))


def test_between_with_exact_end_timestamp_is_inclusive():
    w = config_from_dict(base(**{"mode": "between", "from": "2025-06-01T00:00:00",
                                 "to": "2025-08-31T12:00:00"})).filter.window()
    assert w.contains(utc(2025, 8, 31, 12, 0, 0))
    assert not w.contains(utc(2025, 8, 31, 12, 0, 1))


def test_yaml_native_dates_are_accepted():
    w = config_from_dict(base(**{"mode": "between", "from": date(2025, 6, 1), "to": date(2025, 8, 31)})).filter.window()
    assert w.start == utc(2025, 6, 1) and w.end == utc(2025, 9, 1)


def test_timezone_ist_shifts_boundaries():
    w = config_from_dict(base(**{"mode": "since", "from": "2025-06-01", "timezone": "Asia/Kolkata"})).filter.window()
    assert w.start == utc(2025, 5, 31, 18, 30)
    w2 = config_from_dict(base(**{"mode": "since", "from": "2025-06-01", "timezone": "+05:30"})).filter.window()
    assert w2.start == w.start


@pytest.mark.parametrize("flt,msg", [
    ({"mode": "since"}, "requires filter.from"),
    ({"mode": "since", "from": "2025-06-01", "to": "2025-07-01"}, "must not set filter.to"),
    ({"mode": "between", "from": "2025-06-01"}, "requires filter.from and filter.to"),
    ({"mode": "between", "from": "2025-09-01", "to": "2025-06-01"}, "before"),
    ({"mode": "full", "from": "2025-06-01"}, "must not set"),
    ({"mode": "sometimes"}, "filter.mode"),
    ({"mode": "since", "from": "01/06/2025"}, "invalid date"),
    ({"mode": "full", "date_source": "key_pattern", "key_pattern": "(\\d{4})"}, "named groups"),
])
def test_invalid_filters_rejected(flt, msg):
    with pytest.raises(ConfigError, match=msg):
        config_from_dict(base(**flt))


def test_cli_overrides_switch_modes():
    raw = base(**{"mode": "between", "from": "2025-06-01", "to": "2025-08-31"})
    assert config_from_dict(raw, {"mode": "full"}).filter.window().start is None
    c = config_from_dict(raw, {"mode": "since"})
    assert c.filter.to is None and c.filter.window().end is None
    c = config_from_dict(base(mode="full"), {"mode": "between", "from": "2025-06-01", "to": "2025-06-30"})
    assert c.filter.window().end == utc(2025, 7, 1)


def test_unknown_keys_rejected():
    raw = base(mode="full")
    raw["transfer"] = {"wokers": 4}
    with pytest.raises(ConfigError, match="unknown key"):
        config_from_dict(raw)


def test_same_server_same_bucket_rejected():
    raw = base(mode="full")
    raw["destination"] = dict(raw["source"])
    with pytest.raises(ConfigError, match="identical"):
        config_from_dict(raw)
    raw["buckets"] = [{"source": "rts", "destination": "rts-archive"}]
    config_from_dict(raw)  # different bucket on the same MinIO is fine


def test_transfer_limits_validated():
    raw = base(mode="full")
    raw["transfer"] = {"part_size_mb": 4}
    with pytest.raises(ConfigError, match="part_size_mb"):
        config_from_dict(raw)
    raw["transfer"] = {"multipart_threshold_mb": 64, "max_inflight_mb": 32}
    with pytest.raises(ConfigError, match="max_inflight_mb"):
        config_from_dict(raw)


def test_fingerprint_changes_with_window_not_with_workers():
    a = config_from_dict(base(**{"mode": "since", "from": "2025-06-01"}))
    b = config_from_dict(base(**{"mode": "since", "from": "2025-06-02"}))
    c = config_from_dict(base(**{"mode": "since", "from": "2025-06-01"}), {"workers": 32})
    assert a.fingerprint() != b.fingerprint()
    assert a.fingerprint() == c.fingerprint()


def test_parse_tz_variants():
    assert parse_tz("UTC") is timezone.utc
    assert parse_tz("-03:00").utcoffset(None).total_seconds() == -3 * 3600
    with pytest.raises(ConfigError):
        parse_tz("Mars/Olympus")


def test_cli_bucket_overrides():
    c = config_from_dict(base(mode="full"), {"src_bucket": "epcis", "dest_bucket": "epcis-copy"})
    assert [(b.source, b.destination) for b in c.buckets] == [("epcis", "epcis-copy")]
    c = config_from_dict(base(mode="full"), {"dest_bucket": "rts-2025"})
    assert [(b.source, b.destination) for b in c.buckets] == [("rts", "rts-2025")]


def test_cli_prefix_and_regex_overrides():
    c = config_from_dict(base(mode="full"), {"src_bucket": "chpd", "dest_bucket": "test",
                                             "prefixes": ["1KB", "inbound-files-Archive/"],
                                             "include_regex": r"\.xml$"})
    b = c.buckets[0]
    assert (b.source, b.destination, b.prefixes) == ("chpd", "test", ["1KB/", "inbound-files-Archive/"])
    assert c.filter.include_regex == r"\.xml$"
