from datetime import datetime, timezone

from s3mover.config import FilterCfg
from s3mover.filters import ObjectFilter
from s3mover.ratelimit import AdaptiveRateLimiter


def utc(*a):
    return datetime(*a, tzinfo=timezone.utc)


def test_filter_last_modified_between():
    f = ObjectFilter(FilterCfg(mode="between", from_="2025-06-01", to="2025-08-31"))
    assert f.matches("a.xml", utc(2025, 7, 1))
    assert not f.matches("a.xml", utc(2025, 9, 1))
    assert not f.matches("a.xml", utc(2025, 5, 31, 23, 59))


def test_filter_key_pattern_uses_path_date_not_mtime():
    f = ObjectFilter(FilterCfg(mode="since", from_="2025-06-01", date_source="key_pattern"))
    late, early = utc(2026, 1, 1), utc(2024, 1, 1)
    assert f.matches("epcis/2025/06/01/doc.xml", early)          # path date wins over LastModified
    assert not f.matches("epcis/2025/05/31/doc.xml", late)
    assert f.matches("epcis/2025-07-04-doc.xml", early)
    assert f.matches("1KB/2025/11/2/x.bin", early)                 # 1-digit day (MinIO console: 2025/11/2/)
    assert f.matches("1KB/2025/6/1/x.bin", early)                  # 1-digit month
    assert not f.matches("1KB/2025/5/31/x.bin", late)
    # no date in the key -> falls back to LastModified (default) ...
    assert f.matches("CHINA/undated.xml", late) and not f.matches("CHINA/undated.xml", early)
    assert f.matches("epcis/2025/13/45/doc.xml", late)            # invalid date -> fallback too
    # ... or is excluded
    g = ObjectFilter(FilterCfg(mode="since", from_="2025-06-01", date_source="key_pattern",
                               key_date_fallback="exclude"))
    assert not g.matches("CHINA/undated.xml", late)


def test_include_exclude_regex():
    f = ObjectFilter(FilterCfg(mode="full", include_regex=r"\.xml$", exclude_regex=r"/tmp/"))
    assert f.matches("a/b.xml", utc(2020, 1, 1))
    assert not f.matches("a/b.json", utc(2020, 1, 1))
    assert not f.matches("a/tmp/b.xml", utc(2020, 1, 1))


def test_folder_objects_use_the_same_date_rule():
    f = ObjectFilter(FilterCfg(mode="since", from_="2025-06-01"))
    assert f.matches("inbound-files-archive/", utc(2025, 7, 1))       # not skipped any more
    assert not f.matches("inbound-files-archive/", utc(2025, 5, 1))   # outside window
    assert ObjectFilter(FilterCfg(mode="full")).matches("dir/", utc(2020, 1, 1))


class FakeClock:
    def __init__(self):
        self.t = 0.0
        self.slept = 0.0

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += s
        self.slept += s


def test_token_bucket_enforces_rate():
    clk = FakeClock()
    rl = AdaptiveRateLimiter(10, clock=clk.now, sleep=clk.sleep)
    for _ in range(101):
        rl.acquire()
    # 100 tokens beyond the initial one at 10/s -> ~10 seconds
    assert 9.5 <= clk.slept <= 10.5


def test_aimd_halves_on_throttle_and_recovers():
    rl = AdaptiveRateLimiter(100, min_rate=5, recover_every=10)
    rl.on_throttle()
    assert rl.rate == 50
    for _ in range(5):
        rl.on_throttle()
    assert rl.rate == 5  # floor
    for _ in range(10 * 50):
        rl.on_success()
    assert rl.rate == 100  # back to ceiling, never above
    assert rl.throttle_events == 6


def test_non_adaptive_limiter_keeps_rate():
    rl = AdaptiveRateLimiter(100, adaptive=False)
    rl.on_throttle()
    assert rl.rate == 100
