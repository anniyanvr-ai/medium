"""Thread-safe AIMD token bucket.

Every request to an endpoint takes one token. When the server signals
overload (503 SlowDown, timeouts, connection resets) the rate is halved
(multiplicative decrease); after a run of successes it creeps back up
(additive increase) to the configured ceiling. This keeps MinIO and
SeaweedFS out of memory/queue pressure without manual babysitting.
"""
from __future__ import annotations

import threading
import time


class AdaptiveRateLimiter:
    def __init__(self, rate: float, *, min_rate: float = 5.0, adaptive: bool = True,
                 recover_every: int = 200, clock=time.monotonic, sleep=time.sleep):
        if rate <= 0:
            raise ValueError("rate must be > 0")
        self.max_rate = float(rate)
        self.min_rate = min(float(min_rate), self.max_rate)
        self.rate = float(rate)
        self.adaptive = adaptive
        self.recover_every = recover_every
        self._tokens = 1.0
        self._last = clock()
        self._ok_streak = 0
        self._lock = threading.Lock()
        self._clock = clock
        self._sleep = sleep
        self.throttle_events = 0

    def acquire(self, n: float = 1.0) -> None:
        while True:
            with self._lock:
                now = self._clock()
                self._tokens = min(self.rate, self._tokens + (now - self._last) * self.rate)
                self._last = now
                if self._tokens >= n - 1e-9:
                    self._tokens = max(0.0, self._tokens - n)
                    return
                wait = (n - self._tokens) / self.rate
            self._sleep(wait)

    def on_success(self) -> None:
        if not self.adaptive or self.rate >= self.max_rate:
            return
        with self._lock:
            self._ok_streak += 1
            if self._ok_streak >= self.recover_every:
                self._ok_streak = 0
                self.rate = min(self.max_rate, self.rate + max(1.0, self.max_rate * 0.05))

    def on_throttle(self) -> None:
        if not self.adaptive:
            return
        with self._lock:
            self._ok_streak = 0
            self.rate = max(self.min_rate, self.rate * 0.5)
            self._tokens = min(self._tokens, self.rate)
            self.throttle_events += 1

    def set_max(self, rate: float) -> None:
        with self._lock:
            self.max_rate = float(rate)
            self.rate = min(self.rate, self.max_rate)
