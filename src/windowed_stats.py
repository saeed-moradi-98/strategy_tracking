"""
Time-based (not count-based) rolling window statistics.

Why time-based: samples arrive at irregular intervals (~5s apart, but not
exact, and strategies can be null on some ticks). A count-based window
(e.g. "last 60 observations") would silently stretch or shrink in wall-clock
time whenever the arrival rate changes. A time-based window ("last 5
minutes") keeps the statistic's meaning stable regardless of gaps, bursts,
or missing prices - which matters a lot for an alerting threshold.

Design: a deque of (timestamp, value) pairs per strategy. On each new
observation we evict everything older than `window` from the left, then
append the new point. Median/MAD need a sort, which is O(k log k) for
k = points currently in the window. For a 5-minute window at ~5s cadence
that's k ~= 60, trivial. (Scaling notes for much larger k / higher
frequency are in the README.)

Mean/variance use a SLIDING-WINDOW WELFORD'S ALGORITHM, not the naive
`sum_of_squares/n - mean**2` formula. The naive formula subtracts two
large, nearly-equal numbers (sum_of_squares/n and mean**2) whenever values
are large relative to their variance - e.g. prices ~103.xx with spreads of
a few cents - and that subtraction is exactly where floating-point
catastrophic cancellation happens, silently eating precision as the
window fills. Welford's algorithm instead tracks a running mean and M2
(sum of squared deviations from the *current* mean), updated
incrementally on both insert and evict, and never differences two large
numbers - it stays O(1) per update with none of the cancellation risk.
Removal uses the standard reverse-Welford identity: given (n, mean, M2)
that include point x, the pre-x mean and M2 are recovered exactly (up to
normal floating-point rounding, not cancellation) via
`mean_without_x = (mean*n - x) / (n-1)` and
`M2_without_x = M2 - (x - mean)*(x - mean_without_x)`.
"""
from __future__ import annotations

import math
from collections import deque
from datetime import datetime, timedelta
from typing import Deque, NamedTuple, Optional, Tuple


class RollingResult(NamedTuple):
    mean: Optional[float]
    std: Optional[float]
    median: Optional[float]
    mad: Optional[float]
    n: int


class RollingWindow:
    """Maintains rolling mean/std/median/MAD over a trailing time window."""

    def __init__(self, window: timedelta):
        self.window = window
        self._points: Deque[Tuple[datetime, float]] = deque()
        self._n = 0
        self._mean = 0.0
        self._m2 = 0.0  # sum of squared deviations from self._mean

    def _welford_add(self, x: float) -> None:
        self._n += 1
        delta = x - self._mean
        self._mean += delta / self._n
        delta2 = x - self._mean
        self._m2 += delta * delta2

    def _welford_remove(self, x: float) -> None:
        n_new = self._n - 1
        if n_new == 0:
            self._n, self._mean, self._m2 = 0, 0.0, 0.0
            return
        mean_without_x = (self._mean * self._n - x) / n_new
        self._m2 -= (x - self._mean) * (x - mean_without_x)
        self._m2 = max(self._m2, 0.0)  # guard tiny negative rounding residue
        self._mean = mean_without_x
        self._n = n_new

    def _evict_old(self, now: datetime) -> None:
        cutoff = now - self.window
        while self._points and self._points[0][0] < cutoff:
            _, old_val = self._points.popleft()
            self._welford_remove(old_val)

    def update(self, timestamp: datetime, value: Optional[float]) -> RollingResult:
        """Push a new observation (skip if null) and return the *current*
        rolling stats (computed over the window BEFORE this point is
        needed for the alert - see engine.py for the exact ordering used
        for alerting vs. the ordering used for the reported metric)."""
        self._evict_old(timestamp)
        if value is not None:
            self._points.append((timestamp, value))
            self._welford_add(value)
        return self.snapshot()

    def snapshot(self) -> RollingResult:
        n = len(self._points)
        if n == 0:
            return RollingResult(None, None, None, None, 0)

        mean = self._mean
        if n > 1:
            variance = max(self._m2 / n, 0.0)
            std = math.sqrt(variance)
        else:
            std = None

        values = sorted(v for _, v in self._points)
        median = _median(values)
        mad = _mad(values, median) if n > 1 else None

        return RollingResult(mean=mean, std=std, median=median, mad=mad, n=n)


def _median(sorted_values: list[float]) -> float:
    n = len(sorted_values)
    mid = n // 2
    if n % 2:
        return sorted_values[mid]
    return (sorted_values[mid - 1] + sorted_values[mid]) / 2.0


def _mad(sorted_values: list[float], median: float) -> float:
    deviations = sorted(abs(v - median) for v in sorted_values)
    return _median(deviations)
