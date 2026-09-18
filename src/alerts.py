"""
Alert scoring per the spec:

  Alert 1 (mean/std):    | spread - rolling_mean(5min) | / rolling_std(5min)   > 3.5
  Alert 2 (median/MAD):  | spread - rolling_median(5min) | / rolling_MAD(5min) > 3.5

Both are z-score-style outlier tests on the spread; Alert 2 is the
"robust" version (median/MAD instead of mean/std), which matters a lot
here - see README "Which alert type do you prefer?".
"""
from __future__ import annotations

from typing import NamedTuple, Optional

THRESHOLD = 3.5

# MAD -> "std-equivalent" scale factor for a normal distribution.
# Using this constant lets alert_2's threshold be compared apples-to-apples
# with alert_1's if you ever want a single unified severity score; the raw
# ratio in the spec doesn't require it, but it's cheap to expose.
MAD_TO_STD_SCALE = 1.4826


class AlertScore(NamedTuple):
    triggered: bool
    score: Optional[float]


def _score(diff: Optional[float], scale: Optional[float]) -> AlertScore:
    if diff is None or scale is None or scale == 0:
        # Can't score: not enough history, or a degenerate (zero-variance)
        # window. We choose NOT to alert on undefined scores. Alerting on
        # noise (e.g. very early in the stream) would just create alert
        # fatigue. This is a deliberate false-negative-over-false-positive
        # choice for the cold-start period; documented in README.
        return AlertScore(False, None)
    score = abs(diff) / scale
    return AlertScore(score > THRESHOLD, score)


def alert_1(spread: Optional[float], rolling_mean: Optional[float], rolling_std: Optional[float]) -> AlertScore:
    if spread is None or rolling_mean is None:
        return AlertScore(False, None)
    return _score(spread - rolling_mean, rolling_std)


def alert_2(spread: Optional[float], rolling_median: Optional[float], rolling_mad: Optional[float]) -> AlertScore:
    if spread is None or rolling_median is None:
        return AlertScore(False, None)
    return _score(spread - rolling_median, rolling_mad)
