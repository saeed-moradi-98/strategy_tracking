"""
The streaming engine: consumes Tick objects one at a time (as they'd
arrive off a Kafka topic in production) and emits an ObservationResult
per tick, with per-strategy rolling stats, alerts, rank, and up-time.

Ordering choice for alerting (important, see README):
  For each strategy, we snapshot the rolling stats BEFORE folding the
  current spread into the window, and score the alert against that prior
  baseline. Then we fold the current point in for future ticks. This
  means a single extreme print can't inflate the very std/MAD used to
  judge itself - it gets judged against history, not against
  history+itself. This is a meaningful modeling choice, not a detail:
  the "include self" alternative systematically damps true positives
  (an outlier widens its own band right when you want it to stay tight).
"""
from __future__ import annotations

from datetime import timedelta
from typing import Dict, Iterable, Iterator, Optional

from . import alerts
from .models import ObservationResult, StrategyMetrics, Tick
from .ranking import UptimeTracker, rank_strategies
from .windowed_stats import RollingWindow


class StrategyTrackingEngine:
    def __init__(self, strategy_columns: Iterable[str], window: timedelta = timedelta(minutes=5)):
        self.strategy_columns = list(strategy_columns)
        self._windows: Dict[str, RollingWindow] = {
            s: RollingWindow(window) for s in self.strategy_columns
        }
        self._uptime = UptimeTracker()

    def process(self, tick: Tick) -> ObservationResult:
        prices = {s: tick.strategy_prices.get(s) for s in self.strategy_columns}
        ranks = rank_strategies(prices)
        uptimes = self._uptime.update(prices)

        result = ObservationResult(timestamp=tick.timestamp, reference_price=tick.reference_price)

        for strategy in self.strategy_columns:
            price = prices[strategy]
            spread = (
                price - tick.reference_price
                if price is not None and tick.reference_price is not None
                else None
            )

            window = self._windows[strategy]
            baseline = window.snapshot()  # stats BEFORE this tick

            a1 = alerts.alert_1(spread, baseline.mean, baseline.std)
            a2 = alerts.alert_2(spread, baseline.median, baseline.mad)

            # Now fold this tick's spread into the window for future ticks.
            updated = window.update(tick.timestamp, spread)

            result.metrics[strategy] = StrategyMetrics(
                strategy=strategy,
                price=price,
                spread=spread,
                rolling_mean=updated.mean,
                rolling_std=updated.std,
                rolling_median=updated.median,
                rolling_mad=updated.mad,
                alert_1=a1.triggered,
                alert_1_score=a1.score,
                alert_2=a2.triggered,
                alert_2_score=a2.score,
                rank=ranks[strategy],
                uptime_pct=uptimes[strategy],
            )

        return result

    def run(self, ticks: Iterable[Tick]) -> Iterator[ObservationResult]:
        for tick in ticks:
            yield self.process(tick)
