"""
Unit tests. Run with:  python -m pytest tests/ -v
(or `python -m unittest discover tests` if pytest isn't installed)
"""
import math
import statistics
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.alerts import alert_1, alert_2
from src.engine import StrategyTrackingEngine
from src.ingest import load_ticks
from src.models import Tick
from src.ranking import UptimeTracker, rank_strategies
from src.windowed_stats import RollingWindow


class TestRollingWindow(unittest.TestCase):
    def test_mean_std_match_reference_implementation(self):
        base = datetime(2020, 1, 1, 9, 30, 0)
        values = [1.0, 2.0, 3.0, 4.0, 5.0, 100.0]  # includes an outlier
        w = RollingWindow(timedelta(minutes=5))
        for i, v in enumerate(values):
            snap = w.update(base + timedelta(seconds=i), v)

        self.assertAlmostEqual(snap.mean, statistics.mean(values), places=9)
        self.assertAlmostEqual(snap.std, statistics.pstdev(values), places=9)
        self.assertAlmostEqual(snap.median, statistics.median(values), places=9)

    def test_window_evicts_points_outside_time_range(self):
        base = datetime(2020, 1, 1, 9, 30, 0)
        w = RollingWindow(timedelta(minutes=1))
        w.update(base, 10.0)
        w.update(base + timedelta(seconds=30), 20.0)
        # This point is 2 minutes later -> both prior points should evict
        snap = w.update(base + timedelta(minutes=2, seconds=30), 30.0)
        self.assertEqual(snap.n, 1)
        self.assertEqual(snap.mean, 30.0)
        self.assertIsNone(snap.std)  # only 1 point -> std undefined

    def test_null_values_are_skipped_not_zero_filled(self):
        base = datetime(2020, 1, 1, 9, 30, 0)
        w = RollingWindow(timedelta(minutes=5))
        w.update(base, 10.0)
        snap = w.update(base + timedelta(seconds=5), None)  # null price tick
        self.assertEqual(snap.n, 1)
        self.assertEqual(snap.mean, 10.0)

    def test_empty_window_returns_none(self):
        w = RollingWindow(timedelta(minutes=5))
        snap = w.snapshot()
        self.assertEqual(snap.n, 0)
        self.assertIsNone(snap.mean)
        self.assertIsNone(snap.median)

    def test_variance_numerically_stable_for_large_offset_small_spread(self):
        # This is exactly the regime that breaks the naive
        # sum_of_squares/n - mean**2 formula: large absolute values
        # (prices ~1e8) with a tiny true variance. Welford's should stay
        # accurate; the naive formula would produce a wildly wrong (often
        # negative-before-clamping, or grossly inflated) variance here.
        base = datetime(2020, 1, 1, 9, 30, 0)
        offset = 1e8
        # true values: offset + [0, 1e-6, 2e-6, ..., 9e-6] -> tiny true variance
        values = [offset + i * 1e-6 for i in range(10)]
        w = RollingWindow(timedelta(minutes=5))
        for i, v in enumerate(values):
            snap = w.update(base + timedelta(seconds=i), v)

        expected_std = statistics.pstdev(values)
        # At this offset:spread ratio (1e8 : 1e-6) we're near float64's
        # ~15-17 significant-digit limit, so we check relative closeness,
        # not absolute decimal places. The point of this test isn't "exact
        # to the last bit" - it's "close, and never negative/NaN/blown up",
        # which is exactly what the naive sum-of-squares formula fails at
        # in this regime (it can produce a negative pre-clamp variance or
        # an answer off by orders of magnitude, not just a rounding blip).
        self.assertTrue(
            math.isclose(snap.std, expected_std, rel_tol=1e-2),
            f"got {snap.std}, expected ~{expected_std}",
        )
        self.assertGreaterEqual(snap.std, 0.0)  # must never go negative/NaN

    def test_welford_matches_brute_force_over_random_add_evict_sequence(self):
        import random

        random.seed(42)
        base = datetime(2020, 1, 1, 9, 30, 0)
        w = RollingWindow(timedelta(seconds=30))
        window_points = []  # brute-force ground truth: (timestamp, value)

        t = base
        for _ in range(500):
            t += timedelta(seconds=random.uniform(0.5, 3.0))
            v = 100.0 + random.uniform(-0.05, 0.05)
            w.update(t, v)

            window_points.append((t, v))
            cutoff = t - timedelta(seconds=30)
            window_points = [(ts, val) for ts, val in window_points if ts >= cutoff]

            expected_mean = statistics.mean(val for _, val in window_points)
            expected_std = (
                statistics.pstdev(val for _, val in window_points)
                if len(window_points) > 1
                else None
            )
            snap = w.snapshot()
            self.assertEqual(snap.n, len(window_points))
            self.assertAlmostEqual(snap.mean, expected_mean, places=6)
            if expected_std is None:
                self.assertIsNone(snap.std)
            else:
                self.assertAlmostEqual(snap.std, expected_std, places=6)

    def test_mad_matches_manual_calculation(self):
        # values: 1, 2, 3, 4, 100 -> median = 3
        # abs deviations from median: 2, 1, 0, 1, 97 -> sorted: 0,1,1,2,97 -> median = 1
        base = datetime(2020, 1, 1, 9, 30, 0)
        w = RollingWindow(timedelta(minutes=5))
        for i, v in enumerate([1.0, 2.0, 3.0, 4.0, 100.0]):
            snap = w.update(base + timedelta(seconds=i), v)
        self.assertEqual(snap.median, 3.0)
        self.assertEqual(snap.mad, 1.0)


class TestAlerts(unittest.TestCase):
    def test_alert_triggers_above_threshold(self):
        result = alert_1(spread=10.0, rolling_mean=0.0, rolling_std=1.0)
        self.assertTrue(result.triggered)
        self.assertAlmostEqual(result.score, 10.0)

    def test_alert_does_not_trigger_below_threshold(self):
        result = alert_1(spread=1.0, rolling_mean=0.0, rolling_std=1.0)
        self.assertFalse(result.triggered)

    def test_alert_handles_missing_data_gracefully(self):
        result = alert_1(spread=None, rolling_mean=None, rolling_std=None)
        self.assertFalse(result.triggered)
        self.assertIsNone(result.score)

    def test_alert_handles_zero_std_without_crashing(self):
        # all recent spreads identical -> std/MAD = 0 -> avoid division by zero
        result = alert_2(spread=5.0, rolling_median=1.0, rolling_mad=0.0)
        self.assertFalse(result.triggered)
        self.assertIsNone(result.score)


class TestRanking(unittest.TestCase):
    def test_rank_orders_ascending_by_price(self):
        ranks = rank_strategies({"A": 103.5, "B": 103.1, "C": 103.3})
        self.assertEqual(ranks, {"A": 3, "B": 1, "C": 2})

    def test_rank_skips_null_prices(self):
        ranks = rank_strategies({"A": 103.5, "B": None, "C": 103.3})
        self.assertEqual(ranks["B"], None)
        self.assertEqual(ranks["A"], 2)
        self.assertEqual(ranks["C"], 1)

    def test_uptime_tracks_non_null_fraction(self):
        tracker = UptimeTracker()
        tracker.update({"A": 1.0, "B": None})
        tracker.update({"A": 1.0, "B": 1.0})
        result = tracker.update({"A": None, "B": 1.0})
        self.assertAlmostEqual(result["A"], 2 / 3)
        self.assertAlmostEqual(result["B"], 2 / 3)


class TestIngestion(unittest.TestCase):
    def test_load_sample_csv_drops_blank_rows_and_sorts(self):
        ticks = load_ticks("data/sample_stream_data.csv")
        self.assertGreater(len(ticks), 4000)
        # sorted ascending
        self.assertTrue(all(ticks[i].timestamp <= ticks[i + 1].timestamp for i in range(len(ticks) - 1)))
        # first timestamp should match the PDF's Table 1 example (09:30:04.5xx)
        self.assertEqual(ticks[0].timestamp.strftime("%H:%M:%S"), "09:30:04")

    def test_null_strategy_price_preserved_as_none(self):
        ticks = load_ticks("data/sample_stream_data.csv")
        # STRATEGY1 is blank on the 10th raw data row per the PDF table sample
        nulls = [t for t in ticks if t.strategy_prices.get("STRATEGY1") is None]
        self.assertGreater(len(nulls), 0)

    def test_dropped_row_count_is_surfaced_not_discarded(self):
        ticks, dropped = load_ticks("data/sample_stream_data.csv", return_dropped=True)
        self.assertGreater(dropped, 0)
        # every raw CSV data row is either a valid tick or a counted drop -
        # nothing should silently vanish
        with open("data/sample_stream_data.csv") as f:
            raw_data_rows = sum(1 for _ in f) - 1  # minus header
        self.assertEqual(len(ticks) + dropped, raw_data_rows)

    def test_anchor_date_override_changes_reconstructed_date(self):
        default_ticks = load_ticks("data/sample_stream_data.csv")
        override_ticks = load_ticks(
            "data/sample_stream_data.csv", anchor_date=datetime(2020, 1, 1, 9, 30, 0)
        )
        self.assertNotEqual(default_ticks[0].timestamp.date(), override_ticks[0].timestamp.date())
        # time-of-day and relative ordering should be unaffected by the date override
        self.assertEqual(
            default_ticks[0].timestamp.strftime("%H:%M:%S"),
            override_ticks[0].timestamp.strftime("%H:%M:%S"),
        )

    def test_wrap_detection_matches_manually_verified_count(self):
        # Independently verified against the raw file (see interview prep):
        # the minute column wraps 59->00 exactly 6 times across the sample.
        ticks = load_ticks("data/sample_stream_data.csv")
        hours_seen = sorted({t.timestamp.hour for t in ticks})
        # 09 through 15 inclusive = 7 distinct hours = 6 wraps
        self.assertEqual(hours_seen, list(range(9, 16)))


class TestEngineIntegration(unittest.TestCase):
    def test_engine_runs_full_sample_without_error(self):
        ticks = load_ticks("data/sample_stream_data.csv")
        engine = StrategyTrackingEngine(["STRATEGY1", "STRATEGY2", "STRATEGY3"])
        results = list(engine.run(ticks))
        self.assertEqual(len(results), len(ticks))
        # every result should have exactly 3 strategies scored
        self.assertEqual(len(results[-1].metrics), 3)
        # up-time should be monotonically non-decreasing in numerator terms,
        # i.e. always a valid fraction in [0, 1]
        for r in results:
            for m in r.metrics.values():
                self.assertTrue(0.0 <= m.uptime_pct <= 1.0)

    def test_synthetic_spike_triggers_alert(self):
        base = datetime(2020, 1, 1, 9, 30, 0)
        ticks = []
        # 60 stable ticks (ref=100, strategy hovering ~100.01) then one spike
        for i in range(60):
            ticks.append(Tick(base + timedelta(seconds=i), 100.0, {"S1": 100.01 + (0.001 if i % 2 else -0.001)}))
        ticks.append(Tick(base + timedelta(seconds=60), 100.0, {"S1": 105.0}))  # huge spread spike

        engine = StrategyTrackingEngine(["S1"])
        results = list(engine.run(ticks))
        last = results[-1].metrics["S1"]
        self.assertTrue(last.alert_1)
        self.assertTrue(last.alert_2)


if __name__ == "__main__":
    unittest.main()
