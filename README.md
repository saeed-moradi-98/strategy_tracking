BMO Strategy Tracking — Take-Home Solution

Real-time monitoring of pricing-strategy spreads against a reference price: rolling statistics, two alert types, per-tick ranking, and up-time tracking.

Quick Start
pip install -r requirements.txt
python -m unittest discover tests -v
python main.py --input data/sample_stream_data.csv --output output/metrics.csv


main.py --speed 50 replays the file at 50× real-time and prints alerts as they fire, making the "streaming" nature visible rather than treating the input as a batch.

Project Layout
src/
  models.py             Tick / StrategyMetrics / ObservationResult dataclasses
  ingest.py             CSV loading, cleaning, timestamp reconstruction, stream simulator
  windowed_stats.py     Time-based rolling mean/std/median/MAD (deque-based)
  alerts.py             Alert 1 (mean/std) and Alert 2 (median/MAD) scoring
  ranking.py            Price ranking + up-time tracking
  engine.py             Orchestrates the above per tick
main.py                 CLI: run the simulated stream end to end
tests/test_pipeline.py  Unit + integration tests

Data-Quality Issues Found and How I Handled Them

The raw CSV surprised me in three ways, all handled explicitly rather than silently:

1. Timestamps are truncated to mm:ss.d

There is no date or hour in the raw timestamps. I reconstructed full timestamps by anchoring on the date/hour shown in the PDF's Table 1 (2017-05-08 09:xx) and detecting minute wrap-around.

Any decrease in the minute value within an hour signals a wrap from 59 → 00, causing the hour to roll forward.

Verified against the raw file: the minute counter wraps 6 times across approximately 4,656 valid rows, landing the last tick at 15:59:59, which is internally consistent.

Important: This anchor date is not derived from the data. It is a documented assumption exposed as a --anchor-date CLI flag on main.py, precisely so it does not silently mislabel a different file.

Running without the flag logs an explicit line stating that the default is in effect rather than assuming silently.

2. Fully blank rows

Approximately 24 fully blank rows are scattered throughout the file (no timestamp and no prices).

These are dropped because there is no timestamp to anchor them to, so imputation is not defensible.

The drop count is returned by:

load_ticks(..., return_dropped=True)


Both main.py and the ingestion layer expose this information so the rows are not silently discarded.

3. Per-strategy nulls

A strategy may simply not quote during a particular tick.

These values are preserved as None, rather than being zero-filled or forward-filled.

Forward-filling would understate a strategy's actual down-time and could mask a stuck quote as a live one. Up-time and alerting both depend on nulls representing real missing quotes.

Design Decisions Worth Defending
Numerically stable rolling statistics

Mean/variance use a sliding-window Welford's algorithm rather than the naive:

sum_of_squares / n - mean²


formula.

The naive formula subtracts two large, nearly equal numbers whenever values are large relative to their variance, which can cause catastrophic cancellation.

It was not visibly broken at this dataset's actual price magnitudes (~103.xx), but "wasn't broken on this particular input" is not the same as "is numerically correct."

Welford's algorithm, combined with the standard reverse-Welford identity for eviction, avoids the cancellation while remaining O(1) per update.

tests/test_pipeline.py includes:

A stress test at a 1e8 offset with a 1e-6 spread specifically designed to expose numerical instability.

A randomized brute-force cross-check over hundreds of add/evict operations.

Time-based windows, not count-based

Samples arrive at irregular gaps (~5 seconds, but not exactly) and can be null.

A "last 60 observations" window silently stretches or shrinks in wall-clock time whenever arrival rate changes. A "last 5 minutes" window does not.

This matters directly for the meaning of the alert threshold.

Score alerts against the previous window

Alerts are scored against the window before folding in the current point. The current point is added afterward.

If the point being tested is already included in the window, a genuine outlier widens its own standard deviation or MAD right when the detector needs the band to remain tight. This systematically dampens true positives.

Scoring against strictly prior history avoids that problem.

The modeling principle is:

Judge new evidence against the established baseline, not against itself.

Cold start and degenerate windows

Cold-start and degenerate windows never alert.

With fewer than two points, or with a zero-variance window where all spreads so far are identical, std/MAD are either undefined or zero.

Alerting in that situation would effectively mean that everything triggers on the first spike after a stable patch — exactly when alert fatigue is least desirable.

I chose a false-negative-over-false-positive approach for this narrow cold-start case and documented it in alerts.py.

Per-tick ranking and up-time

Up-time and rank are computed per tick rather than only at the end.

The specification calls for results "for every observation," and a live dashboard would need the running value rather than a batch-end summary.

Validation

tests/test_pipeline.py contains 21 tests, run with:

python -m unittest discover tests -v


The test suite covers:

Rolling mean/std/median validated against Python's statistics module on a synthetic series containing an outlier.

A numerical-stability stress test using large-offset, tiny-variance values.

A randomized brute-force cross-check of the Welford implementation against naive ground-truth recomputation over hundreds of add/evict operations.

MAD validation using a hand-computed example.

Window eviction by advancing time beyond the window boundary and confirming stale points are removed.

Null handling, confirming nulls are not treated as zero and do not corrupt the rolling window.

An end-to-end test using a synthetic 60-tick stable series followed by one large spike, asserting that both alert types trigger.

Ingestion tests checking the blank-row drop count and confirming that the reconstructed first timestamp matches the PDF's example (09:30:04).

On the actual sample file with a 5-minute window:

Alert	Events
Alert 1 — Mean/Std	84
Alert 2 — Median/MAD	947

See below for the interpretation of these results.

Discussion Questions
Which alert type do you prefer? Which strategy would work better in production?

Alert 2 (median/MAD) is the more robust choice for production anomaly detection, with an important caveat.

Mean and standard deviation are themselves affected by the outliers they are trying to detect. One bad print can inflate the standard deviation enough that a subsequent bad print no longer clears the 3.5 threshold.

Median and MAD are much less affected by individual extreme observations. That is the purpose of using a robust statistic in the first place.

That behavior is visible in the sample data: Alert 2 fires 947 times versus 84 events for Alert 1 over the same period. Some of those additional events may represent legitimate large deviations rather than anomalies, so the difference should not be interpreted as proof that Alert 2 is detecting "more true anomalies."

The caveat is that Alert 2's sensitivity can also produce more noise in production if the underlying spread naturally has fat tails. For example, thin or illiquid names may legitimately experience several wide prints within a window.

A practical production design would therefore run both:

Alert 1: a coarse persistent-drift detector for things such as mean-reversion or calibration failures.

Alert 2: the primary detector for individual large deviations.

Both firing: an additional signal that could be assigned higher operational severity.

With larger windows and more granular data, the case for robust statistics becomes stronger in some settings. Larger windows give an early outlier more opportunity to influence the mean/std baseline, while median/MAD remain substantially less sensitive to individual extreme observations.

However, more granular data also increases the number of observations per window, which makes the current O(k log k) sort-per-update implementation increasingly expensive for median/MAD. See the scaling discussion below.

In a live trading system, monitoring thousands of securities/strategies, what would you do differently?
Partition by security

Route each security's ticks to the same shard or consumer so all of its strategies see a consistent ordering and share a reference price without requiring a cross-shard join.

For example, a Kafka topic could be keyed by security ID, with each partition owning an in-memory window for each (security, strategy) pair.

Move state out of a single Python process

This implementation keeps windows in per-process memory using RollingWindow deques.

That is appropriate for a small-scale implementation, but not for thousands of securities.

I would move the processing to a stream processor such as Kafka Streams, Flink, or Spark Structured Streaming, with checkpointed window state so a restart does not lose the rolling history.

Separate the hot and cold paths

The hot path should handle:

Recent rolling windows.

Alert evaluation.

Low-latency processing.

The cold path should handle:

Full historical data.

Backtesting.

Alert-threshold analysis.

Compliance/audit queries.

These workloads have different latency and storage requirements and should not compete for the same resources.

Add alert deduplication and hysteresis

A spread sitting just above the 3.5 threshold could generate an alert on every tick until it moves back inside the threshold.

At scale, that becomes alert fatigue.

I would add a state machine per (security, strategy, alert_type):

OK
 ↓
ALERTING
 ↓
wait for lower clear
