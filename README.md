# BMO Strategy Tracking — Take-Home Solution

Real-time monitoring of pricing-strategy spreads against a reference price:
rolling statistics, two alert types, per-tick ranking, and up-time tracking.

## Quick start

```bash
pip install -r requirements.txt   # only pandas, used in analysis/validation, not required by the engine itself
python -m unittest discover tests -v
python main.py --input data/sample_stream_data.csv --output output/metrics.csv
```

`main.py --speed 50` replays the file at 50x real-time and prints alerts as
they fire, to make the "streaming" nature visible rather than just batch.

## Project layout

```
src/
  models.py          Tick / StrategyMetrics / ObservationResult dataclasses
  ingest.py           CSV loading, cleaning, timestamp reconstruction, stream simulator
  windowed_stats.py   Time-based rolling mean/std/median/MAD (deque-based)
  alerts.py           Alert 1 (mean/std) and Alert 2 (median/MAD) scoring
  ranking.py          Price ranking + up-time tracking
  engine.py           Orchestrates the above per tick
main.py               CLI: run the simulated stream end to end
tests/test_pipeline.py 16 unit + integration tests
```

## Data-quality issues found and how I handled them

The raw CSV surprised me in three ways, all handled explicitly rather than
silently:

1. **Timestamps are truncated to `mm:ss.d`** : no date, no hour. I
   reconstructed full timestamps by anchoring on the date/hour shown in the
   PDF's Table 1 (`2017-05-08 09:xx`) and detecting a minute wrap-around
   (any decrease in the minute value within an hour, minutes are
   non-decreasing as real time advances, so any decrease at all signals
   wrapping past `59 -> 00`) to roll the hour forward. Verified against
   the raw file: the minute counter wraps 6 times across ~4,656 valid
   rows, landing the last tick at `15:59:59`, which is internally consistent.
   **This anchor date is not derived from the data. It's a documented
   assumption, exposed as a `--anchor-date` CLI flag on `main.py`**
   precisely so it doesn't silently mislabel a different file. Running
   without the flag logs an explicit line stating the default is in
   effect, rather than assuming silently.
2. **~24 fully blank rows** scattered through the file (no timestamp, no
   prices at all) are dropped. There's no timestamp to anchor them to, so
   imputation isn't defensible. The drop count is returned by
   `load_ticks(..., return_dropped=True)` and both logged and printed by
   `main.py` (`"24 malformed row(s) dropped"`), not just computed and
   discarded.
3. **Per-strategy nulls** (a strategy just isn't quoting that tick) are
   preserved as `None`, not zero-filled or forward-filled. Forward-filling
   would understate a strategy's actual down-time and could mask a stuck
   quote as a live one. Up-time and alerting both depend on nulls being
   real.

## Design decisions worth defending

- **Mean/variance use a sliding-window Welford's algorithm, not the naive
  `sum_of_squares/n - mean**2` formula.** The naive formula subtracts two
  large, nearly-equal numbers whenever values are large relative to their
  variance, which can cause catastrophic cancellation. It
  wasn't visibly broken at this dataset's actual price magnitudes
  (~103.xx), but "wasn't broken on this particular input" isn't the same
  as "is correct". Welford's algorithm (with the standard reverse-Welford
  identity for eviction) avoids the cancellation entirely and costs
  nothing extra (still O(1) per update). `tests/test_pipeline.py` includes
  a stress test at a 1e8-offset/1e-6-spread scale specifically designed to
  expose the difference, plus a randomized brute-force cross-check over
  hundreds of add/evict operations.
- **Time-based windows, not count-based.** Samples arrive at irregular
  gaps (~5s but not exact) and can be null. A "last 60 observations" window
  silently stretches or shrinks in wall-clock time whenever arrival rate
  changes; "last 5 minutes" doesn't. This matters directly for the alert
  threshold's meaning.
- **Alerts are scored against the window *before* folding in the current
  point**, then the point is folded in afterward. If you score against a
  window that already includes the point being tested, a genuine outlier
  widens its own std/MAD right when you want the band to stay tight, 
  which systematically damps true positives. Scoring against strictly
  prior history avoids that. This is a real modeling choice, not
  implementation detail. I'd defend it as "judge new evidence against the
  established baseline, not against itself."
- **Cold start / degenerate windows never alert.** With <2 points, or a
  zero-variance window (all identical spreads so far), `std`/`MAD` are
  either undefined or zero, alerting there would mean "everything
  triggers on the very first spike after a stable patch," which is exactly
  when you least want alert fatigue. I chose false-negative-over-false-positive
  for that narrow cold-start case and documented it in `alerts.py`.
- **Up-time and rank are computed per-tick, not just at the end**, since
  the spec says "for every observation," and a live dashboard would want
  the running value, not a batch-end summary.

## Validation

`tests/test_pipeline.py` — 21 tests, `python -m unittest discover tests -v`:
- Rolling mean/std/median validated against Python's `statistics` module
  on a synthetic series that includes an outlier.
- A numerical-stability stress test (large-offset, tiny-variance values)
  and a randomized brute-force cross-check of the Welford's-algorithm
  implementation against a naive ground-truth recomputation over hundreds
  of add/evict operations.
- MAD checked by hand-computed example.
- Window eviction checked by advancing time past the window boundary and
  confirming stale points drop out.
- Null-handling: nulls don't get treated as zero and don't corrupt the
  window.
- A synthetic 60-tick stable series followed by one large spike is
  asserted to trigger **both** alert types: an end-to-end sanity check
  that the whole pipeline, not just individual functions, behaves as
  expected.
- Ingestion tests check the blank-row drop count and that the
  reconstructed first timestamp matches the PDF's own example
  (`09:30:04`).

On the actual sample file (5-minute window): Alert 2 (median/MAD) fires far
more often than Alert 1 (mean/std) — 947 vs. 84 events. See below for why,
and which I'd actually run in production.

---

## Discussion questions from the assignment

### Which alert type do you prefer? Which strategy would work better in production?

**Alert 2 (median/MAD)**, for production, with a caveat below.

Mean and standard deviation are themselves distorted by the very outliers
you're trying to detect: One bad print inflates the std enough that the
*next* bad print might not clear the 3.5 threshold. Median and MAD don't
have that problem; each individual point has bounded influence on them
(that's the whole point of a robust statistic). That's exactly what we see
in the sample: Alert 2 caught roughly 11x more events than Alert 1 over the
same data, several of which were legitimate large deviations that Alert 1's
own inflated std had effectively hidden from itself.

The caveat: Alert 2's sensitivity is also its downside in production.
It's not "smarter," it's more responsive, which means more noise if your
spread naturally has fat tails (e.g. thin, illiquid names where a couple of
wide prints per window are normal, not anomalous). In practice I'd run
**both** side by side rather than picking one: Alert 1 as a coarse
persistent-drift detector (mean-reversion / calibration failure), Alert 2
as the primary anomaly detector for single-tick blowouts, and treat
"both fired" as higher severity than either alone.

**With larger window sizes and more granular data**, the case for Alert 2
gets stronger, not weaker: larger windows make the mean/std pair even more
vulnerable to a single early outlier dragging the baseline (a 30-minute
window contaminated by one bad print stays contaminated for 30 minutes),
while median/MAD's breakdown point (up to ~50% of points can be outliers
before it breaks) barely notices. More granular data (sub-second ticks)
means far more points per window, which helps both, but it also means the
O(k log k) sort-per-update in this implementation starts to matter (see
Scaling below).

### In a live trading system, monitoring thousands of securities/strategies, what would you do differently?

- **Partition by security**, not by strategy. We should route each security's ticks
  to the same shard/consumer so all its strategies see a consistent
  ordering and share a reference price without a cross-shard join. Kafka
  topic keyed by security ID; each partition owns an in-memory window per
  (security, strategy) pair.
- **Move state out of a single Python process.** This implementation keeps
  windows in per-process memory (`RollingWindow` deques), which is fine for one
  security, not for thousands. I'd move to a stream processor (Kafka
  Streams, Flink, or Spark Structured Streaming) that manages windowed
  state per key natively with checkpointing so a restart doesn't lose the
  rolling window.
- **Separate the "hot path" (alerting) from the "cold path" (full
  history/audit).** Alerting needs the last few minutes in memory and
  needs to be fast; historical analysis, backtesting alert thresholds, and
  compliance audit don't need sub-second latency and shouldn't share
  infra with the thing that has to page someone at 2am.
- **Alert deduplication / hysteresis.** A spread parked just above the
  3.5 threshold will alert on every tick until it moves. That's alert
  fatigue at scale. I'd add a state machine per (security, strategy, alert
  type): OK -> ALERTING -> (stays ALERTING until it clears a lower
  threshold, e.g. 2.5, for N consecutive ticks) -> OK, and only emit a
  notification on state transitions, not on every tick.
- **Backpressure and load shedding per key**, so one misbehaving security
  spamming updates can't starve processing for the other thousands.

### Scaling to tens of thousands of observations/day, multiple time windows, thousands of securities/strategies

- **Multiple windows (5min/30min/1hr) from one state store, not
  independent recomputation.** Maintain multiple `RollingWindow` instances
  keyed by window size but backed by the *same* incoming stream, so a
  single ingest reads the tick once and fans it out. As a result, we don't reread history
  per window.
- **Approximate/streaming algorithms once k (points per window) gets
  large.** This implementation sorts the window on every update for
  median/MAD, which is fine for k~60 (5min @ 5s), but not for a 1-hour window at
  millisecond ticks (k in the hundreds of thousands). At that scale I'd
  use a t-digest or KLL sketch for approximate quantiles/MAD, and Welford's
  algorithm (already effectively what the running-sum approach does) for
  numerically stable mean/std.
- **Compute layer: Spark Structured Streaming or Flink** for the
  thousands-of-securities case. Windowed aggregations are a first-class
  primitive, state is checkpointed (recovers cleanly from a crash without
  losing the rolling window), and it scales horizontally by partitioning
  on security ID.
- **Storage tiering:**
  - **Kafka** as the ingest/transport layer, retained a few days for replay.
  - **Hot store for current window state**: something like Redis or a
    stream processor's own RocksDB-backed state store with sub-ms reads for
    "what's the current rolling mean for this key."
  - **Warm/cold store for history**: columnar, partitioned by date and
    security (Parquet on S3/ADLS, or a time-series-oriented warehouse like
    Snowflake/BigQuery/Timescale) for backtesting alert thresholds,
    compliance queries, and model retraining. Partition + sort by
    (date, security) so a query for "STRATEGY1 spreads for security X over
    the last month" doesn't scan everything.
  - **Data versioning**: object storage with a table format that supports
    time travel and schema evolution (Delta Lake / Iceberg) rather than
    raw Parquet files, so "what did this alert threshold look like on the
    data as of last Tuesday" is an actual query, not a manual reconstruction.
- **Model/config storage and serving**: alert thresholds and window sizes
  aren't really "a model" here, but if this evolved into a learned
  anomaly-detection model, I'd version it in a model registry (e.g.
  MLflow) and serve it from the same stream processor as a UDF, so scoring
  happens in the same pass as windowing instead of round-tripping to a
  separate service per tick.

### How do you launch, monitor, and maintain this once deployed?

- **Launch**: containerize the stream-processing job, deploy behind CI/CD
  with a staging environment replaying historical data before promoting to
  prod; canary against a subset of securities before full rollout. (Not
  suggesting Kubernetes here per your instructions since for a single
  streaming job, a managed service, such as AWS MSK + Kinesis Data Analytics
  / Managed Flink, or Databricks jobs for Spark Structured Streaming, gets
  you most of the operational benefit without owning cluster orchestration.)
- **Monitor**: emit pipeline health as its own metrics stream. Ingest
  lag, `rows_dropped`, window-state size, alert rate per security (a
  sudden spike in alert *volume* is itself worth alerting on, since it
  often means upstream data corruption rather than real anomalies), and
  end-to-end latency from tick arrival to alert emission. Dashboards +
  on-call paging on lag/error-rate thresholds, separate from the trading
  alerts themselves.
- **Maintain**: version-control alert thresholds and window sizes as
  config, not hardcoded constants, so tuning them is a reviewed PR, not a
  code change; keep the validation test suite (like `tests/test_pipeline.py`
  here) running in CI against both synthetic edge cases and a fixed
  historical replay, so a change to the windowing logic can be checked
  against known-good alert counts before it ships.
#   s t r a t e g y _ t r a c k i n g  
 #   s t r a t e g y _ t r a c k i n g  
 