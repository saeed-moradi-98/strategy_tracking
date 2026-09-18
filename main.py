#!/usr/bin/env python3
"""
Run the strategy-tracking pipeline over the sample CSV, simulating a
real-time stream, and write per-tick metrics + a separate alert log.

Usage:
    python main.py --input data/sample_stream_data.csv --output output/metrics.csv --speed 0
    python main.py --speed 50          # replay at 50x real-time, print alerts live
"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.engine import StrategyTrackingEngine
from src.ingest import DEFAULT_ANCHOR_DATE, STRATEGY_COLUMNS_DEFAULT, load_ticks, stream_ticks


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(description="BMO strategy tracking - streaming simulation")
    parser.add_argument("--input", default="data/sample_stream_data.csv")
    parser.add_argument("--output", default="output/metrics.csv")
    parser.add_argument("--window-minutes", type=float, default=5.0)
    parser.add_argument(
        "--speed",
        type=float,
        default=0.0,
        help="0 = replay as fast as possible (batch mode). >0 = that multiple of real-time.",
    )
    parser.add_argument(
        "--anchor-date",
        default=None,
        help=(
            "Calendar date the truncated 'mm:ss' timestamps are anchored to, "
            "format YYYY-MM-DD (e.g. 2017-05-08). The source data has no date "
            "or hour in it, so this is NOT inferred from the file - it defaults "
            f"to this assignment's own PDF example ({DEFAULT_ANCHOR_DATE.date()}), "
            "which will be WRONG for any other file unless you pass this explicitly."
        ),
    )
    args = parser.parse_args()

    anchor_date = (
        datetime.strptime(args.anchor_date, "%Y-%m-%d").replace(
            hour=DEFAULT_ANCHOR_DATE.hour, minute=DEFAULT_ANCHOR_DATE.minute
        )
        if args.anchor_date
        else None
    )
    if anchor_date is None:
        logging.info(
            "No --anchor-date given; defaulting to %s (matches this assignment's "
            "sample data - pass --anchor-date explicitly for any other file).",
            DEFAULT_ANCHOR_DATE.date(),
        )

    ticks, dropped = load_ticks(
        args.input,
        strategy_columns=STRATEGY_COLUMNS_DEFAULT,
        anchor_date=anchor_date,
        return_dropped=True,
    )
    print(f"Loaded {len(ticks)} valid ticks from {args.input} ({dropped} malformed row(s) dropped)")

    engine = StrategyTrackingEngine(
        strategy_columns=STRATEGY_COLUMNS_DEFAULT,
        window=timedelta(minutes=args.window_minutes),
    )

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "timestamp", "reference_price", "strategy",
        "price", "spread", "rank", "uptime_pct",
        "rolling_mean", "rolling_std", "rolling_median", "rolling_mad",
        "alert_1", "alert_1_score", "alert_2", "alert_2_score",
    ]

    alert_count = 0
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for result in engine.run(stream_ticks(ticks, speed=args.speed)):
            for strategy, m in result.metrics.items():
                row = {
                    "timestamp": result.timestamp.isoformat(),
                    "reference_price": result.reference_price,
                    "strategy": strategy,
                    "price": m.price,
                    "spread": m.spread,
                    "rank": m.rank,
                    "uptime_pct": round(m.uptime_pct, 4) if m.uptime_pct is not None else None,
                    "rolling_mean": m.rolling_mean,
                    "rolling_std": m.rolling_std,
                    "rolling_median": m.rolling_median,
                    "rolling_mad": m.rolling_mad,
                    "alert_1": m.alert_1,
                    "alert_1_score": m.alert_1_score,
                    "alert_2": m.alert_2,
                    "alert_2_score": m.alert_2_score,
                }
                writer.writerow(row)

                if m.alert_1 or m.alert_2:
                    alert_count += 1
                    kinds = []
                    if m.alert_1:
                        kinds.append(f"alert_1={m.alert_1_score:.2f}")
                    if m.alert_2:
                        kinds.append(f"alert_2={m.alert_2_score:.2f}")
                    print(f"[ALERT] {result.timestamp} {strategy}: {' '.join(kinds)}")

    print(f"Done. {alert_count} alert events written to {out_path}")


if __name__ == "__main__":
    main()
