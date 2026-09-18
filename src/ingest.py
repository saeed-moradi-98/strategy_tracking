"""
Ingestion layer: reads the sample CSV and yields Tick objects in
timestamp order, optionally throttled to simulate real-time arrival.

Data-quality notes (see README for full reasoning):
  1. The `ts` column only contains `mm:ss.d` (minutes:seconds.decisecond) -
     no date or hour. We reconstruct a full timestamp by anchoring on a
     caller-supplied (or default) date/hour and rolling the hour forward
     whenever the minute counter decreases from one row to the next -
     within a single hour, minutes are non-decreasing as real time
     advances, so ANY decrease (not just a big one) is the signal that
     we've wrapped past 59 -> 00. There's no data-derived way to know
     which calendar date/hour row 0 actually belongs to - that has to
     come from the caller (see `anchor_date` below); the default matches
     this assignment's own PDF example (2017-05-08 09:30) but will be
     WRONG if pointed at a different file without overriding it.
  2. A handful of rows are entirely blank (no timestamp, no prices). These
     are dropped as malformed records rather than imputed - we can't
     reconstruct a timestamp for a row that has none. The count is
     returned (not just logged) when `return_dropped=True`, so a caller
     can surface or assert on it rather than take on faith that dropping
     silently happened correctly.
"""
from __future__ import annotations

import csv
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple, Union

from .models import Tick

STRATEGY_COLUMNS_DEFAULT = ["STRATEGY1", "STRATEGY2", "STRATEGY3"]
DEFAULT_ANCHOR_DATE = datetime(2017, 5, 8, 9, 30, 0)

logger = logging.getLogger(__name__)


def _parse_float(raw: str) -> Optional[float]:
    raw = (raw or "").strip()
    if raw == "":
        return None
    return float(raw)


def _reconstruct_timestamp(
    mm_ss: str, anchor_date: datetime, state: Dict[str, int]
) -> datetime:
    """Turn 'MM:SS.d' into a full datetime, tracking hour rollover.

    `state` carries `last_minute` across calls (mutable dict used as a
    cheap closure cell) so we can detect a minute wrap (59 -> 00) and
    bump the hour forward. This is a pragmatic reconstruction, not a
    guarantee - documented explicitly as an assumption.
    """
    minute_str, sec_str = mm_ss.split(":")
    minute = int(minute_str)
    second = float(sec_str)

    last_minute = state.get("last_minute")
    hour_offset = state.get("hour_offset", 0)
    if last_minute is not None and minute < last_minute:
        # Any decrease in the minute value signals a wrap past 59 -> 00:
        # under the assumption that rows arrive in non-decreasing time
        # order, minutes-within-an-hour can only ever stay the same or
        # go up, never down, except at exactly this boundary.
        hour_offset += 1
    state["last_minute"] = minute
    state["hour_offset"] = hour_offset

    ts = anchor_date + timedelta(hours=hour_offset, minutes=minute)
    ts = ts.replace(second=int(second), microsecond=int((second % 1) * 1e6))
    return ts


def load_ticks(
    csv_path: Union[str, Path],
    strategy_columns: Optional[List[str]] = None,
    anchor_date: Optional[datetime] = None,
    return_dropped: bool = False,
) -> Union[List[Tick], Tuple[List[Tick], int]]:
    """Load and clean the CSV into an ordered list of Tick objects.

    anchor_date: the calendar date/hour that reconstructed timestamps are
        anchored to. Defaults to this assignment's own PDF example
        (2017-05-08 09:30). THIS IS NOT DERIVED FROM THE DATA - if you
        point this at a different file, pass the correct anchor_date
        explicitly or every reconstructed timestamp will be silently
        wrong (same time-of-day, wrong calendar date).
    return_dropped: if True, return (ticks, dropped_row_count) instead of
        just ticks, so a caller can surface/assert on how many malformed
        rows were dropped rather than have that count computed and
        discarded internally.
    """
    strategy_columns = strategy_columns or STRATEGY_COLUMNS_DEFAULT
    anchor_date = anchor_date or DEFAULT_ANCHOR_DATE

    # anchor_date is the *hour boundary* the reconstructed minutes are added
    # to (e.g. 09:00), not 09:30 - the "30" in the first row's "30:04.6" IS
    # the minute value, so baking :30 into the anchor would double-count it.
    anchor_date = anchor_date.replace(minute=0, second=0, microsecond=0)

    ticks: List[Tick] = []
    state: Dict[str, int] = {}
    dropped = 0

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts_raw = (row.get("ts") or "").strip()
            ref_raw = (row.get("REFERENCE_PRICE") or "").strip()
            if ts_raw == "" and ref_raw == "":
                # fully blank / malformed row - drop, can't recover a timestamp
                dropped += 1
                continue

            timestamp = _reconstruct_timestamp(ts_raw, anchor_date, state)
            reference_price = _parse_float(ref_raw)
            strategy_prices = {
                col: _parse_float(row.get(col, "")) for col in strategy_columns
            }
            ticks.append(
                Tick(
                    timestamp=timestamp,
                    reference_price=reference_price,
                    strategy_prices=strategy_prices,
                )
            )

    ticks.sort(key=lambda t: t.timestamp)
    if dropped:
        logger.warning("Dropped %d malformed (fully blank) row(s) from %s", dropped, csv_path)

    if return_dropped:
        return ticks, dropped
    return ticks


def stream_ticks(
    ticks: List[Tick], speed: float = 0.0
) -> Iterator[Tick]:
    """Yield ticks one at a time, sleeping to mimic real-time arrival.

    speed=0.0  -> no sleeping, replay as fast as possible (tests, batch)
    speed=1.0  -> real-time (sleep for the actual gap between ticks)
    speed=10.0 -> 10x real-time, etc.
    """
    prev_ts: Optional[datetime] = None
    for tick in ticks:
        if speed > 0 and prev_ts is not None:
            gap = (tick.timestamp - prev_ts).total_seconds()
            if gap > 0:
                time.sleep(gap / speed)
        prev_ts = tick.timestamp
        yield tick
