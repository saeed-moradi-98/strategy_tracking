"""
Core data structures for the strategy tracking pipeline.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional


@dataclass
class Tick:
    """A single observation from the market data stream (one CSV row)."""

    timestamp: datetime
    reference_price: Optional[float]
    strategy_prices: Dict[str, Optional[float]]  # e.g. {"STRATEGY1": 103.23, ...}


@dataclass
class StrategyMetrics:
    """Everything computed for one strategy at one point in time."""

    strategy: str
    price: Optional[float]
    spread: Optional[float]  # price - reference_price

    rolling_mean: Optional[float] = None
    rolling_std: Optional[float] = None
    rolling_median: Optional[float] = None
    rolling_mad: Optional[float] = None

    alert_1: bool = False  # mean/std (modified) z-score
    alert_1_score: Optional[float] = None
    alert_2: bool = False  # median/MAD (robust) z-score
    alert_2_score: Optional[float] = None

    rank: Optional[int] = None
    uptime_pct: Optional[float] = None


@dataclass
class ObservationResult:
    """Output row: one Tick, fanned out into per-strategy metrics."""

    timestamp: datetime
    reference_price: Optional[float]
    metrics: Dict[str, StrategyMetrics] = field(default_factory=dict)
