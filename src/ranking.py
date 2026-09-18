"""
Cross-strategy metrics computed at each tick: price rank and up-time.
"""
from __future__ import annotations

from typing import Dict, Optional


def rank_strategies(prices: Dict[str, Optional[float]]) -> Dict[str, Optional[int]]:
    """Rank strategies by price, ascending (rank 1 = lowest price).

    Null prices get no rank (None) - a strategy that isn't quoting can't
    be meaningfully ranked against ones that are. Ties are broken by
    strategy name for determinism (stable, reproducible output); in
    practice float ties are rare enough not to matter for alerting.
    """
    quoting = [(name, p) for name, p in prices.items() if p is not None]
    quoting.sort(key=lambda item: (item[1], item[0]))

    ranks: Dict[str, Optional[int]] = {name: None for name in prices}
    for i, (name, _) in enumerate(quoting, start=1):
        ranks[name] = i
    return ranks


class UptimeTracker:
    """Running up-time pct per strategy: non-null observations / total observations."""

    def __init__(self) -> None:
        self._non_null: Dict[str, int] = {}
        self._total: Dict[str, int] = {}

    def update(self, prices: Dict[str, Optional[float]]) -> Dict[str, float]:
        result: Dict[str, float] = {}
        for name, price in prices.items():
            self._total[name] = self._total.get(name, 0) + 1
            if price is not None:
                self._non_null[name] = self._non_null.get(name, 0) + 1
            result[name] = self._non_null.get(name, 0) / self._total[name]
        return result
