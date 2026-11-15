"""Watermark-based incremental loading.

The watermark is the latest ``trade_date`` already stored for a symbol. In
incremental mode only bars after it are loaded; older bars are counted as
skipped. A full refresh uses no watermark, so corrections to older rows are
picked up by the upsert.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from market_pipeline.models import PriceBar


@dataclass(frozen=True)
class IncrementalSplit:
    """Bars to load, and bars skipped because they are not newer than the watermark."""

    new: list[PriceBar]
    skipped: list[PriceBar]


def split_incremental(bars: Iterable[PriceBar], watermark: date | None) -> IncrementalSplit:
    """Split bars around ``watermark`` (``None`` means load everything)."""
    new: list[PriceBar] = []
    skipped: list[PriceBar] = []
    for bar in bars:
        if watermark is None or bar.trade_date > watermark:
            new.append(bar)
        else:
            skipped.append(bar)
    return IncrementalSplit(new=new, skipped=skipped)
