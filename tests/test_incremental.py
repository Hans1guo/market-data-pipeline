from __future__ import annotations

from datetime import date

from market_pipeline.incremental import split_incremental
from market_pipeline.models import PriceBar


def bars(*days: int) -> list[PriceBar]:
    return [PriceBar("AAA", date(2024, 1, d), 10, 11, 9, 10, 1) for d in days]


def test_no_watermark_loads_everything() -> None:
    split = split_incremental(bars(2, 3, 4), None)
    assert len(split.new) == 3
    assert split.skipped == []


def test_only_dates_after_watermark_are_new() -> None:
    split = split_incremental(bars(2, 3, 4, 5), date(2024, 1, 3))
    assert [b.trade_date.day for b in split.new] == [4, 5]
    assert [b.trade_date.day for b in split.skipped] == [2, 3]


def test_everything_skipped_when_nothing_newer() -> None:
    split = split_incremental(bars(2, 3), date(2024, 1, 10))
    assert split.new == []
    assert len(split.skipped) == 2
