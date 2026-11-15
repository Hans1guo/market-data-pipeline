from __future__ import annotations

from datetime import date

import pytest

from market_pipeline.models import SOURCE_REF_FIELD
from market_pipeline.validation import (
    ValidationError,
    parse_price_bar,
    validate_price_bars,
    validate_securities,
)
from tests.conftest import price_row


def test_valid_row_is_typed_and_symbol_normalized() -> None:
    bar = parse_price_bar(price_row(symbol=" aaa ", volume="1200"))
    assert bar.symbol == "AAA"
    assert bar.trade_date == date(2024, 1, 2)
    assert (bar.open, bar.high, bar.low, bar.close) == (10.0, 11.0, 9.5, 10.5)
    assert bar.volume == 1200


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"close": ""}, "missing value for 'close'"),
        ({"open": "ten"}, "'open' is not a number"),
        ({"low": "0"}, "'low' must be a positive number"),
        ({"high": "nan"}, "'high' must be a positive number"),
        ({"volume": "-1"}, "'volume' must not be negative"),
        ({"volume": "10.5"}, "'volume' must be a whole number"),
        ({"trade_date": "2024/01/02"}, "must be YYYY-MM-DD"),
        ({"trade_date": "20240102"}, "must be YYYY-MM-DD"),
        ({"trade_date": "2024-02-30"}, "not a real date"),
        ({"high": "10.20"}, "high 10.2 is below open, close or low"),
        ({"low": "10.40", "open": "10.30"}, "low 10.4 is above open or close"),
        ({"symbol": "BAD SYMBOL"}, "invalid symbol"),
    ],
)
def test_invalid_rows_raise_with_reason(overrides: dict[str, str], message: str) -> None:
    raw = {**price_row(), **overrides}
    with pytest.raises(ValidationError, match=message):
        parse_price_bar(raw)


def test_batch_collects_rejections_with_source_reference() -> None:
    good = price_row(trade_date="2024-01-03")
    bad = {**price_row(volume="x"), SOURCE_REF_FIELD: "daily_prices.csv:7"}
    result = validate_price_bars([good, bad])
    assert [b.trade_date for b in result.valid] == [date(2024, 1, 3)]
    assert len(result.rejected) == 1
    assert result.rejected[0].source_ref == "daily_prices.csv:7"
    assert result.rejected[0].symbol == "AAA"


def test_duplicate_symbol_date_keeps_first_and_rejects_second() -> None:
    first = price_row(close="10.50")
    second = price_row(close="10.60")
    result = validate_price_bars([first, second])
    assert [b.close for b in result.valid] == [10.5]
    assert "duplicate row for AAA on 2024-01-02" in result.rejected[0].reason


def test_rows_for_other_symbols_are_rejected_when_symbol_given() -> None:
    result = validate_price_bars([price_row(symbol="BBB")], symbol="AAA")
    assert result.valid == []
    assert "expected symbol AAA" in result.rejected[0].reason


def test_valid_rows_are_sorted_by_date() -> None:
    rows = [price_row(trade_date=d) for d in ("2024-01-04", "2024-01-02", "2024-01-03")]
    result = validate_price_bars(rows)
    assert [b.trade_date.day for b in result.valid] == [2, 3, 4]


def test_securities_validation() -> None:
    result = validate_securities(
        [
            {"symbol": "aaa", "name": "A Corp", "exchange": "test", "sector": ""},
            {"symbol": "AAA", "name": "A again", "exchange": "TEST"},
            {"symbol": "BBB", "name": "", "exchange": "TEST"},
        ]
    )
    assert len(result.valid) == 1
    assert result.valid[0].symbol == "AAA"
    assert result.valid[0].exchange == "TEST"
    assert result.valid[0].sector is None
    reasons = [r.reason for r in result.rejected]
    assert "duplicate security AAA" in reasons
    assert "missing value for 'name'" in reasons
