from __future__ import annotations

import csv
from collections.abc import Callable, Iterator, Sequence
from datetime import date
from pathlib import Path

import pytest

from market_pipeline.models import RawRecord
from market_pipeline.storage import Database

PRICE_HEADER = ["symbol", "trade_date", "open", "high", "low", "close", "volume"]
SECURITY_HEADER = ["symbol", "name", "exchange", "sector"]


@pytest.fixture
def db(tmp_path: Path) -> Iterator[Database]:
    database = Database(tmp_path / "test.db")
    database.init_schema()
    yield database
    database.close()


def price_row(
    symbol: str = "AAA",
    trade_date: str = "2024-01-02",
    open_: str = "10.00",
    high: str = "11.00",
    low: str = "9.50",
    close: str = "10.50",
    volume: str = "1000",
) -> RawRecord:
    return {
        "symbol": symbol,
        "trade_date": trade_date,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


def write_csv(path: Path, header: Sequence[str], rows: Sequence[Sequence[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


@pytest.fixture
def make_dataset(tmp_path: Path) -> Callable[..., Path]:
    """Create a CSV dataset directory and return its path."""

    def _make(
        securities: Sequence[Sequence[str]],
        prices: Sequence[Sequence[str]],
        name: str = "dataset",
    ) -> Path:
        directory = tmp_path / name
        write_csv(directory / "securities.csv", SECURITY_HEADER, securities)
        write_csv(directory / "daily_prices.csv", PRICE_HEADER, prices)
        return directory

    return _make


class FakeSource:
    """In-memory source; symbols listed in ``failing`` raise on fetch."""

    name = "fake"

    def __init__(
        self,
        securities: list[RawRecord],
        prices: dict[str, list[RawRecord]],
        failing: Sequence[str] = (),
    ) -> None:
        self.securities = securities
        self.prices = prices
        self.failing = set(failing)

    def list_securities(self) -> list[RawRecord]:
        return list(self.securities)

    def fetch_prices(self, symbol: str, since: date | None = None) -> list[RawRecord]:
        if symbol in self.failing:
            raise ConnectionError(f"simulated outage for {symbol}")
        return list(self.prices.get(symbol, []))


def security(symbol: str) -> RawRecord:
    return {"symbol": symbol, "name": f"{symbol} Corp", "exchange": "TEST", "sector": ""}
