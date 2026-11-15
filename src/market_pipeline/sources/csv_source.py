"""Offline data source that reads two CSV files from a directory."""

from __future__ import annotations

import csv
from collections import defaultdict
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from market_pipeline.models import SOURCE_REF_FIELD, RawRecord
from market_pipeline.sources.base import DataSourceError

SECURITY_COLUMNS = ("symbol", "name", "exchange")
PRICE_COLUMNS = ("symbol", "trade_date", "open", "high", "low", "close", "volume")


class CsvSource:
    """Read ``securities.csv`` and ``daily_prices.csv`` from ``data_dir``.

    Values are returned as stripped strings with a ``_source_ref`` of the form
    ``"daily_prices.csv:12"`` so validation errors point at the exact line.
    """

    name = "csv"
    securities_file = "securities.csv"
    prices_file = "daily_prices.csv"

    def __init__(self, data_dir: str | Path) -> None:
        self.data_dir = Path(data_dir)
        self._prices_by_symbol: dict[str, list[RawRecord]] | None = None

    def list_securities(self) -> list[RawRecord]:
        return self._read(self.securities_file, SECURITY_COLUMNS)

    def fetch_prices(self, symbol: str, since: date | None = None) -> list[RawRecord]:
        # The whole file is small, so it is read once and grouped by symbol.
        # ``since`` is ignored; the pipeline applies the incremental filter.
        if self._prices_by_symbol is None:
            grouped: dict[str, list[RawRecord]] = defaultdict(list)
            for record in self._read(self.prices_file, PRICE_COLUMNS):
                grouped[record.get("symbol", "").upper()].append(record)
            self._prices_by_symbol = dict(grouped)
        return list(self._prices_by_symbol.get(symbol.strip().upper(), []))

    def _read(self, filename: str, required: Sequence[str]) -> list[RawRecord]:
        path = self.data_dir / filename
        if not path.is_file():
            raise DataSourceError(f"CSV file not found: {path}")
        try:
            with path.open(newline="", encoding="utf-8-sig") as handle:
                reader = csv.DictReader(handle)
                header = [column.strip() for column in (reader.fieldnames or [])]
                missing = [column for column in required if column not in header]
                if missing:
                    raise DataSourceError(
                        f"{filename} is missing required column(s): {', '.join(missing)}"
                    )
                reader.fieldnames = header
                records: list[RawRecord] = []
                for row in reader:
                    # Extra unnamed fields (row[None]) are ignored.
                    record = {
                        key: (value or "").strip()
                        for key, value in row.items()
                        if isinstance(key, str)
                    }
                    record[SOURCE_REF_FIELD] = f"{filename}:{reader.line_num}"
                    records.append(record)
        except (OSError, UnicodeDecodeError, csv.Error) as exc:
            raise DataSourceError(f"could not read {path}: {exc}") from exc
        return records
