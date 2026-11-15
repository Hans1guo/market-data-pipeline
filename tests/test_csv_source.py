from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from market_pipeline.config import sample_data_dir
from market_pipeline.models import SOURCE_REF_FIELD
from market_pipeline.sources import CsvSource, DataSourceError
from tests.conftest import PRICE_HEADER, write_csv


def test_reads_bundled_synthetic_dataset() -> None:
    source = CsvSource(sample_data_dir("base"))
    securities = source.list_securities()
    assert len(securities) == 6
    assert all(s["symbol"].startswith("SYN-") for s in securities)
    prices = source.fetch_prices("SYN-AUR")
    assert len(prices) == 261  # Monday-Friday dates in 2024
    assert prices[0][SOURCE_REF_FIELD] == "daily_prices.csv:2"


def test_symbol_lookup_is_case_insensitive_and_unknown_is_empty(
    make_dataset: Callable[..., Path],
) -> None:
    directory = make_dataset(
        [["AAA", "A Corp", "TEST", ""]],
        [["AAA", "2024-01-02", "10", "11", "9", "10.5", "100"]],
    )
    source = CsvSource(directory)
    assert len(source.fetch_prices("aaa")) == 1
    assert source.fetch_prices("ZZZ") == []


def test_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(DataSourceError, match="CSV file not found"):
        CsvSource(tmp_path).list_securities()


def test_missing_required_column_raises(tmp_path: Path) -> None:
    write_csv(tmp_path / "securities.csv", ["symbol", "name", "exchange"], [["AAA", "A", "T"]])
    header_without_volume = [c for c in PRICE_HEADER if c != "volume"]
    write_csv(tmp_path / "daily_prices.csv", header_without_volume, [])
    with pytest.raises(DataSourceError, match="missing required column\\(s\\): volume"):
        CsvSource(tmp_path).fetch_prices("AAA")


def test_bom_whitespace_and_blank_lines_are_handled(tmp_path: Path) -> None:
    (tmp_path / "securities.csv").write_text(
        "\ufeff symbol , name , exchange \n AAA , A Corp , TEST \n\n", encoding="utf-8"
    )
    records = CsvSource(tmp_path).list_securities()
    assert records == [
        {
            "symbol": "AAA",
            "name": "A Corp",
            "exchange": "TEST",
            SOURCE_REF_FIELD: "securities.csv:2",
        }
    ]


def test_undecodable_file_raises_source_error(tmp_path: Path) -> None:
    (tmp_path / "securities.csv").write_bytes(b"symbol,name,exchange\n\xff\xfe\xfa,x,y\n")
    with pytest.raises(DataSourceError, match="could not read"):
        CsvSource(tmp_path).list_securities()
