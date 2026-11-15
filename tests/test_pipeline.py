from __future__ import annotations

import csv
from pathlib import Path

import pytest

from market_pipeline.config import sample_data_dir
from market_pipeline.pipeline import derive_status, run_pipeline
from market_pipeline.sources import CsvSource
from market_pipeline.storage import Database
from tests.conftest import FakeSource, price_row, security


def csv_rows(dataset: str) -> int:
    with (sample_data_dir(dataset) / "daily_prices.csv").open(newline="") as handle:
        return sum(1 for _ in csv.DictReader(handle))


# -- end-to-end on the bundled synthetic data ---------------------------------


def test_base_dataset_loads_every_row(db: Database) -> None:
    summary = run_pipeline(db, CsvSource(sample_data_dir("base")))
    expected = csv_rows("base")
    assert summary.status == "success"
    assert summary.counters.rows_read == expected
    assert summary.counters.rows_inserted == expected
    assert summary.counters.rows_rejected == 0
    assert db.count_prices() == expected
    assert len(db.list_securities()) == 6


def test_second_identical_run_creates_no_duplicates(db: Database) -> None:
    run_pipeline(db, CsvSource(sample_data_dir("base")))
    before = db.count_prices()
    summary = run_pipeline(db, CsvSource(sample_data_dir("base")))
    assert summary.status == "success"
    assert summary.counters.rows_inserted == 0
    assert summary.counters.rows_skipped == before
    assert db.count_prices() == before


def test_full_refresh_rerun_is_idempotent(db: Database) -> None:
    run_pipeline(db, CsvSource(sample_data_dir("base")))
    before = db.count_prices()
    summary = run_pipeline(db, CsvSource(sample_data_dir("base")), full_refresh=True)
    assert summary.mode == "full"
    assert summary.counters.rows_unchanged == before
    assert summary.counters.rows_inserted == summary.counters.rows_updated == 0
    assert db.count_prices() == before


def test_update_batch_is_incremental_and_reports_bad_rows(db: Database) -> None:
    run_pipeline(db, CsvSource(sample_data_dir("base")))
    base_count = db.count_prices()

    summary = run_pipeline(db, CsvSource(sample_data_dir("update")))

    c = summary.counters
    assert summary.status == "partial_success"
    assert c.rows_read == csv_rows("update")
    assert c.rows_inserted == 35  # 30 new days for 6 securities + 5 for the new one
    assert c.rows_rejected == 4  # 3 invalid rows + 1 duplicate
    assert c.rows_skipped == 1  # the correction is not newer than the watermark
    assert db.count_prices() == base_count + 35
    assert db.latest_trade_date("SYN-GLC") is not None

    stages = {e.stage for e in summary.errors}
    assert stages == {"validate_price"}
    stored = db.run_errors(summary.run_id)
    assert len(stored) == 4
    assert any("duplicate row for SYN-EVR" in e["message"] for e in stored)
    assert any("daily_prices.csv:" in e["message"] for e in stored)


def test_full_refresh_applies_correction(db: Database) -> None:
    run_pipeline(db, CsvSource(sample_data_dir("base")))
    original = db.price_history("SYN-AUR")[-1]["close"]
    run_pipeline(db, CsvSource(sample_data_dir("update")))

    summary = run_pipeline(db, CsvSource(sample_data_dir("update")), full_refresh=True)

    assert summary.counters.rows_updated == 1
    assert summary.counters.rows_unchanged == 35
    corrected = [r for r in db.price_history("SYN-AUR") if r["trade_date"] == "2024-12-31"]
    assert corrected[0]["close"] == pytest.approx(original + 1.0)


# -- status and failure handling ----------------------------------------------


def test_one_failing_symbol_gives_partial_success(db: Database) -> None:
    source = FakeSource(
        [security("AAA"), security("BBB")],
        {"AAA": [price_row("AAA")], "BBB": [price_row("BBB")]},
        failing=["BBB"],
    )
    summary = run_pipeline(db, source)
    assert summary.status == "partial_success"
    assert (summary.counters.symbols_succeeded, summary.counters.symbols_failed) == (1, 1)
    assert db.count_prices("AAA") == 1
    errors = db.run_errors(summary.run_id)
    assert errors == [
        {"symbol": "BBB", "stage": "load", "message": "ConnectionError: simulated outage for BBB"}
    ]


def test_all_symbols_failing_marks_run_failed(db: Database) -> None:
    source = FakeSource([security("AAA")], {}, failing=["AAA"])
    summary = run_pipeline(db, source)
    assert summary.status == "failed"
    run = db.get_run(summary.run_id)
    assert run is not None
    assert run["status"] == "failed"
    assert run["error_message"] == "All 1 symbol(s) failed to load."


def test_unreadable_source_fails_run_without_leaving_it_running(
    db: Database, tmp_path: Path
) -> None:
    summary = run_pipeline(db, CsvSource(tmp_path / "missing"))
    assert summary.status == "failed"
    run = db.get_run(summary.run_id)
    assert run is not None
    assert run["status"] == "failed"
    assert run["finished_at"] is not None
    assert "CSV file not found" in run["error_message"]


def test_empty_source_fails(db: Database) -> None:
    summary = run_pipeline(db, FakeSource([], {}))
    assert summary.status == "failed"
    assert summary.error_message == "No securities to process."


def test_rejected_security_makes_run_partial(db: Database) -> None:
    source = FakeSource(
        [security("AAA"), {"symbol": "BBB", "name": "", "exchange": "TEST"}],
        {"AAA": [price_row("AAA")]},
    )
    summary = run_pipeline(db, source)
    assert summary.status == "partial_success"
    assert summary.counters.securities_rejected == 1
    assert summary.errors[0].stage == "validate_security"


def test_symbol_subset_and_unknown_symbol(db: Database) -> None:
    source = FakeSource(
        [security("AAA"), security("BBB")],
        {"AAA": [price_row("AAA")], "BBB": [price_row("BBB")]},
    )
    summary = run_pipeline(db, source, symbols=["aaa", "AAA", "NOPE"])
    assert summary.status == "partial_success"
    assert summary.counters.symbols_planned == 2
    assert db.count_prices("AAA") == 1
    assert db.count_prices("BBB") == 0
    assert summary.errors[0].symbol == "NOPE"
    assert summary.errors[0].stage == "select"


def test_derive_status_rules() -> None:
    from market_pipeline.models import RunCounters

    assert derive_status(RunCounters(symbols_planned=2, symbols_succeeded=2)) == "success"
    assert derive_status(RunCounters(symbols_planned=0)) == "failed"
    assert (
        derive_status(RunCounters(symbols_planned=2, symbols_succeeded=2, rows_rejected=1))
        == "partial_success"
    )
