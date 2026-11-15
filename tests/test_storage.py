from __future__ import annotations

import sqlite3
from datetime import date, timedelta

import pytest

from market_pipeline.models import PriceBar, RunCounters, RunError, Security
from market_pipeline.storage import Database


def bar(day: int, close: float = 10.5, symbol: str = "AAA") -> PriceBar:
    return PriceBar(symbol, date(2024, 1, day), 10.0, 11.0, 9.5, close, 1000)


@pytest.fixture
def seeded(db: Database) -> Database:
    db.upsert_securities([Security("AAA", "A Corp", "TEST")])
    return db


def test_schema_is_idempotent(db: Database) -> None:
    db.init_schema()
    db.init_schema()
    assert {"securities", "daily_prices", "pipeline_runs", "pipeline_run_errors"} <= set(
        db.table_names()
    )


def test_upsert_inserts_then_reports_unchanged(seeded: Database) -> None:
    bars = [bar(2), bar(3)]
    first = seeded.upsert_price_bars(bars, source="test")
    second = seeded.upsert_price_bars(bars, source="test")
    assert (first.inserted, first.updated, first.unchanged) == (2, 0, 0)
    assert (second.inserted, second.updated, second.unchanged) == (0, 0, 2)
    assert seeded.count_prices() == 2


def test_upsert_updates_changed_values_in_place(seeded: Database) -> None:
    seeded.upsert_price_bars([bar(2)], source="test")
    stats = seeded.upsert_price_bars([bar(2, close=10.9), bar(3)], source="test")
    assert (stats.inserted, stats.updated, stats.unchanged) == (1, 1, 0)
    assert seeded.count_prices() == 2
    assert seeded.price_history("AAA")[0]["close"] == 10.9


def test_unique_constraint_blocks_duplicates_at_database_level(seeded: Database) -> None:
    seeded.upsert_price_bars([bar(2)], source="test")
    with pytest.raises(sqlite3.IntegrityError), seeded.transaction() as conn:
        conn.execute(
            "INSERT INTO daily_prices (symbol, trade_date, open, high, low, close, volume, "
            "source, created_at, updated_at) VALUES ('AAA', '2024-01-02', 1, 1, 1, 1, 1, "
            "'x', 'now', 'now')"
        )
    assert seeded.count_prices() == 1  # failed transaction was rolled back


def test_foreign_key_requires_known_security(db: Database) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        db.upsert_price_bars([bar(2, symbol="ZZZ")], source="test")
    assert db.count_prices() == 0


def test_check_constraint_rejects_inconsistent_prices(seeded: Database) -> None:
    broken = PriceBar("AAA", date(2024, 1, 2), 10.0, 9.0, 9.5, 10.0, 1)  # high < low
    with pytest.raises(sqlite3.IntegrityError):
        seeded.upsert_price_bars([broken], source="test")


def test_duplicate_keys_in_one_call_are_refused(seeded: Database) -> None:
    with pytest.raises(ValueError, match="duplicate"):
        seeded.upsert_price_bars([bar(2), bar(2, close=11.0)], source="test")


def test_latest_trade_date(seeded: Database) -> None:
    assert seeded.latest_trade_date("AAA") is None
    seeded.upsert_price_bars([bar(2), bar(5), bar(3)], source="test")
    assert seeded.latest_trade_date("AAA") == date(2024, 1, 5)


def test_security_upsert_updates_name(db: Database) -> None:
    db.upsert_securities([Security("AAA", "Old", "TEST")])
    db.upsert_securities([Security("AAA", "New", "TEST", "Tech")])
    assert db.list_securities() == [
        {"symbol": "AAA", "name": "New", "exchange": "TEST", "sector": "Tech"}
    ]


def test_run_lifecycle_and_errors(db: Database) -> None:
    run_id = db.start_run("csv", "incremental")
    assert db.get_run(run_id)["status"] == "running"  # type: ignore[index]

    db.add_run_errors(run_id, [RunError("AAA", "load", "boom")])
    db.finish_run(run_id, "partial_success", RunCounters(rows_inserted=5, symbols_failed=1))

    run = db.get_run(run_id)
    assert run is not None
    assert run["status"] == "partial_success"
    assert run["rows_inserted"] == 5
    assert run["finished_at"] is not None
    assert db.run_errors(run_id) == [{"symbol": "AAA", "stage": "load", "message": "boom"}]
    assert db.list_runs()[0]["id"] == run_id


def test_finish_run_rejects_invalid_status(db: Database) -> None:
    run_id = db.start_run("csv", "full")
    with pytest.raises(ValueError):
        db.finish_run(run_id, "running", RunCounters())


def test_stale_running_runs_are_marked_failed(db: Database) -> None:
    old = db.start_run("csv", "incremental")
    db._conn.execute(
        "UPDATE pipeline_runs SET started_at = '2000-01-01T00:00:00+00:00' WHERE id = ?", (old,)
    )
    fresh = db.start_run("csv", "incremental")
    assert db.mark_stale_runs(timedelta(hours=1)) == 1
    assert db.get_run(old)["status"] == "failed"  # type: ignore[index]
    assert db.get_run(fresh)["status"] == "running"  # type: ignore[index]


def test_in_memory_database_works() -> None:
    with Database(":memory:") as memory:
        memory.init_schema()
        assert memory.count_prices() == 0
