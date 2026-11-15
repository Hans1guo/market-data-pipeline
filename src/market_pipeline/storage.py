"""SQLite storage: schema setup, idempotent upserts, run logging and read queries.

All SQL lives in this module, so supporting another database later means
reimplementing :class:`Database` rather than touching the pipeline.
"""

from __future__ import annotations

import math
import sqlite3
from collections import defaultdict
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from importlib import resources
from pathlib import Path
from types import TracebackType
from typing import Any

from market_pipeline.models import PriceBar, RunCounters, RunError, Security

RUN_STATUSES = ("running", "success", "partial_success", "failed")

_UPSERT_SECURITY_SQL = """
INSERT INTO securities (symbol, name, exchange, sector, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (symbol) DO UPDATE SET
    name = excluded.name,
    exchange = excluded.exchange,
    sector = excluded.sector,
    updated_at = excluded.updated_at
WHERE securities.name IS NOT excluded.name
   OR securities.exchange IS NOT excluded.exchange
   OR securities.sector IS NOT excluded.sector
"""

_UPSERT_PRICE_SQL = """
INSERT INTO daily_prices
    (symbol, trade_date, open, high, low, close, volume, source, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (symbol, trade_date) DO UPDATE SET
    open = excluded.open,
    high = excluded.high,
    low = excluded.low,
    close = excluded.close,
    volume = excluded.volume,
    source = excluded.source,
    updated_at = excluded.updated_at
"""


def utc_now() -> datetime:
    """Current time in UTC (wrapped so tests can reason about it)."""
    return datetime.now(timezone.utc)


def _timestamp(moment: datetime) -> str:
    return moment.isoformat(timespec="seconds")


def _same_values(stored: sqlite3.Row, bar: PriceBar) -> bool:
    return (
        all(
            math.isclose(stored[name], getattr(bar, name), rel_tol=1e-9)
            for name in ("open", "high", "low", "close")
        )
        and stored["volume"] == bar.volume
    )


@dataclass(frozen=True)
class UpsertStats:
    """What an upsert did with each incoming row."""

    inserted: int = 0
    updated: int = 0
    unchanged: int = 0


class Database:
    """A SQLite database holding market data and pipeline run history.

    Use as a context manager, or call :meth:`close` when done. Pass
    ``":memory:"`` for a throwaway in-memory database.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        # isolation_level=None: transactions are managed explicitly below.
        self._conn = sqlite3.connect(self.path, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")

    # -- lifecycle -----------------------------------------------------------

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> Database:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Run statements atomically; roll back if anything raises."""
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield self._conn
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        self._conn.execute("COMMIT")

    def init_schema(self) -> None:
        """Create tables and indexes if they do not exist (safe to re-run)."""
        schema = resources.files("market_pipeline").joinpath("schema.sql")
        self._conn.executescript(schema.read_text(encoding="utf-8"))

    def table_names(self) -> list[str]:
        rows = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        )
        return [row["name"] for row in rows]

    # -- writes --------------------------------------------------------------

    def upsert_securities(self, securities: Sequence[Security]) -> None:
        """Insert new securities and update changed ones."""
        now = _timestamp(utc_now())
        rows = [(s.symbol, s.name, s.exchange, s.sector, now, now) for s in securities]
        with self.transaction() as conn:
            conn.executemany(_UPSERT_SECURITY_SQL, rows)

    def upsert_price_bars(self, bars: Sequence[PriceBar], source: str) -> UpsertStats:
        """Insert new bars, update changed ones and leave identical ones alone.

        Uniqueness is enforced by ``UNIQUE (symbol, trade_date)``, so running
        the same load twice never creates duplicate rows.
        """
        keys = [(bar.symbol, bar.trade_date.isoformat()) for bar in bars]
        if len(set(keys)) != len(keys):
            raise ValueError("bars contain duplicate (symbol, trade_date) pairs")
        if not bars:
            return UpsertStats()

        now = _timestamp(utc_now())
        inserted = updated = unchanged = 0
        with self.transaction() as conn:
            existing = self._existing_bars(conn, bars)
            to_write = []
            for bar, key in zip(bars, keys, strict=True):
                stored = existing.get(key)
                if stored is None:
                    inserted += 1
                elif _same_values(stored, bar):
                    unchanged += 1
                    continue
                else:
                    updated += 1
                to_write.append(
                    (*key, bar.open, bar.high, bar.low, bar.close, bar.volume, source, now, now)
                )
            conn.executemany(_UPSERT_PRICE_SQL, to_write)
        return UpsertStats(inserted=inserted, updated=updated, unchanged=unchanged)

    @staticmethod
    def _existing_bars(
        conn: sqlite3.Connection, bars: Sequence[PriceBar]
    ) -> dict[tuple[str, str], sqlite3.Row]:
        dates_by_symbol: dict[str, list[str]] = defaultdict(list)
        for bar in bars:
            dates_by_symbol[bar.symbol].append(bar.trade_date.isoformat())
        existing: dict[tuple[str, str], sqlite3.Row] = {}
        for symbol, dates in dates_by_symbol.items():
            rows = conn.execute(
                "SELECT trade_date, open, high, low, close, volume FROM daily_prices "
                "WHERE symbol = ? AND trade_date BETWEEN ? AND ?",
                (symbol, min(dates), max(dates)),
            )
            for row in rows:
                existing[(symbol, row["trade_date"])] = row
        return existing

    # -- run logging ---------------------------------------------------------

    def start_run(self, source: str, mode: str) -> int:
        """Record a new run with status ``running`` and return its id."""
        with self.transaction() as conn:
            cursor = conn.execute(
                "INSERT INTO pipeline_runs (source, mode, status, started_at) "
                "VALUES (?, ?, 'running', ?)",
                (source, mode, _timestamp(utc_now())),
            )
        if cursor.lastrowid is None:  # pragma: no cover - sqlite always sets it
            raise RuntimeError("could not create pipeline run")
        return cursor.lastrowid

    def finish_run(
        self,
        run_id: int,
        status: str,
        counters: RunCounters,
        error_message: str | None = None,
    ) -> None:
        """Store the final status and counters of a run."""
        if status not in RUN_STATUSES or status == "running":
            raise ValueError(f"invalid final status {status!r}")
        values = asdict(counters)
        assignments = ", ".join(f"{name} = :{name}" for name in values)
        with self.transaction() as conn:
            conn.execute(
                f"UPDATE pipeline_runs SET status = :status, finished_at = :finished_at, "
                f"error_message = :error_message, {assignments} WHERE id = :run_id",
                {
                    **values,
                    "status": status,
                    "finished_at": _timestamp(utc_now()),
                    "error_message": error_message,
                    "run_id": run_id,
                },
            )

    def add_run_errors(self, run_id: int, errors: Iterable[RunError]) -> None:
        now = _timestamp(utc_now())
        rows = [(run_id, e.symbol, e.stage, e.message, now) for e in errors]
        with self.transaction() as conn:
            conn.executemany(
                "INSERT INTO pipeline_run_errors (run_id, symbol, stage, message, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                rows,
            )

    def mark_stale_runs(self, older_than: timedelta = timedelta(hours=1)) -> int:
        """Mark runs stuck in ``running`` (e.g. after a crash) as failed."""
        now = utc_now()
        with self.transaction() as conn:
            cursor = conn.execute(
                "UPDATE pipeline_runs SET status = 'failed', finished_at = ?, "
                "error_message = 'Run did not finish (process stopped); marked as failed.' "
                "WHERE status = 'running' AND started_at < ?",
                (_timestamp(now), _timestamp(now - older_than)),
            )
        return cursor.rowcount

    # -- reads ---------------------------------------------------------------

    def _rows(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        return [dict(row) for row in self._conn.execute(sql, params)]

    def latest_trade_date(self, symbol: str) -> date | None:
        row = self._conn.execute(
            "SELECT MAX(trade_date) AS latest FROM daily_prices WHERE symbol = ?", (symbol,)
        ).fetchone()
        return date.fromisoformat(row["latest"]) if row["latest"] else None

    def count_prices(self, symbol: str | None = None) -> int:
        if symbol is None:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM daily_prices").fetchone()
        else:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM daily_prices WHERE symbol = ?", (symbol,)
            ).fetchone()
        return int(row["n"])

    def list_securities(self) -> list[dict[str, Any]]:
        return self._rows("SELECT symbol, name, exchange, sector FROM securities ORDER BY symbol")

    def price_history(
        self, symbol: str, start: date | None = None, end: date | None = None
    ) -> list[dict[str, Any]]:
        return self._rows(
            "SELECT trade_date, open, high, low, close, volume FROM daily_prices "
            "WHERE symbol = ? AND trade_date >= ? AND trade_date <= ? ORDER BY trade_date",
            (
                symbol,
                start.isoformat() if start else "0000-01-01",
                end.isoformat() if end else "9999-12-31",
            ),
        )

    def coverage(self) -> list[dict[str, Any]]:
        """Rows and date range per security (securities without prices included)."""
        return self._rows(
            "SELECT s.symbol, s.name, s.exchange, COUNT(p.id) AS rows, "
            "MIN(p.trade_date) AS first_date, MAX(p.trade_date) AS last_date "
            "FROM securities s LEFT JOIN daily_prices p ON p.symbol = s.symbol "
            "GROUP BY s.symbol, s.name, s.exchange ORDER BY s.symbol"
        )

    def list_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT ?", (limit,))

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        rows = self._rows("SELECT * FROM pipeline_runs WHERE id = ?", (run_id,))
        return rows[0] if rows else None

    def run_errors(self, run_id: int) -> list[dict[str, Any]]:
        return self._rows(
            "SELECT symbol, stage, message FROM pipeline_run_errors WHERE run_id = ? ORDER BY id",
            (run_id,),
        )

    def overview(self) -> dict[str, Any]:
        """Headline numbers for the dashboard."""
        row = self._conn.execute(
            "SELECT (SELECT COUNT(*) FROM securities) AS securities, "
            "(SELECT COUNT(*) FROM daily_prices) AS price_rows, "
            "(SELECT MIN(trade_date) FROM daily_prices) AS first_date, "
            "(SELECT MAX(trade_date) FROM daily_prices) AS last_date, "
            "(SELECT COUNT(*) FROM pipeline_runs) AS runs"
        ).fetchone()
        result = dict(row)
        latest = self.list_runs(limit=1)
        result["last_run"] = latest[0] if latest else None
        return result
