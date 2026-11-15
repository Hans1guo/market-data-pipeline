"""Command-line interface: ``mdp init-db | run | status | dashboard``."""

from __future__ import annotations

import argparse
import importlib.util
import logging
import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from market_pipeline import __version__
from market_pipeline.config import (
    DB_PATH_ENV,
    DEFAULT_DB_PATH,
    SAMPLE_DATASETS,
    default_db_path,
    sample_data_dir,
)
from market_pipeline.pipeline import RunSummary, run_pipeline
from market_pipeline.reporting import coverage_report
from market_pipeline.sources.csv_source import CsvSource
from market_pipeline.storage import Database

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_PARTIAL = 3  # 2 is reserved for argparse usage errors

DASHBOARD_APP = Path(__file__).resolve().parent / "dashboard" / "app.py"


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--db",
        type=Path,
        default=None,
        help=f"SQLite database file (default: ${DB_PATH_ENV} or {DEFAULT_DB_PATH})",
    )

    parser = argparse.ArgumentParser(
        prog="mdp", description="Market Data Pipeline: an offline market-data ETL demo."
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("-v", "--verbose", action="store_true", help="show per-symbol progress")
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("init-db", parents=[common], help="create the schema (safe to re-run)")

    run = commands.add_parser("run", parents=[common], help="run the ETL pipeline")
    source = run.add_mutually_exclusive_group()
    source.add_argument(
        "--sample",
        choices=SAMPLE_DATASETS,
        help="bundled synthetic dataset to load (default: base)",
    )
    source.add_argument(
        "--data-dir", type=Path, help="directory containing securities.csv and daily_prices.csv"
    )
    run.add_argument(
        "--full-refresh",
        action="store_true",
        help="re-check every row instead of only rows newer than the latest stored date",
    )
    run.add_argument("--symbols", help="comma-separated subset of symbols to load")

    status = commands.add_parser("status", parents=[common], help="show runs and data coverage")
    status.add_argument("--limit", type=int, default=10, help="number of runs to show")
    status.add_argument("--run-id", type=int, help="show the errors recorded for one run")

    dashboard = commands.add_parser("dashboard", parents=[common], help="open the dashboard")
    dashboard.add_argument("--port", type=int, default=8501)
    dashboard.add_argument(
        "--headless", action="store_true", help="start the server without opening a browser"
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )
    db_path: Path = args.db if args.db is not None else default_db_path()
    handlers = {
        "init-db": _cmd_init_db,
        "run": _cmd_run,
        "status": _cmd_status,
        "dashboard": _cmd_dashboard,
    }
    result: int = handlers[args.command](args, db_path)
    return result


def _cmd_init_db(args: argparse.Namespace, db_path: Path) -> int:
    with Database(db_path) as db:
        db.init_schema()
        tables = ", ".join(db.table_names())
    print(f"Database ready at {db_path} (tables: {tables})")
    return EXIT_OK


def _cmd_run(args: argparse.Namespace, db_path: Path) -> int:
    data_dir = args.data_dir or sample_data_dir(args.sample or "base")
    symbols = [s for s in args.symbols.split(",") if s.strip()] if args.symbols else None
    with Database(db_path) as db:
        summary = run_pipeline(
            db, CsvSource(data_dir), full_refresh=args.full_refresh, symbols=symbols
        )
    _print_summary(summary)
    if summary.status == "success":
        return EXIT_OK
    return EXIT_PARTIAL if summary.status == "partial_success" else EXIT_FAILED


def _print_summary(summary: RunSummary) -> None:
    c = summary.counters
    print(f"Run #{summary.run_id}: {summary.status} (mode={summary.mode}, source={summary.source})")
    print(
        f"  securities  loaded={c.securities_loaded} rejected={c.securities_rejected}\n"
        f"  symbols     planned={c.symbols_planned} succeeded={c.symbols_succeeded} "
        f"failed={c.symbols_failed}\n"
        f"  price rows  read={c.rows_read} inserted={c.rows_inserted} updated={c.rows_updated} "
        f"unchanged={c.rows_unchanged} skipped={c.rows_skipped} rejected={c.rows_rejected}"
    )
    if summary.error_message:
        print(f"  error: {summary.error_message}")
    shown = summary.errors[:10]
    if shown:
        print(f"  problems ({len(summary.errors)}):")
        for error in shown:
            print(f"    [{error.stage}] {error.symbol or '-'}: {error.message}")
        if len(summary.errors) > len(shown):
            print(f"    ... see `mdp status --run-id {summary.run_id}` for all")


def _cmd_status(args: argparse.Namespace, db_path: Path) -> int:
    if not db_path.exists():
        print(f"No database at {db_path}. Run `mdp init-db` or `mdp run` first.")
        return EXIT_FAILED
    with Database(db_path) as db:
        db.init_schema()
        if args.run_id is not None:
            return _print_run_errors(db, args.run_id)
        runs = db.list_runs(limit=args.limit)
        print("Recent pipeline runs")
        _print_table(
            [
                "id",
                "started (UTC)",
                "status",
                "mode",
                "inserted",
                "updated",
                "skipped",
                "rejected",
                "failed symbols",
            ],
            [
                [
                    r["id"],
                    r["started_at"],
                    r["status"],
                    r["mode"],
                    r["rows_inserted"],
                    r["rows_updated"],
                    r["rows_skipped"],
                    r["rows_rejected"],
                    r["symbols_failed"],
                ]
                for r in runs
            ],
        )
        print("\nData coverage")
        _print_table(
            ["symbol", "rows", "first date", "last date", "coverage %"],
            [
                [
                    r["symbol"],
                    r["rows"],
                    r["first_date"] or "-",
                    r["last_date"] or "-",
                    r["coverage_pct"] if r["coverage_pct"] is not None else "-",
                ]
                for r in coverage_report(db)
            ],
        )
    return EXIT_OK


def _print_run_errors(db: Database, run_id: int) -> int:
    run = db.get_run(run_id)
    if run is None:
        print(f"No run with id {run_id}.")
        return EXIT_FAILED
    print(
        f"Run #{run_id}: {run['status']}"
        + (f" - {run['error_message']}" if run["error_message"] else "")
    )
    errors = db.run_errors(run_id)
    if not errors:
        print("No errors recorded.")
    else:
        _print_table(
            ["stage", "symbol", "message"],
            [[e["stage"], e["symbol"] or "-", e["message"]] for e in errors],
        )
    return EXIT_OK


def _print_table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> None:
    if not rows:
        print("  (none)")
        return
    cells = [[str(value) for value in row] for row in rows]
    widths = [max(len(h), *(len(row[i]) for row in cells)) for i, h in enumerate(headers)]
    for line in [list(headers), ["-" * w for w in widths], *cells]:
        padded = (value.ljust(w) for value, w in zip(line, widths, strict=True))
        print(("  " + "  ".join(padded)).rstrip())


def _cmd_dashboard(args: argparse.Namespace, db_path: Path) -> int:
    if importlib.util.find_spec("streamlit") is None:
        print(
            'Streamlit is not installed. Install it with: pip install -e ".[dashboard]"',
            file=sys.stderr,
        )
        return EXIT_FAILED
    env = {**os.environ, DB_PATH_ENV: str(db_path.resolve())}
    command = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(DASHBOARD_APP),
        "--server.port",
        str(args.port),
        # Keep the demo fully offline: no usage statistics sent to Streamlit.
        "--browser.gatherUsageStats",
        "false",
        "--server.headless",
        "true" if args.headless else "false",
    ]
    return subprocess.call(command, env=env)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
