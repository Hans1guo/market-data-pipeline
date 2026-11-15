"""Pipeline orchestration: extract -> validate -> filter -> load, with run logging.

Each symbol is processed independently, so one failing symbol does not stop
the others. Every run is recorded in ``pipeline_runs`` with its counters and
in ``pipeline_run_errors`` with the individual problems.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import timedelta

from market_pipeline.incremental import split_incremental
from market_pipeline.models import RunCounters, RunError
from market_pipeline.sources.base import DataSource
from market_pipeline.storage import Database
from market_pipeline.validation import (
    ValidationError,
    normalize_symbol,
    validate_price_bars,
    validate_securities,
)

logger = logging.getLogger(__name__)

#: Upper bound on error rows stored per run, so a broken file cannot flood the table.
MAX_STORED_ERRORS = 200


@dataclass
class RunSummary:
    """Outcome of one pipeline run."""

    run_id: int
    status: str
    mode: str
    source: str
    counters: RunCounters
    error_message: str | None = None
    errors: list[RunError] = field(default_factory=list)


def derive_status(counters: RunCounters) -> str:
    """Map counters to ``success``, ``partial_success`` or ``failed``.

    A run fails when nothing was loaded successfully. It is a partial success
    when some symbols failed or some rows were rejected.
    """
    if counters.symbols_planned == 0 or counters.symbols_succeeded == 0:
        return "failed"
    if counters.symbols_failed or counters.rows_rejected or counters.securities_rejected:
        return "partial_success"
    return "success"


def run_pipeline(
    db: Database,
    source: DataSource,
    *,
    full_refresh: bool = False,
    symbols: Sequence[str] | None = None,
    stale_after: timedelta = timedelta(hours=1),
) -> RunSummary:
    """Run the pipeline once and return its summary.

    Errors are captured in the run log instead of being raised, except for
    errors writing the run log itself.
    """
    db.init_schema()
    stale = db.mark_stale_runs(stale_after)
    if stale:
        logger.warning("Marked %d unfinished run(s) as failed", stale)

    mode = "full" if full_refresh else "incremental"
    run_id = db.start_run(source.name, mode)
    logger.info("Run %d started (source=%s, mode=%s)", run_id, source.name, mode)

    counters = RunCounters()
    errors: list[RunError] = []
    error_message: str | None = None
    try:
        _execute(db, source, counters, errors, full_refresh=full_refresh, symbols=symbols)
    except Exception as exc:  # whole-run failure: unreadable source, database error, ...
        error_message = f"{type(exc).__name__}: {exc}"
        errors.append(RunError(None, "run", error_message))
        status = "failed"
        logger.error("Run %d failed: %s", run_id, error_message)
    else:
        status = derive_status(counters)
        if status == "failed":
            error_message = (
                "No securities to process."
                if counters.symbols_planned == 0
                else f"All {counters.symbols_planned} symbol(s) failed to load."
            )

    stored = errors[:MAX_STORED_ERRORS]
    if len(errors) > MAX_STORED_ERRORS:
        stored.append(
            RunError(None, "run", f"{len(errors) - MAX_STORED_ERRORS} more error(s) not stored")
        )
    db.add_run_errors(run_id, stored)
    db.finish_run(run_id, status, counters, error_message)
    logger.info("Run %d finished with status %s", run_id, status)
    return RunSummary(run_id, status, mode, source.name, counters, error_message, errors)


def _execute(
    db: Database,
    source: DataSource,
    counters: RunCounters,
    errors: list[RunError],
    *,
    full_refresh: bool,
    symbols: Sequence[str] | None,
) -> None:
    securities = validate_securities(source.list_securities())
    for rejection in securities.rejected:
        errors.append(
            RunError(
                rejection.symbol or None,
                "validate_security",
                f"{rejection.source_ref}: {rejection.reason}",
            )
        )
    counters.securities_rejected = len(securities.rejected)
    db.upsert_securities(securities.valid)
    counters.securities_loaded = len(securities.valid)

    known = [security.symbol for security in securities.valid]
    targets = known if symbols is None else _select(symbols, known, counters, errors)
    counters.symbols_planned += len(targets)

    for symbol in targets:
        try:
            watermark = None if full_refresh else db.latest_trade_date(symbol)
            raw = source.fetch_prices(symbol, since=watermark)
            counters.rows_read += len(raw)

            result = validate_price_bars(raw, symbol=symbol)
            counters.rows_rejected += len(result.rejected)
            errors.extend(
                RunError(symbol, "validate_price", f"{r.source_ref}: {r.reason}")
                for r in result.rejected
            )

            split = split_incremental(result.valid, watermark)
            counters.rows_skipped += len(split.skipped)
            stats = db.upsert_price_bars(split.new, source=source.name)
            counters.rows_inserted += stats.inserted
            counters.rows_updated += stats.updated
            counters.rows_unchanged += stats.unchanged
            counters.symbols_succeeded += 1
            logger.info(
                "%s: %d inserted, %d updated, %d skipped, %d rejected",
                symbol,
                stats.inserted,
                stats.updated,
                len(split.skipped),
                len(result.rejected),
            )
        except Exception as exc:  # isolate per-symbol failures
            counters.symbols_failed += 1
            errors.append(RunError(symbol, "load", f"{type(exc).__name__}: {exc}"))
            logger.warning("%s failed: %s", symbol, exc)


def _select(
    requested: Sequence[str],
    known: Sequence[str],
    counters: RunCounters,
    errors: list[RunError],
) -> list[str]:
    """Resolve a user-supplied symbol subset; unknown symbols count as failures."""
    targets: list[str] = []
    for text in requested:
        try:
            symbol = normalize_symbol(text)
        except ValidationError as exc:
            symbol, problem = text, str(exc)
        else:
            if symbol in targets:
                continue
            if symbol in known:
                targets.append(symbol)
                continue
            problem = "symbol not found in the source's securities list"
        counters.symbols_planned += 1
        counters.symbols_failed += 1
        errors.append(RunError(symbol, "select", problem))
    return targets
