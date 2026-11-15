"""Domain objects shared across the pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

#: Key added to every raw record that points back to its origin
#: (for CSV files: ``"<file>:<line>"``), used in validation messages.
SOURCE_REF_FIELD = "_source_ref"

#: A raw record as delivered by a data source: field name -> text value.
RawRecord = dict[str, str]


@dataclass(frozen=True)
class Security:
    """A tradable instrument."""

    symbol: str
    name: str
    exchange: str
    sector: str | None = None


@dataclass(frozen=True)
class PriceBar:
    """One validated daily OHLCV bar."""

    symbol: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass(frozen=True)
class RunError:
    """A problem recorded against a pipeline run."""

    symbol: str | None
    stage: str
    message: str


@dataclass
class RunCounters:
    """Counts collected while a pipeline run executes.

    ``rows_*`` counters refer to daily price rows.
    """

    securities_loaded: int = 0
    securities_rejected: int = 0
    symbols_planned: int = 0
    symbols_succeeded: int = 0
    symbols_failed: int = 0
    rows_read: int = 0
    rows_rejected: int = 0
    rows_skipped: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_unchanged: int = 0
