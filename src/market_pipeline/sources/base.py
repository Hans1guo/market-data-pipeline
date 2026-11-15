"""The contract every data source implements."""

from __future__ import annotations

from datetime import date
from typing import Protocol

from market_pipeline.models import RawRecord


class DataSourceError(RuntimeError):
    """Raised when a source cannot be read at all (missing file, bad header, ...)."""


class DataSource(Protocol):
    """Interface between the pipeline and wherever market data comes from.

    Sources return *raw* text records; validation and typing happen later in
    :mod:`market_pipeline.validation`, so every source gets the same checks.

    Security records use the fields ``symbol``, ``name``, ``exchange`` and
    optionally ``sector``. Price records use ``symbol``, ``trade_date``
    (``YYYY-MM-DD``), ``open``, ``high``, ``low``, ``close`` and ``volume``.

    Only :class:`~market_pipeline.sources.csv_source.CsvSource` ships with the
    project. To add a documented public API later, implement this protocol:
    map the provider's response onto the field names above, read credentials
    from environment variables, respect the provider's terms and rate limits,
    and raise :class:`DataSourceError` for failures that affect the whole source.
    """

    #: Short identifier stored with each loaded row and pipeline run.
    name: str

    def list_securities(self) -> list[RawRecord]:
        """Return all securities the source knows about."""
        ...

    def fetch_prices(self, symbol: str, since: date | None = None) -> list[RawRecord]:
        """Return daily price records for one symbol.

        ``since`` is the latest date already stored. It is a hint that lets a
        remote source request less data; sources may ignore it, because the
        pipeline filters incrementally on its own.
        """
        ...
