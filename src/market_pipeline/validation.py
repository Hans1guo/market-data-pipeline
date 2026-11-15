"""Turn raw text records into validated, typed domain objects.

Bad rows are never silently dropped: each one becomes a :class:`Rejection`
with a human-readable reason, which the pipeline stores with the run.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date
from typing import Generic, TypeVar

from market_pipeline.models import SOURCE_REF_FIELD, PriceBar, Security

T = TypeVar("T")

_SYMBOL_RE = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,19}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class ValidationError(ValueError):
    """A single record failed validation."""


@dataclass(frozen=True)
class Rejection:
    """A record that failed validation."""

    source_ref: str
    symbol: str
    reason: str


@dataclass
class ValidationResult(Generic[T]):
    """Valid objects plus the records that were rejected."""

    valid: list[T] = field(default_factory=list)
    rejected: list[Rejection] = field(default_factory=list)


def normalize_symbol(text: str) -> str:
    """Upper-case and check a ticker symbol."""
    symbol = text.strip().upper()
    if not _SYMBOL_RE.match(symbol):
        raise ValidationError(f"invalid symbol {text!r}")
    return symbol


def _require(raw: Mapping[str, str], name: str) -> str:
    value = (raw.get(name) or "").strip()
    if not value:
        raise ValidationError(f"missing value for '{name}'")
    return value


def _parse_date(raw: Mapping[str, str]) -> date:
    text = _require(raw, "trade_date")
    if not _DATE_RE.match(text):
        raise ValidationError(f"'trade_date' must be YYYY-MM-DD, got {text!r}")
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise ValidationError(f"'trade_date' is not a real date: {text!r}") from None


def _parse_price(raw: Mapping[str, str], name: str) -> float:
    text = _require(raw, name)
    try:
        value = float(text)
    except ValueError:
        raise ValidationError(f"'{name}' is not a number: {text!r}") from None
    if not math.isfinite(value) or value <= 0:
        raise ValidationError(f"'{name}' must be a positive number, got {text!r}")
    return value


def _parse_volume(raw: Mapping[str, str]) -> int:
    text = _require(raw, "volume")
    try:
        value = int(text)
    except ValueError:
        raise ValidationError(f"'volume' must be a whole number, got {text!r}") from None
    if value < 0:
        raise ValidationError(f"'volume' must not be negative, got {text!r}")
    return value


def parse_security(raw: Mapping[str, str]) -> Security:
    """Validate one security record."""
    sector = (raw.get("sector") or "").strip() or None
    return Security(
        symbol=normalize_symbol(_require(raw, "symbol")),
        name=_require(raw, "name"),
        exchange=_require(raw, "exchange").upper(),
        sector=sector,
    )


def parse_price_bar(raw: Mapping[str, str]) -> PriceBar:
    """Validate one daily price record, including OHLC consistency."""
    symbol = normalize_symbol(_require(raw, "symbol"))
    trade_date = _parse_date(raw)
    open_ = _parse_price(raw, "open")
    high = _parse_price(raw, "high")
    low = _parse_price(raw, "low")
    close = _parse_price(raw, "close")
    volume = _parse_volume(raw)
    if high < max(open_, close, low):
        raise ValidationError(f"high {high} is below open, close or low")
    if low > min(open_, close):
        raise ValidationError(f"low {low} is above open or close")
    return PriceBar(symbol, trade_date, open_, high, low, close, volume)


def _reject(raw: Mapping[str, str], reason: str) -> Rejection:
    return Rejection(
        source_ref=raw.get(SOURCE_REF_FIELD, "?"),
        symbol=(raw.get("symbol") or "").strip().upper(),
        reason=reason,
    )


def validate_securities(records: Iterable[Mapping[str, str]]) -> ValidationResult[Security]:
    """Validate security records; later duplicates of a symbol are rejected."""
    result: ValidationResult[Security] = ValidationResult()
    seen: set[str] = set()
    for raw in records:
        try:
            security = parse_security(raw)
        except ValidationError as exc:
            result.rejected.append(_reject(raw, str(exc)))
            continue
        if security.symbol in seen:
            result.rejected.append(_reject(raw, f"duplicate security {security.symbol}"))
            continue
        seen.add(security.symbol)
        result.valid.append(security)
    return result


def validate_price_bars(
    records: Iterable[Mapping[str, str]], symbol: str | None = None
) -> ValidationResult[PriceBar]:
    """Validate price records, sorted by symbol and date in the result.

    If ``symbol`` is given, rows for any other symbol are rejected. A second
    row for the same symbol and date is rejected as a duplicate; the first wins.
    """
    result: ValidationResult[PriceBar] = ValidationResult()
    seen: set[tuple[str, date]] = set()
    for raw in records:
        try:
            bar = parse_price_bar(raw)
        except ValidationError as exc:
            result.rejected.append(_reject(raw, str(exc)))
            continue
        if symbol is not None and bar.symbol != symbol:
            result.rejected.append(_reject(raw, f"expected symbol {symbol}, got {bar.symbol}"))
            continue
        key = (bar.symbol, bar.trade_date)
        if key in seen:
            result.rejected.append(
                _reject(raw, f"duplicate row for {bar.symbol} on {bar.trade_date.isoformat()}")
            )
            continue
        seen.add(key)
        result.valid.append(bar)
    result.valid.sort(key=lambda bar: (bar.symbol, bar.trade_date))
    return result
