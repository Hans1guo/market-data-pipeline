"""Read-side helpers shared by the CLI and the dashboard."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from market_pipeline.storage import Database


def weekdays_between(start: date, end: date) -> int:
    """Number of Monday-Friday dates from ``start`` to ``end``, inclusive."""
    if end < start:
        return 0
    days = (end - start).days + 1
    full_weeks, remainder = divmod(days, 7)
    count = full_weeks * 5
    for offset in range(remainder):
        if (start + timedelta(days=full_weeks * 7 + offset)).weekday() < 5:
            count += 1
    return count


def coverage_report(db: Database) -> list[dict[str, Any]]:
    """Per-security coverage, comparing stored rows with expected weekdays.

    Expected weekdays ignore exchange holidays, so real data would show
    slightly under 100% even when complete.
    """
    report = []
    for row in db.coverage():
        expected = 0
        if row["first_date"] and row["last_date"]:
            expected = weekdays_between(
                date.fromisoformat(row["first_date"]), date.fromisoformat(row["last_date"])
            )
        row["expected_weekdays"] = expected
        row["coverage_pct"] = round(100 * row["rows"] / expected, 1) if expected else None
        report.append(row)
    return report
