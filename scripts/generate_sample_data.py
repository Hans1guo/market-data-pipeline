"""Generate the bundled synthetic dataset (deterministic; re-running gives identical files).

Prices follow a seeded geometric random walk on Monday-Friday dates. Nothing is
derived from real market data.

Usage:  python scripts/generate_sample_data.py
"""

from __future__ import annotations

import csv
import math
import random
from datetime import date, timedelta
from pathlib import Path

SEED = 20240101
OUT_DIR = Path(__file__).resolve().parents[1] / "src" / "market_pipeline" / "sample_data"

# symbol, name, exchange, sector, start price, daily drift, daily volatility, base volume
SECURITIES = [
    ("SYN-AUR", "Aurora Robotics (Synthetic)", "DEMO", "Industrials", 42.0, 0.0004, 0.018, 900_000),
    (
        "SYN-BLU",
        "Bluefin Logistics (Synthetic)",
        "DEMO",
        "Transportation",
        18.5,
        0.0002,
        0.014,
        1_400_000,
    ),
    (
        "SYN-COB",
        "Cobalt Semiconductors (Synthetic)",
        "DEMO",
        "Technology",
        96.0,
        0.0006,
        0.024,
        650_000,
    ),
    ("SYN-DLT", "Delta Grid Energy (Synthetic)", "DEMO", "Utilities", 27.3, 0.0001, 0.010, 800_000),
    (
        "SYN-EVR",
        "Evergreen Foods (Synthetic)",
        "DEMO",
        "Consumer Staples",
        55.8,
        0.0002,
        0.009,
        500_000,
    ),
    (
        "SYN-FJD",
        "Fjord Biotech (Synthetic)",
        "DEMO",
        "Health Care",
        12.4,
        -0.0001,
        0.030,
        2_100_000,
    ),
]
# Only present in the update batch, to show a new security being picked up.
NEW_SECURITY = (
    "SYN-GLC",
    "Glacier Minerals (Synthetic)",
    "DEMO",
    "Materials",
    33.0,
    0.0003,
    0.016,
    700_000,
)

BASE_DAYS = (date(2024, 1, 2), date(2024, 12, 31))
UPDATE_DAYS = (date(2025, 1, 2), date(2025, 1, 8))

SECURITY_HEADER = ["symbol", "name", "exchange", "sector"]
PRICE_HEADER = ["symbol", "trade_date", "open", "high", "low", "close", "volume"]


def weekdays(start: date, end: date) -> list[date]:
    days, current = [], start
    while current <= end:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def simulate(rng: random.Random, spec: tuple, days: list[date]) -> list[list[str]]:
    symbol, _, _, _, price, drift, vol, base_volume = spec
    rows, prev_close = [], price
    for day in days:
        open_ = round(max(prev_close * (1 + rng.gauss(0, vol / 4)), 0.5), 2)
        step = math.exp(drift - vol**2 / 2 + vol * rng.gauss(0, 1))
        close = round(max(prev_close * step, 0.5), 2)
        high = round(max(open_, close) * (1 + abs(rng.gauss(0, vol / 2))), 2)
        low = round(min(open_, close) * (1 - abs(rng.gauss(0, vol / 2))), 2)
        volume = int(base_volume * rng.lognormvariate(0, 0.35))
        rows.append(
            [
                symbol,
                day.isoformat(),
                f"{open_:.2f}",
                f"{high:.2f}",
                f"{low:.2f}",
                f"{close:.2f}",
                str(volume),
            ]
        )
        prev_close = close
    return rows


def write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def main() -> None:
    rng = random.Random(SEED)
    base_days = weekdays(*BASE_DAYS)
    update_days = weekdays(*UPDATE_DAYS)

    base_rows: list[list[str]] = []
    update_rows: list[list[str]] = []
    for spec in SECURITIES:
        rows = simulate(rng, spec, base_days + update_days)
        base_rows += rows[: len(base_days)]
        update_rows += rows[len(base_days) :]
    update_rows += simulate(rng, NEW_SECURITY, update_days)

    # A correction to an already-loaded day: only applied by --full-refresh.
    correction = next(r for r in base_rows if r[0] == "SYN-AUR" and r[1] == "2024-12-31").copy()
    corrected_close = float(correction[5]) + 1.00
    correction[5] = f"{corrected_close:.2f}"
    correction[3] = f"{max(float(correction[3]), corrected_close):.2f}"

    # Deliberately invalid rows, to demonstrate validation and partial success.
    invalid = [
        ["SYN-BLU", "2025-01-09", "11.00", "10.00", "12.00", "11.00", "900000"],  # high < low
        ["SYN-COB", "2025-13-01", "90.00", "91.00", "89.00", "90.50", "600000"],  # bad date
        ["SYN-DLT", "2025-01-09", "27.00", "27.50", "26.80", "27.20", "-500"],  # negative volume
    ]
    duplicate = next(r for r in update_rows if r[0] == "SYN-EVR" and r[1] == "2025-01-06")

    securities = [list(spec[:4]) for spec in SECURITIES]
    write_csv(OUT_DIR / "base" / "securities.csv", SECURITY_HEADER, securities)
    write_csv(OUT_DIR / "base" / "daily_prices.csv", PRICE_HEADER, base_rows)
    write_csv(
        OUT_DIR / "update" / "securities.csv",
        SECURITY_HEADER,
        [*securities, list(NEW_SECURITY[:4])],
    )
    write_csv(
        OUT_DIR / "update" / "daily_prices.csv",
        PRICE_HEADER,
        [*update_rows, correction, *invalid, list(duplicate)],
    )
    print(
        f"base: {len(base_rows)} price rows; update: {len(update_rows)} new rows, "
        f"1 correction, {len(invalid)} invalid, 1 duplicate"
    )


if __name__ == "__main__":
    main()
