"""Runtime configuration and locations of bundled resources."""

from __future__ import annotations

import os
from importlib import resources
from pathlib import Path

#: Environment variable that overrides the default database location.
DB_PATH_ENV = "MDP_DB_PATH"

#: Default SQLite file, relative to the current working directory.
DEFAULT_DB_PATH = Path("data") / "market_data.db"

#: Names of the synthetic datasets shipped inside the package.
SAMPLE_DATASETS = ("base", "update")


def default_db_path() -> Path:
    """Return the database path from ``MDP_DB_PATH``, or the default."""
    raw = os.environ.get(DB_PATH_ENV, "").strip()
    return Path(raw).expanduser() if raw else DEFAULT_DB_PATH


def sample_data_dir(name: str = "base") -> Path:
    """Return the directory of a bundled synthetic dataset.

    ``base`` holds one year of history; ``update`` is a follow-up batch with
    new trading days, one price correction and a few deliberately invalid rows.
    """
    if name not in SAMPLE_DATASETS:
        raise ValueError(f"unknown sample dataset {name!r}; choose from {SAMPLE_DATASETS}")
    package = resources.files("market_pipeline")
    return Path(str(package.joinpath("sample_data").joinpath(name)))
