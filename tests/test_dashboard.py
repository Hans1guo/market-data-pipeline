"""Smoke tests that render the Streamlit app headlessly with AppTest."""

from __future__ import annotations

from pathlib import Path

import pytest

from market_pipeline.cli import DASHBOARD_APP
from market_pipeline.config import sample_data_dir
from market_pipeline.pipeline import run_pipeline
from market_pipeline.sources import CsvSource
from market_pipeline.storage import Database

pytest.importorskip("streamlit")
pytest.importorskip("plotly")

from streamlit.testing.v1 import AppTest


@pytest.fixture
def db_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "dash.db"
    monkeypatch.setenv("MDP_DB_PATH", str(path))
    return path


def test_empty_database_offers_sample_load(db_path: Path) -> None:
    app = AppTest.from_file(str(DASHBOARD_APP), default_timeout=60).run()
    assert not app.exception
    assert app.title[0].value == "Market Data Pipeline"
    assert "no price data" in app.warning[0].value

    app.button[0].click().run()
    assert not app.exception
    assert [m.label for m in app.metric][:2] == ["Securities", "Daily price rows"]


def test_populated_database_renders_all_sections(db_path: Path) -> None:
    with Database(db_path) as db:
        run_pipeline(db, CsvSource(sample_data_dir("base")))
        run_pipeline(db, CsvSource(sample_data_dir("update")))

    app = AppTest.from_file(str(DASHBOARD_APP), default_timeout=60).run()

    assert not app.exception
    assert "synthetic" in app.info[0].value
    metrics = {m.label: m.value for m in app.metric}
    assert metrics["Securities"] == "7"
    assert metrics["Daily price rows"] == "1,601"
    assert metrics["Latest trade date"] == "2025-01-08"
    assert metrics["Last pipeline run"] == "Partial"
    assert [t.label for t in app.tabs] == ["Prices", "Data coverage", "Pipeline runs"]
    assert app.selectbox[0].value == "SYN-AUR"
    assert len(app.dataframe) >= 2  # coverage + run history

    app.selectbox[0].set_value("SYN-GLC").run()
    assert not app.exception
