from __future__ import annotations

from pathlib import Path

import pytest

from market_pipeline.cli import EXIT_FAILED, EXIT_OK, EXIT_PARTIAL, main


def test_init_db_creates_database(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db_path = tmp_path / "nested" / "market.db"
    assert main(["init-db", "--db", str(db_path)]) == EXIT_OK
    assert db_path.exists()
    assert "daily_prices" in capsys.readouterr().out


def test_run_and_status(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db_path = str(tmp_path / "market.db")
    assert main(["run", "--db", db_path]) == EXIT_OK
    out = capsys.readouterr().out
    assert "Run #1: success" in out
    assert "inserted=1566" in out

    assert main(["status", "--db", db_path]) == EXIT_OK
    out = capsys.readouterr().out
    assert "Recent pipeline runs" in out
    assert "SYN-AUR" in out
    assert "100.0" in out


def test_partial_run_exit_code_and_error_listing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = str(tmp_path / "market.db")
    main(["run", "--db", db_path])
    assert main(["run", "--db", db_path, "--sample", "update"]) == EXIT_PARTIAL
    assert "partial_success" in capsys.readouterr().out

    assert main(["status", "--db", db_path, "--run-id", "2"]) == EXIT_OK
    out = capsys.readouterr().out
    assert "validate_price" in out
    assert "2025-13-01" in out


def test_bad_data_dir_fails(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["run", "--db", str(tmp_path / "m.db"), "--data-dir", str(tmp_path / "none")])
    assert code == EXIT_FAILED
    assert "CSV file not found" in capsys.readouterr().out


def test_status_without_database(tmp_path: Path) -> None:
    assert main(["status", "--db", str(tmp_path / "absent.db")]) == EXIT_FAILED


def test_symbols_option(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db_path = str(tmp_path / "market.db")
    assert main(["run", "--db", db_path, "--symbols", "SYN-AUR,SYN-BLU"]) == EXIT_OK
    assert "inserted=522" in capsys.readouterr().out


def test_db_path_from_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "from_env.db"
    monkeypatch.setenv("MDP_DB_PATH", str(db_path))
    assert main(["init-db"]) == EXIT_OK
    assert db_path.exists()
