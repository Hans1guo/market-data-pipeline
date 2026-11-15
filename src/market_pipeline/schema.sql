-- Market Data Pipeline schema (SQLite). Every statement is idempotent.

CREATE TABLE IF NOT EXISTS securities (
    symbol      TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    exchange    TEXT NOT NULL,
    sector      TEXT,
    created_at  TEXT NOT NULL,   -- UTC, ISO-8601
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_prices (
    id          INTEGER PRIMARY KEY,
    symbol      TEXT NOT NULL REFERENCES securities (symbol) ON DELETE CASCADE,
    trade_date  TEXT NOT NULL,   -- YYYY-MM-DD
    open        REAL NOT NULL CHECK (open > 0),
    high        REAL NOT NULL CHECK (high > 0),
    low         REAL NOT NULL CHECK (low > 0),
    close       REAL NOT NULL CHECK (close > 0),
    volume      INTEGER NOT NULL CHECK (volume >= 0),
    source      TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    UNIQUE (symbol, trade_date),
    CHECK (high >= low)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id                   INTEGER PRIMARY KEY,
    source               TEXT NOT NULL,
    mode                 TEXT NOT NULL CHECK (mode IN ('incremental', 'full')),
    status               TEXT NOT NULL
                         CHECK (status IN ('running', 'success', 'partial_success', 'failed')),
    started_at           TEXT NOT NULL,
    finished_at          TEXT,
    securities_loaded    INTEGER NOT NULL DEFAULT 0,
    securities_rejected  INTEGER NOT NULL DEFAULT 0,
    symbols_planned      INTEGER NOT NULL DEFAULT 0,
    symbols_succeeded    INTEGER NOT NULL DEFAULT 0,
    symbols_failed       INTEGER NOT NULL DEFAULT 0,
    rows_read            INTEGER NOT NULL DEFAULT 0,
    rows_rejected        INTEGER NOT NULL DEFAULT 0,
    rows_skipped         INTEGER NOT NULL DEFAULT 0,
    rows_inserted        INTEGER NOT NULL DEFAULT 0,
    rows_updated         INTEGER NOT NULL DEFAULT 0,
    rows_unchanged       INTEGER NOT NULL DEFAULT 0,
    error_message        TEXT
);

CREATE TABLE IF NOT EXISTS pipeline_run_errors (
    id          INTEGER PRIMARY KEY,
    run_id      INTEGER NOT NULL REFERENCES pipeline_runs (id) ON DELETE CASCADE,
    symbol      TEXT,
    stage       TEXT NOT NULL,
    message     TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs (status, started_at);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_errors_run ON pipeline_run_errors (run_id);
