-- tickstream schema: TimescaleDB hypertables, staging tables, and the Phase 2
-- NOTIFY trigger used to push live updates to the API over SSE.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Raw trades: a plain table so trade_id stays globally unique for idempotent
-- upserts (hypertables require the time column in every unique index).
CREATE TABLE IF NOT EXISTS trades (
    trade_id   TEXT PRIMARY KEY,
    exchange   TEXT             NOT NULL,
    symbol     TEXT             NOT NULL,
    price      DOUBLE PRECISION NOT NULL,
    quantity   DOUBLE PRECISION NOT NULL,
    side       TEXT             NOT NULL,
    trade_ts   TIMESTAMPTZ      NOT NULL,
    ingest_ts  TIMESTAMPTZ      NOT NULL
);

-- One-minute OHLCV candles produced by Spark windowed aggregation.
CREATE TABLE IF NOT EXISTS candles (
    symbol       TEXT             NOT NULL,
    window_start TIMESTAMPTZ      NOT NULL,
    window_end   TIMESTAMPTZ      NOT NULL,
    open         DOUBLE PRECISION NOT NULL,
    high         DOUBLE PRECISION NOT NULL,
    low          DOUBLE PRECISION NOT NULL,
    close        DOUBLE PRECISION NOT NULL,
    volume       DOUBLE PRECISION NOT NULL,
    buy_volume   DOUBLE PRECISION NOT NULL,
    sell_volume  DOUBLE PRECISION NOT NULL,
    vwap         DOUBLE PRECISION NOT NULL,
    trade_count  BIGINT           NOT NULL,
    updated_at   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, window_start)
);

-- Statistical anomalies emitted by the streaming detector.
CREATE TABLE IF NOT EXISTS anomalies (
    symbol       TEXT             NOT NULL,
    window_start TIMESTAMPTZ      NOT NULL,
    window_end   TIMESTAMPTZ      NOT NULL,
    metric       TEXT             NOT NULL,
    value        DOUBLE PRECISION NOT NULL,
    mean         DOUBLE PRECISION NOT NULL,
    stddev       DOUBLE PRECISION NOT NULL,
    zscore       DOUBLE PRECISION NOT NULL,
    detected_at  TIMESTAMPTZ      NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, window_start, metric)
);

-- Staging tables: Spark's JDBC writer lands each micro-batch here, then a small
-- transactional INSERT ... ON CONFLICT merges it into the target tables. This
-- keeps writes parallel/scalable while making them idempotent (exactly-once).
CREATE TABLE IF NOT EXISTS candles_staging (LIKE candles INCLUDING DEFAULTS);
CREATE TABLE IF NOT EXISTS anomalies_staging (LIKE anomalies INCLUDING DEFAULTS);

SELECT create_hypertable('candles', 'window_start', if_not_exists => TRUE);
SELECT create_hypertable('anomalies', 'window_start', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts ON trades (symbol, trade_ts DESC);
CREATE INDEX IF NOT EXISTS idx_candles_symbol_ts ON candles (symbol, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_symbol_ts ON anomalies (symbol, window_start DESC);

-- Emit the full row as JSON whenever a candle is written so the FastAPI layer
-- can stream updates to the React terminal without polling.
CREATE OR REPLACE FUNCTION notify_candle_change() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('tickstream_candles', row_to_json(NEW)::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS candles_notify ON candles;
CREATE TRIGGER candles_notify
    AFTER INSERT OR UPDATE ON candles
    FOR EACH ROW EXECUTE FUNCTION notify_candle_change();

-- Same for anomalies.
CREATE OR REPLACE FUNCTION notify_anomaly_change() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('tickstream_anomalies', row_to_json(NEW)::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS anomalies_notify ON anomalies;
CREATE TRIGGER anomalies_notify
    AFTER INSERT OR UPDATE ON anomalies
    FOR EACH ROW EXECUTE FUNCTION notify_anomaly_change();
