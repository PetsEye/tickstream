import json
from pathlib import Path

from common.config import Settings


def _config_path() -> str:
    return str(Path(__file__).resolve().parents[2] / "config" / "config.yaml")


def test_defaults_come_from_yaml(monkeypatch):
    monkeypatch.setenv("TICKSTREAM_CONFIG_FILE", _config_path())
    settings = Settings()
    assert settings.kafka.topic == "trades"
    assert settings.producer.source == "coinbase"
    assert settings.timescale.jdbc_url.endswith(":5432/tickstream")
    assert settings.delta.enabled is True


def test_env_overrides_yaml(monkeypatch):
    monkeypatch.setenv("TICKSTREAM_CONFIG_FILE", _config_path())
    monkeypatch.setenv("TICKSTREAM_KAFKA__TOPIC", "override_topic")
    monkeypatch.setenv("TICKSTREAM_DELTA__ENABLED", "false")
    settings = Settings()
    assert settings.kafka.topic == "override_topic"
    assert settings.delta.enabled is False


def test_trade_serialization_roundtrip():
    from common.schemas import Side, Trade

    trade = Trade(
        trade_id="x:1",
        exchange="coinbase",
        symbol="btc-usd",
        price=1.5,
        quantity=2.0,
        side=Side.BUY,
        trade_ts=1,
        ingest_ts=2,
    )
    payload = json.loads(trade.to_kafka_value())
    assert payload["symbol"] == "BTC-USD"
    assert payload["side"] == "BUY"
