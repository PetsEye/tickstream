"""Typed application configuration.

Precedence (highest first): explicit init args, environment variables, .env file,
YAML file, defaults. Environment variables use the ``TICKSTREAM_`` prefix and a
double underscore for nesting, e.g. ``TICKSTREAM_KAFKA__BOOTSTRAP_SERVERS``.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)

CONFIG_ENV_VAR = "TICKSTREAM_CONFIG_FILE"
DEFAULT_CONFIG_PATH = Path("config/config.yaml")


class AppSettings(BaseModel):
    name: str = "tickstream"
    log_level: str = "INFO"


class KafkaSettings(BaseModel):
    bootstrap_servers: str = "localhost:9092"
    topic: str = "trades"
    dead_letter_topic: str = "trades_dlq"
    client_id: str = "tickstream-producer"


class SyntheticSettings(BaseModel):
    file: str = "data/sample_trades.jsonl"
    speed: float = 50.0
    loop: bool = True


class ProducerSettings(BaseModel):
    source: Literal["coinbase", "binance", "kraken", "synthetic"] = "coinbase"
    symbols: list[str] = Field(default_factory=lambda: ["BTC-USD", "ETH-USD"])
    reconnect_backoff_seconds: float = 5.0
    synthetic: SyntheticSettings = Field(default_factory=SyntheticSettings)


class SparkSettings(BaseModel):
    app_name: str = "tickstream-streaming"
    kafka_bootstrap_servers: str = "localhost:9092"
    topic: str = "trades"
    checkpoint_dir: str = "/tmp/tickstream/checkpoints"
    trigger_interval: str = "5 seconds"
    watermark_delay: str = "15 seconds"
    window_duration: str = "1 minute"
    window_slide: str = "1 minute"
    max_offsets_per_trigger: int = 5000


class TimescaleSettings(BaseModel):
    host: str = "localhost"
    port: int = 5432
    database: str = "tickstream"
    user: str = "tickstream"
    password: str = "tickstream"
    batch_size: int = 500

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"


class DeltaSettings(BaseModel):
    enabled: bool = True
    base_path: str = "/tmp/tickstream/delta"


class AnomalySettings(BaseModel):
    enabled: bool = True
    window: str = "5 minutes"
    slide: str = "1 minute"
    zscore_threshold: float = 3.0
    min_trades: int = 20


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="TICKSTREAM_",
        env_nested_delimiter="__",
        extra="ignore",
        case_sensitive=False,
    )

    app: AppSettings = Field(default_factory=AppSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    producer: ProducerSettings = Field(default_factory=ProducerSettings)
    spark: SparkSettings = Field(default_factory=SparkSettings)
    timescale: TimescaleSettings = Field(default_factory=TimescaleSettings)
    delta: DeltaSettings = Field(default_factory=DeltaSettings)
    anomaly: AnomalySettings = Field(default_factory=AnomalySettings)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        path = _config_path()
        yaml_source = YamlConfigSettingsSource(
            settings_cls, yaml_file=path if path.exists() else None
        )
        return (init_settings, env_settings, dotenv_settings, yaml_source, file_secret_settings)


def _config_path() -> Path:
    return Path(os.environ.get(CONFIG_ENV_VAR, DEFAULT_CONFIG_PATH))


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings singleton."""
    return Settings()


def load_config() -> Settings:
    """Alias for :func:`get_settings` for readability at call sites."""
    return get_settings()
