from __future__ import annotations

import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def _bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def csv(name: str, default: str = "") -> list[str]:
    return [x.strip() for x in os.getenv(name, default).split(",") if x.strip()]


@dataclass(frozen=True, slots=True)
class Settings:
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
    clickhouse_host: str = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    clickhouse_port: int = _int("CLICKHOUSE_HTTP_INTERNAL_PORT", 8123)
    clickhouse_db: str = os.getenv("CLICKHOUSE_DB", "market")
    clickhouse_user: str = os.getenv("CLICKHOUSE_USER", "market_app")
    clickhouse_password: str = os.getenv("CLICKHOUSE_PASSWORD", "")
    clickhouse_query_final: bool = _bool("CLICKHOUSE_QUERY_FINAL", True)
    lakekeeper_uri: str = os.getenv("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog")
    iceberg_warehouse: str = os.getenv("ICEBERG_WAREHOUSE", "market-data")
    s3_endpoint: str = os.getenv("S3_ENDPOINT", "http://garage:3900")
    s3_access_key: str = os.getenv("S3_ACCESS_KEY", "GK0123456789abcdef01234567")
    s3_secret_key: str = os.getenv("S3_SECRET_KEY", "")
    s3_region: str = os.getenv("S3_REGION", "us-east-1")
    reconcile_interval_seconds: int = _int("RECONCILE_INTERVAL_SECONDS", 300)
    reconcile_lookback_minutes: int = _int("RECONCILE_LOOKBACK_MINUTES", 15)
    reconcile_settle_delay_seconds: int = _int("RECONCILE_SETTLE_DELAY_SECONDS", 45)
    reconcile_tolerance_count: int = _int("RECONCILE_TOLERANCE_COUNT", 0)
    reconcile_tolerance_volume_pct: float = _float("RECONCILE_TOLERANCE_VOLUME_PCT", 0.0001)
    reconcile_tolerance_notional_pct: float = _float("RECONCILE_TOLERANCE_NOTIONAL_PCT", 0.0001)
    replay_max_events: int = _int("REPLAY_MAX_EVENTS", 500_000)
    replay_batch_size: int = _int("REPLAY_BATCH_SIZE", 5_000)
    api_max_history_rows: int = _int("API_MAX_HISTORY_ROWS", 10_000)
    analytics_default_bars: int = _int("ANALYTICS_DEFAULT_BARS", 300)
    book_max_levels: int = _int("BOOK_MAX_LEVELS", 1000)
    book_feature_levels: int = _int("BOOK_FEATURE_LEVELS", 20)
    book_trade_window_seconds: int = _int("BOOK_TRADE_WINDOW_SECONDS", 60)
    book_dedup_events: int = _int("BOOK_DEDUP_EVENTS", 50_000)


def settings() -> Settings:
    return Settings()
