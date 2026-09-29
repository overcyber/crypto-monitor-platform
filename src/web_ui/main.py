from __future__ import annotations

import asyncio
import os
import shutil
import time
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import yaml
from fastapi import Body, FastAPI, Header, HTTPException, Query
from fastapi.responses import HTMLResponse

from src.common.config import settings
from src.common.markets import load_market_config

app = FastAPI(title="Crypto Monitor Control Center", version="4.3.6")

MONITOR_URL = os.getenv("MONITOR_API_INTERNAL_URL", "http://monitor-api:8081").rstrip("/")
ANALYTICS_URL = os.getenv("ANALYTICS_API_INTERNAL_URL", "http://analytics-api:8082").rstrip("/")
REPLAY_URL = os.getenv("REPLAY_API_INTERNAL_URL", "http://replay-api:8084").rstrip("/")
FLINK_URL = os.getenv("FLINK_INTERNAL_URL", "http://flink-jobmanager:8081").rstrip("/")
LAKEKEEPER_URL = os.getenv("LAKEKEEPER_INTERNAL_URL", "http://lakekeeper:8181").rstrip("/")
GRAFANA_URL = os.getenv("GRAFANA_INTERNAL_URL", "http://grafana:3000").rstrip("/")
MARKETS_PATH = Path(os.getenv("MARKET_CONFIG", "/opt/app/config/markets.yaml"))
ALERTS_PATH = Path(os.getenv("ALERT_CONFIG", "/opt/app/config/alerts.yaml"))
TELEGRAM_PATH = Path(os.getenv("TELEGRAM_CONFIG", "/opt/app/config/telegram.yaml"))
INDEX_PATH = Path(__file__).with_name("index.html")
SYSTEM_CACHE_SECONDS = max(5, int(os.getenv("WEB_UI_SYSTEM_CACHE_SECONDS", "30")))
REFRESH_SECONDS = max(2, int(os.getenv("WEB_UI_REFRESH_SECONDS", "5")))
CONFIG_WRITE_ENABLED = os.getenv("WEB_UI_CONFIG_WRITE_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
BIND_ADDRESS = os.getenv("BIND_ADDRESS", "127.0.0.1").strip()
ADMIN_TOKEN = os.getenv("WEB_UI_ADMIN_TOKEN", "").strip()
ADMIN_TOKEN_REQUIRED = BIND_ADDRESS not in {"127.0.0.1", "localhost", "::1"}
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
_cache: dict[str, tuple[float, Any]] = {}
_cache_locks: dict[str, asyncio.Lock] = {}

# Web UI queries may run concurrently. Never reuse a clickhouse-connect session here;
# each query receives its own client/session and closes it immediately afterwards.
def _ch_sync(query: str) -> list[dict[str, Any]]:
    import clickhouse_connect
    cfg = settings()
    client = clickhouse_connect.get_client(
        host=cfg.clickhouse_host,
        port=cfg.clickhouse_port,
        username=cfg.clickhouse_user,
        password=cfg.clickhouse_password,
        database=cfg.clickhouse_db,
        connect_timeout=5,
        send_receive_timeout=30,
    )
    try:
        result = client.query(query)
        return list(result.named_results())
    finally:
        try:
            client.close()
        except Exception:
            pass


async def _get(url: str, *, params: dict[str, Any] | None = None) -> Any:
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(url, params=params)
        if r.status_code >= 400:
            return {"ok": False, "status_code": r.status_code, "error": r.text[:1000]}
        try:
            data = r.json()
        except Exception:
            data = r.text
        return {"ok": True, "data": data}
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def _yaml(path: Path) -> dict[str, Any]:
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _backup(path: Path) -> None:
    if not path.exists():
        return
    backup_dir = path.parent / ".web-ui-backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    shutil.copy2(path, backup_dir / f"{path.name}.{stamp}.bak")
    old = sorted(backup_dir.glob(f"{path.name}.*.bak"), reverse=True)
    for stale in old[20:]:
        stale.unlink(missing_ok=True)


def _write_guard(token: str | None) -> None:
    if not CONFIG_WRITE_ENABLED:
        raise HTTPException(status_code=403, detail="web configuration writes are disabled")
    if ADMIN_TOKEN_REQUIRED and not ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="WEB_UI_ADMIN_TOKEN is required when BIND_ADDRESS is not loopback")
    if ADMIN_TOKEN and (not token or not secrets.compare_digest(token, ADMIN_TOKEN)):
        raise HTTPException(status_code=401, detail="invalid web UI admin token")


def _atomic_yaml(path: Path, data: dict[str, Any]) -> None:
    _backup(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")
    os.replace(tmp, path)
    _cache.clear()


def _list_str(value: Any, name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or any(not isinstance(x, str) for x in value):
        raise HTTPException(status_code=422, detail=f"{name} must be a list of strings")
    return [x.strip() for x in value if x.strip()]


def _validate_markets(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise HTTPException(status_code=422, detail="markets configuration must be an object")
    out = dict(data)
    watchlist = [x.upper() for x in _list_str(out.get("watchlist", []), "watchlist")]
    if len(set(watchlist)) != len(watchlist):
        watchlist = list(dict.fromkeys(watchlist))
    out["watchlist"] = watchlist
    reload_seconds = int(out.get("reload_seconds", 5))
    if reload_seconds < 1 or reload_seconds > 3600:
        raise HTTPException(status_code=422, detail="reload_seconds must be between 1 and 3600")
    out["reload_seconds"] = reload_seconds
    out["version"] = int(out.get("version", 1))
    sources = out.get("sources") or {}
    if not isinstance(sources, dict):
        raise HTTPException(status_code=422, detail="sources must be an object")
    for venue, cfg in sources.items():
        if not isinstance(cfg, dict):
            raise HTTPException(status_code=422, detail=f"sources.{venue} must be an object")
        cfg["enabled"] = bool(cfg.get("enabled", True))
        cfg["quote"] = str(cfg.get("quote") or ("USD" if venue == "coinbase" else "USDT")).upper()
        cfg["products"] = [x.upper() for x in _list_str(cfg.get("products", []), f"sources.{venue}.products")]
        cfg["channels"] = _list_str(cfg.get("channels", []), f"sources.{venue}.channels")
        if "snapshot_limit" in cfg:
            cfg["snapshot_limit"] = int(cfg["snapshot_limit"])
            if cfg["snapshot_limit"] < 1 or cfg["snapshot_limit"] > 5000:
                raise HTTPException(status_code=422, detail=f"sources.{venue}.snapshot_limit out of range")
    out["sources"] = sources
    return out


def _validate_alerts(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise HTTPException(status_code=422, detail="alerts configuration must be an object")
    rules = data.get("rules") or []
    if not isinstance(rules, list):
        raise HTTPException(status_code=422, detail="rules must be a list")
    allowed_ops = {"gt", "gte", "lt", "lte", "abs_gt", "abs_gte"}
    allowed_sources = {"market", "microstructure"}
    allowed_sev = {"info", "warning", "critical"}
    ids: set[str] = set()
    cleaned: list[dict[str, Any]] = []
    for idx, rule in enumerate(rules):
        if not isinstance(rule, dict):
            raise HTTPException(status_code=422, detail=f"rule {idx} must be an object")
        r = {k: v for k, v in rule.items() if v not in (None, "")}
        rid = str(r.get("id", "")).strip()
        if not rid or rid in ids:
            raise HTTPException(status_code=422, detail=f"rule {idx} has missing/duplicate id")
        ids.add(rid)
        r["id"] = rid
        r["enabled"] = bool(r.get("enabled", True))
        r["source"] = str(r.get("source", "market")).lower()
        r["operator"] = str(r.get("operator", "gte")).lower()
        r["severity"] = str(r.get("severity", "warning")).lower()
        if r["source"] not in allowed_sources or r["operator"] not in allowed_ops or r["severity"] not in allowed_sev:
            raise HTTPException(status_code=422, detail=f"rule {rid} has invalid source/operator/severity")
        if not str(r.get("field", "")).strip():
            raise HTTPException(status_code=422, detail=f"rule {rid} requires field")
        if "value" not in r:
            raise HTTPException(status_code=422, detail=f"rule {rid} requires value")
        r["value"] = float(r["value"])
        r["cooldown_seconds"] = max(0, int(r.get("cooldown_seconds", 60)))
        if "venue" in r:
            r["venue"] = str(r["venue"]).lower()
        if "symbol" in r:
            r["symbol"] = str(r["symbol"]).upper()
        if "event_type" in r:
            r["event_type"] = str(r["event_type"]).lower()
        cleaned.append(r)
    return {"rules": cleaned}


def _validate_telegram(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise HTTPException(status_code=422, detail="telegram configuration must be an object")
    out = dict(data)
    out["version"] = int(out.get("version", 1))
    defaults = dict(out.get("defaults") or {})
    defaults["venue"] = str(defaults.get("venue", "binance")).lower()
    defaults["quote"] = str(defaults.get("quote", "USDT")).upper()
    defaults["cooldown_seconds"] = max(0, int(defaults.get("cooldown_seconds", 60)))
    out["defaults"] = defaults
    forwarding = dict(out.get("forward_alerts") or {})
    forwarding["enabled"] = bool(forwarding.get("enabled", True))
    severities = [x.lower() for x in _list_str(forwarding.get("severities", ["critical"]), "forward_alerts.severities")]
    forwarding["severities"] = [x for x in severities if x in {"info", "warning", "critical"}]
    out["forward_alerts"] = forwarding
    rules = out.get("price_alerts") or {}
    if not isinstance(rules, dict):
        raise HTTPException(status_code=422, detail="price_alerts must be an object")
    clean_rules: dict[str, Any] = {}
    for asset, rule in rules.items():
        if not isinstance(rule, dict):
            raise HTTPException(status_code=422, detail=f"price_alerts.{asset} must be an object")
        a = "".join(ch for ch in str(asset).upper() if ch.isalnum())
        if not a:
            raise HTTPException(status_code=422, detail="invalid price alert asset")
        r = {k: v for k, v in rule.items() if v not in (None, "")}
        r["enabled"] = bool(r.get("enabled", True))
        r["venue"] = str(r.get("venue", defaults["venue"])).lower()
        r["symbol"] = str(r.get("symbol", f"{a}{defaults['quote']}")).upper()
        r["cooldown_seconds"] = max(0, int(r.get("cooldown_seconds", defaults["cooldown_seconds"])))
        for key in ("percent_up", "percent_down", "high", "low"):
            if key in r:
                r[key] = float(r[key])
        clean_rules[a] = r
    out["price_alerts"] = clean_rules
    return out


async def _ch(query: str) -> list[dict[str, Any]]:
    try:
        return await asyncio.to_thread(_ch_sync, query)
    except Exception as exc:
        return [{"error": f"{type(exc).__name__}: {exc}"}]


async def _cached(key: str, producer):
    now = time.monotonic()
    hit = _cache.get(key)
    if hit and now - hit[0] < SYSTEM_CACHE_SECONDS:
        return hit[1]
    lock = _cache_locks.setdefault(key, asyncio.Lock())
    async with lock:
        now = time.monotonic()
        hit = _cache.get(key)
        if hit and now - hit[0] < SYSTEM_CACHE_SECONDS:
            return hit[1]
        value = await producer()
        _cache[key] = (now, value)
        return value


async def _kafka_inventory() -> dict[str, Any]:
    from aiokafka import AIOKafkaConsumer
    consumer = AIOKafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP, request_timeout_ms=5000)
    try:
        await consumer.start()
        topics = sorted(t for t in await consumer.topics() if t.startswith("market."))
        return {"ok": True, "topics": [{"topic": t, "partitions": len(consumer.partitions_for_topic(t) or set())} for t in topics]}
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "topics": []}
    finally:
        try:
            await consumer.stop()
        except Exception:
            pass


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    return HTMLResponse(INDEX_PATH.read_text(encoding="utf-8").replace("__REFRESH_MS__", str(REFRESH_SECONDS * 1000)))


@app.get("/health")
async def health() -> dict[str, Any]:
    q = await _get(f"{MONITOR_URL}/health")
    return {"status": "ok" if q.get("ok") else "degraded", "service": "web-ui", "monitor_api": q.get("ok", False)}


@app.get("/api/config")
async def config() -> dict[str, Any]:
    return {
        "write_enabled": CONFIG_WRITE_ENABLED,
        "write_ready": CONFIG_WRITE_ENABLED and (not ADMIN_TOKEN_REQUIRED or bool(ADMIN_TOKEN)),
        "admin_token_required": bool(ADMIN_TOKEN),
        "external_bind_requires_token": ADMIN_TOKEN_REQUIRED,
        "markets": _yaml(MARKETS_PATH),
        "alerts": _yaml(ALERTS_PATH),
        "telegram": _yaml(TELEGRAM_PATH),
        "telegram_runtime": {
            "enabled": os.getenv("TELEGRAM_ENABLED", "false").lower() in {"1", "true", "yes", "on"},
            "configured": bool(os.getenv("TELEGRAM_BOT_TOKEN", "").strip()),
            "allowed_chat_count": len([x for x in os.getenv("TELEGRAM_ALLOWED_CHAT_IDS", "").split(",") if x.strip()]),
        },
        "web_ui": {"refresh_seconds": REFRESH_SECONDS, "system_cache_seconds": SYSTEM_CACHE_SECONDS},
    }


@app.put("/api/config/markets")
async def save_markets(payload: dict[str, Any] = Body(...), x_admin_token: str | None = Header(None)) -> dict[str, Any]:
    _write_guard(x_admin_token)
    data = _validate_markets(payload)
    _atomic_yaml(MARKETS_PATH, data)
    return {"ok": True, "path": str(MARKETS_PATH), "config": data}


@app.put("/api/config/alerts")
async def save_alerts(payload: dict[str, Any] = Body(...), x_admin_token: str | None = Header(None)) -> dict[str, Any]:
    _write_guard(x_admin_token)
    data = _validate_alerts(payload)
    _atomic_yaml(ALERTS_PATH, data)
    return {"ok": True, "path": str(ALERTS_PATH), "config": data}


@app.put("/api/config/telegram")
async def save_telegram(payload: dict[str, Any] = Body(...), x_admin_token: str | None = Header(None)) -> dict[str, Any]:
    _write_guard(x_admin_token)
    data = _validate_telegram(payload)
    _atomic_yaml(TELEGRAM_PATH, data)
    return {"ok": True, "path": str(TELEGRAM_PATH), "config": data}


@app.get("/api/system")
async def system() -> dict[str, Any]:
    async def produce():
        services, flink, parts, kafka = await asyncio.gather(
            asyncio.gather(
                _get(f"{MONITOR_URL}/health"),
                _get(f"{ANALYTICS_URL}/health"),
                _get(f"{REPLAY_URL}/health"),
                _get(f"{LAKEKEEPER_URL}/health"),
                _get(f"{GRAFANA_URL}/api/health"),
            ),
            _get(f"{FLINK_URL}/jobs/overview"),
            _ch("""
                SELECT table, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS disk
                FROM system.parts
                WHERE active AND database='market'
                GROUP BY table
                ORDER BY sum(bytes_on_disk) DESC
            """),
            _kafka_inventory(),
        )
        names = ["monitor-api", "analytics-api", "replay-api", "lakekeeper", "grafana"]
        return {
            "services": {name: result for name, result in zip(names, services)},
            "flink": flink,
            "inventory": parts,
            "storage": parts,
            "kafka": kafka,
        }
    return await _cached("system", produce)


@app.get("/api/quotes")
async def quotes(venue: str | None = None) -> Any:
    result = await _get(f"{MONITOR_URL}/v1/quotes", params={"venue": venue} if venue else None)
    return result.get("data") if result.get("ok") else result


@app.get("/api/quote/{symbol}")
async def quote(symbol: str, venue: str = "binance") -> Any:
    result = await _get(f"{MONITOR_URL}/v1/quote/{symbol}", params={"venue": venue})
    return result.get("data") if result.get("ok") else result


def _history_interval(days: int) -> tuple[str, str]:
    if days <= 1:
        return "INTERVAL 1 MINUTE", "1m"
    if days <= 3:
        return "INTERVAL 5 MINUTE", "5m"
    if days <= 7:
        return "INTERVAL 15 MINUTE", "15m"
    if days <= 14:
        return "INTERVAL 30 MINUTE", "30m"
    if days <= 30:
        return "INTERVAL 1 HOUR", "1h"
    if days <= 60:
        return "INTERVAL 2 HOUR", "2h"
    return "INTERVAL 4 HOUR", "4h"


def _sql_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "''")


@app.get("/api/history/{symbol}")
async def history(
    symbol: str,
    venue: str = "binance",
    days: int = Query(1, ge=1, le=90),
) -> Any:
    venue = venue.strip().lower()
    if venue not in {"binance", "coinbase"}:
        raise HTTPException(status_code=422, detail="unsupported venue")
    try:
        market_cfg = load_market_config(MARKETS_PATH)
        product = market_cfg.resolve_product(venue, symbol)
    except Exception:
        product = symbol.strip().upper()
    bucket_sql, _ = _history_interval(days)
    venue_sql = _sql_literal(venue)
    product_sql = _sql_literal(product)
    query = f"""
        SELECT
            toUnixTimestamp(bucket) * 1000 AS time_ms,
            argMin(src_open, src_open_time) AS open,
            max(src_high) AS high,
            min(src_low) AS low,
            argMax(src_close, src_open_time) AS close,
            argMax(src_close, src_open_time) AS price,
            sum(src_volume) AS volume,
            sum(src_trade_count) AS trade_count
        FROM
        (
            SELECT
                toStartOfInterval(open_time, {bucket_sql}) AS bucket,
                open_time AS src_open_time,
                open AS src_open,
                high AS src_high,
                low AS src_low,
                close AS src_close,
                volume AS src_volume,
                trade_count AS src_trade_count
            FROM market.candles_1m FINAL
            WHERE venue = '{venue_sql}'
              AND symbol = '{product_sql}'
              AND open_time >= now64(3, 'UTC') - INTERVAL {days} DAY
        )
        GROUP BY bucket
        ORDER BY bucket ASC
    """
    try:
        rows = await asyncio.to_thread(_ch_sync, query)
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}
    if rows:
        return rows

    # Fallback for a fresh deployment before candle-worker has materialized data.
    fallback = f"""
        WITH toStartOfInterval(event_time, {bucket_sql}) AS bucket
        SELECT
            toUnixTimestamp(bucket) * 1000 AS time_ms,
            argMin(price, tuple(event_time_ms, event_id)) AS open,
            max(price) AS high,
            min(price) AS low,
            argMax(price, tuple(event_time_ms, event_id)) AS close,
            argMax(price, tuple(event_time_ms, event_id)) AS price,
            sum(ifNull(quantity, 0.0)) AS volume,
            count() AS trade_count
        FROM market.events_hot FINAL
        WHERE venue = '{venue_sql}'
          AND symbol = '{product_sql}'
          AND event_type = 'trade'
          AND price IS NOT NULL
          AND event_time >= now64(3, 'UTC') - INTERVAL {days} DAY
        GROUP BY bucket
        ORDER BY bucket ASC
    """
    try:
        return await asyncio.to_thread(_ch_sync, fallback)
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


@app.get("/api/analyze/{symbol}")
async def analyze(symbol: str, venue: str = "binance", interval: str = "1h", bars: int = Query(300, ge=1, le=5000)) -> Any:
    result = await _get(f"{ANALYTICS_URL}/v1/analyze/{symbol}", params={"venue": venue, "interval": interval, "bars": bars})
    return result.get("data") if result.get("ok") else result


@app.get("/api/correlation")
async def correlation(
    benchmark: str = "BTCUSDT",
    symbols: str = "ETHUSDT,SOLUSDT",
    venue: str = "binance",
    interval: str = "1h",
    bars: int = Query(300, ge=60, le=2000),
) -> Any:
    result = await _get(f"{ANALYTICS_URL}/v1/correlation", params={
        "benchmark": benchmark, "symbols": symbols, "venue": venue, "interval": interval, "bars": bars,
    })
    return result.get("data") if result.get("ok") else result


@app.get("/api/alerts")
async def alerts(limit: int = Query(100, ge=1, le=1000)) -> Any:
    result = await _get(f"{MONITOR_URL}/v1/alerts", params={"limit": limit})
    return result.get("data") if result.get("ok") else result


@app.get("/api/reconciliation")
async def reconciliation(limit: int = Query(100, ge=1, le=1000)) -> Any:
    result = await _get(f"{MONITOR_URL}/v1/reconciliation/latest", params={"limit": limit})
    return result.get("data") if result.get("ok") else result


@app.get("/api/data-distribution")
async def data_distribution() -> dict[str, Any]:
    async def produce():
        # Each _ch call owns a dedicated ClickHouse client/session, so these can run concurrently.
        events, candles, micro = await asyncio.gather(
            _ch("""
                SELECT venue, symbol, event_type, count() AS rows,
                       formatDateTime(toTimeZone(fromUnixTimestamp64Milli(max(event_time_ms)), 'America/Sao_Paulo'), '%F %T') AS latest_event_time
                FROM market.events_hot
                GROUP BY venue, symbol, event_type
                ORDER BY rows DESC
                LIMIT 500
            """),
            _ch("""
                SELECT venue, symbol, count() AS rows,
                       formatDateTime(toTimeZone(min(open_time), 'America/Sao_Paulo'), '%F %T') AS first_candle,
                       formatDateTime(toTimeZone(max(open_time), 'America/Sao_Paulo'), '%F %T') AS last_candle
                FROM market.candles_1m
                GROUP BY venue, symbol ORDER BY venue, symbol
            """),
            _ch("""
                SELECT venue, symbol, count() AS rows,
                       formatDateTime(toTimeZone(fromUnixTimestamp64Milli(max(event_time_ms)), 'America/Sao_Paulo'), '%F %T') AS latest_event_time
                FROM market.microstructure_features
                GROUP BY venue, symbol ORDER BY venue, symbol
            """),
        )
        return {"events": events, "candles": candles, "microstructure": micro}
    return await _cached("distribution", produce)
