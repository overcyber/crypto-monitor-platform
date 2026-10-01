from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect

from src.common.clickhouse import rows
from src.common.config import settings
from src.common.markets import load_market_config

app = FastAPI(title="Crypto Monitor API", version="4.3.4", description="Monitoring-only endpoint. Analytics routes intentionally live on :8082.")

DISPLAY_TIMEZONE = os.getenv("DISPLAY_TIMEZONE", "America/Sao_Paulo")
QUOTE_STALE_AFTER_SECONDS = int(os.getenv("QUOTE_STALE_AFTER_SECONDS", "60"))


def _zone() -> ZoneInfo:
    try:
        return ZoneInfo(DISPLAY_TIMEZONE)
    except Exception:
        return ZoneInfo("America/Sao_Paulo")


def _resolve_symbol(symbol: str, venue: str) -> str:
    return load_market_config().resolve_product(venue, symbol)


def _decorate_quote(row: dict | None) -> dict | None:
    if not row:
        return None
    out = dict(row)
    event_ms = int(out.pop("latest_event_time_ms", out.get("event_time_ms") or 0) or 0)
    ingest_ms = int(out.pop("latest_ingest_time_ms", out.get("ingest_time_ms") or 0) or 0)
    out["event_time_ms"] = event_ms
    out["ingest_time_ms"] = ingest_ms
    if event_ms:
        out["event_time"] = datetime.fromtimestamp(event_ms / 1000, timezone.utc).astimezone(_zone()).isoformat(timespec="milliseconds")
    if ingest_ms:
        out["ingest_time"] = datetime.fromtimestamp(ingest_ms / 1000, timezone.utc).astimezone(_zone()).isoformat(timespec="milliseconds")
        age = max(0.0, (time.time() * 1000 - ingest_ms) / 1000.0)
        out["age_seconds"] = round(age, 3)
        out["stale"] = age > QUOTE_STALE_AFTER_SECONDS
    return out


def _quote(symbol: str, venue: str) -> dict | None:
    native = _resolve_symbol(symbol, venue)
    out = rows(
        """
        SELECT
          venue,
          symbol,
          argMaxIf(price, tuple(ingest_time_ms, event_time_ms), price IS NOT NULL) AS price,
          argMaxIf(bid_price, tuple(ingest_time_ms, event_time_ms), bid_price IS NOT NULL) AS bid,
          argMaxIf(ask_price, tuple(ingest_time_ms, event_time_ms), ask_price IS NOT NULL) AS ask,
          argMaxIf(quantity, tuple(ingest_time_ms, event_time_ms), quantity IS NOT NULL) AS quantity,
          argMax(event_time_ms, tuple(ingest_time_ms, event_time_ms)) AS latest_event_time_ms,
          max(ingest_time_ms) AS latest_ingest_time_ms
        FROM market.events_hot
        WHERE venue={venue:String} AND symbol={symbol:String}
          AND ingest_time_ms >= toUnixTimestamp64Milli(now64(3, 'UTC') - INTERVAL 6 HOUR)
          AND event_type IN ('trade','ticker','book_ticker')
        GROUP BY venue, symbol
        LIMIT 1
        """,
        {"venue": venue.lower(), "symbol": native},
    )
    if not out:
        out = rows(
            """
            SELECT
              venue,
              symbol,
              argMaxIf(price, tuple(ingest_time_ms, event_time_ms), price IS NOT NULL) AS price,
              argMaxIf(bid_price, tuple(ingest_time_ms, event_time_ms), bid_price IS NOT NULL) AS bid,
              argMaxIf(ask_price, tuple(ingest_time_ms, event_time_ms), ask_price IS NOT NULL) AS ask,
              argMaxIf(quantity, tuple(ingest_time_ms, event_time_ms), quantity IS NOT NULL) AS quantity,
              argMax(event_time_ms, tuple(ingest_time_ms, event_time_ms)) AS latest_event_time_ms,
              max(ingest_time_ms) AS latest_ingest_time_ms
            FROM market.events_hot
            WHERE venue={venue:String} AND symbol={symbol:String}
              AND event_type IN ('trade','ticker','book_ticker')
            GROUP BY venue, symbol
            LIMIT 1
            """,
            {"venue": venue.lower(), "symbol": native},
        )
    res = _decorate_quote(out[0]) if out else None
    if res is not None and not res.get("stale"):
        return res

    # If requested venue has no data or is stale, check other configured sources (e.g. kraken)
    try:
        cfg = load_market_config()
        for other_venue in cfg.sources:
            if other_venue.lower() == venue.lower() or not cfg.sources[other_venue].enabled:
                continue
            other_native = cfg.resolve_product(other_venue, symbol)
            other_out = rows(
                """
                SELECT
                  venue,
                  symbol,
                  argMaxIf(price, tuple(ingest_time_ms, event_time_ms), price IS NOT NULL) AS price,
                  argMaxIf(bid_price, tuple(ingest_time_ms, event_time_ms), bid_price IS NOT NULL) AS bid,
                  argMaxIf(ask_price, tuple(ingest_time_ms, event_time_ms), ask_price IS NOT NULL) AS ask,
                  argMaxIf(quantity, tuple(ingest_time_ms, event_time_ms), quantity IS NOT NULL) AS quantity,
                  argMax(event_time_ms, tuple(ingest_time_ms, event_time_ms)) AS latest_event_time_ms,
                  max(ingest_time_ms) AS latest_ingest_time_ms
                FROM market.events_hot
                WHERE venue={venue:String} AND symbol={symbol:String}
                  AND ingest_time_ms >= toUnixTimestamp64Milli(now64(3, 'UTC') - INTERVAL 6 HOUR)
                  AND event_type IN ('trade','ticker','book_ticker')
                GROUP BY venue, symbol
                LIMIT 1
                """,
                {"venue": other_venue.lower(), "symbol": other_native},
            )
            if other_out:
                dec = _decorate_quote(other_out[0])
                if dec and not dec.get("stale"):
                    return dec
                if res is None:
                    res = dec
    except Exception:
        pass

    return res


@app.get("/health")
async def health():
    try:
        result = await asyncio.to_thread(rows, "SELECT 1 AS ok")
        return {"status": "ok", "service": "monitor-api", "clickhouse": bool(result)}
    except Exception as exc:
        return {"status": "degraded", "service": "monitor-api", "clickhouse": False, "error": str(exc)}


@app.get("/v1/quote/{symbol}")
async def quote(symbol: str, venue: str = Query("binance")):
    try:
        q = await asyncio.to_thread(_quote, symbol, venue)
    except Exception as exc:
        raise HTTPException(503, str(exc)) from exc
    if not q:
        raise HTTPException(404, "symbol/venue not found")
    return q


@app.get("/v1/quotes")
async def quotes(venue: str | None = None, limit: int = Query(100, ge=1, le=1000)):
    where = "WHERE event_type IN ('trade','ticker','book_ticker') AND ingest_time_ms >= toUnixTimestamp64Milli(now64(3, 'UTC') - INTERVAL 6 HOUR)"
    params = {"limit": limit}
    if venue:
        where += " AND venue={venue:String}"
        params["venue"] = venue.lower()
    q = f"""
      SELECT venue,symbol,
        argMaxIf(price,tuple(ingest_time_ms,event_time_ms),price IS NOT NULL) AS price,
        argMaxIf(bid_price,tuple(ingest_time_ms,event_time_ms),bid_price IS NOT NULL) AS bid,
        argMaxIf(ask_price,tuple(ingest_time_ms,event_time_ms),ask_price IS NOT NULL) AS ask,
        argMax(event_time_ms,tuple(ingest_time_ms,event_time_ms)) AS latest_event_time_ms,
        max(ingest_time_ms) AS latest_ingest_time_ms
      FROM market.events_hot {where}
      GROUP BY venue,symbol ORDER BY venue,symbol LIMIT {{limit:UInt32}}
    """
    try:
        result = await asyncio.to_thread(rows, q, params)
        if not result:
            where_fallback = "WHERE event_type IN ('trade','ticker','book_ticker')"
            if venue:
                where_fallback += " AND venue={venue:String}"
            q_fallback = f"""
              SELECT venue,symbol,
                argMaxIf(price,tuple(ingest_time_ms,event_time_ms),price IS NOT NULL) AS price,
                argMaxIf(bid_price,tuple(ingest_time_ms,event_time_ms),bid_price IS NOT NULL) AS bid,
                argMaxIf(ask_price,tuple(ingest_time_ms,event_time_ms),ask_price IS NOT NULL) AS ask,
                argMax(event_time_ms,tuple(ingest_time_ms,event_time_ms)) AS latest_event_time_ms,
                max(ingest_time_ms) AS latest_ingest_time_ms
              FROM market.events_hot {where_fallback}
              GROUP BY venue,symbol ORDER BY venue,symbol LIMIT {{limit:UInt32}}
            """
            result = await asyncio.to_thread(rows, q_fallback, params)
        return [_decorate_quote(x) for x in result]
    except Exception as exc:
        raise HTTPException(503, str(exc)) from exc


@app.get("/v1/history/{symbol}")
async def history(symbol: str, venue: str = Query("binance"), event_type: str = Query("trade"), limit: int = Query(1000, ge=1)):
    limit = min(limit, settings().api_max_history_rows)
    native = _resolve_symbol(symbol, venue)
    q = """
      SELECT event_id,venue,symbol,event_type,event_time_ms,ingest_time_ms,price,quantity,side,bid_price,ask_price
      FROM market.events_hot FINAL
      WHERE venue={venue:String} AND symbol={symbol:String} AND event_type={event_type:String}
      ORDER BY ingest_time_ms DESC LIMIT {limit:UInt32}
    """
    try:
        result = await asyncio.to_thread(rows, q, {"venue": venue.lower(), "symbol": native, "event_type": event_type.lower(), "limit": limit})
        z = _zone()
        for r in result:
            if r.get("event_time_ms"):
                r["event_time"] = datetime.fromtimestamp(int(r["event_time_ms"]) / 1000, timezone.utc).astimezone(z).isoformat(timespec="milliseconds")
            if r.get("ingest_time_ms"):
                r["ingest_time"] = datetime.fromtimestamp(int(r["ingest_time_ms"]) / 1000, timezone.utc).astimezone(z).isoformat(timespec="milliseconds")
        return result
    except Exception as exc:
        raise HTTPException(503, str(exc)) from exc


@app.get("/v1/venues")
async def venues():
    try:
        return await asyncio.to_thread(rows, "SELECT venue,count() AS rows,max(ingest_time_ms) AS last_ingest_time_ms FROM market.events_hot GROUP BY venue ORDER BY venue")
    except Exception as exc:
        raise HTTPException(503, str(exc)) from exc


@app.get("/v1/watchlist")
async def watchlist():
    cfg = load_market_config()
    return {"watchlist": list(cfg.watchlist), "binance": cfg.products_for("binance"), "coinbase": cfg.products_for("coinbase"), "config_source": "config/markets.yaml"}


@app.get("/v1/alerts")
async def alerts(venue: str | None = None, symbol: str | None = None, rule_id: str | None = None, limit: int = Query(100, ge=1, le=1000)):
    clauses = []
    params = {"limit": limit}
    if venue:
        clauses.append("venue={venue:String}")
        params["venue"] = venue.lower()
    if symbol:
        clauses.append("symbol={symbol:String}")
        params["symbol"] = _resolve_symbol(symbol, venue or "binance")
    if rule_id:
        clauses.append("rule_id={rule_id:String}")
        params["rule_id"] = rule_id
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    q = f"""
      SELECT alert_id,rule_id,source_topic,source_id,venue,symbol,event_type,field,operator,
             threshold,observed,event_time,event_time_ms,created_at,created_at_ms,details_json
      FROM market.alerts FINAL {where}
      ORDER BY event_time DESC LIMIT {{limit:UInt32}}
    """
    try:
        return await asyncio.to_thread(rows, q, params)
    except Exception as exc:
        raise HTTPException(503, str(exc)) from exc


@app.get("/v1/reconciliation/latest")
async def reconciliation_latest(limit: int = Query(100, ge=1, le=1000)):
    try:
        return await asyncio.to_thread(rows, "SELECT * FROM market.reconciliation_results ORDER BY created_at DESC LIMIT {limit:UInt32}", {"limit": limit})
    except Exception as exc:
        raise HTTPException(503, str(exc)) from exc


@app.websocket("/v1/stream/{symbol}")
async def stream(websocket: WebSocket, symbol: str, venue: str = "binance", poll_ms: int = 1000):
    await websocket.accept()
    last = None
    poll = max(200, min(poll_ms, 10_000)) / 1000
    try:
        while True:
            q = await asyncio.to_thread(_quote, symbol, venue)
            if q and q.get("ingest_time_ms") != last:
                await websocket.send_json(q)
                last = q.get("ingest_time_ms")
            await asyncio.sleep(poll)
    except WebSocketDisconnect:
        return
