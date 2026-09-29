from __future__ import annotations

import os
from datetime import datetime
from zoneinfo import ZoneInfo

from src.common.clickhouse import rows
from src.common.markets import load_market_config
from .models import Candle

DISPLAY_TIMEZONE = os.getenv("DISPLAY_TIMEZONE", "America/Sao_Paulo")

INTERVAL_SQL = {
    "1m":"1 MINUTE","3m":"3 MINUTE","5m":"5 MINUTE","15m":"15 MINUTE","30m":"30 MINUTE",
    "1h":"1 HOUR","2h":"2 HOUR","4h":"4 HOUR","6h":"6 HOUR","12h":"12 HOUR","1d":"1 DAY",
}


def _native_symbol(symbol: str, venue: str) -> str:
    return load_market_config().resolve_product(venue, symbol)


def candles(symbol:str, venue:str="binance", interval:str="1h", limit:int=300)->list[Candle]:
    if interval not in INTERVAL_SQL:
        raise ValueError(f"unsupported interval: {interval}")
    limit=max(20,min(int(limit),5000)); iv=INTERVAL_SQL[interval]
    q=f"""
    SELECT open_time_ms, open, high, low, close, volume
    FROM (
      SELECT
        toUnixTimestamp(toStartOfInterval(open_time, INTERVAL {iv})) * 1000 AS open_time_ms,
        argMin(open, open_time_ms) AS open,
        max(high) AS high,
        min(low) AS low,
        argMax(close, open_time_ms) AS close,
        sum(volume) AS volume
      FROM market.candles_1m FINAL
      WHERE venue = {{venue:String}} AND symbol = {{symbol:String}}
      GROUP BY toStartOfInterval(open_time, INTERVAL {iv})
      ORDER BY open_time_ms DESC
      LIMIT {{limit:UInt32}}
    ) ORDER BY open_time_ms ASC
    """
    out=rows(q,{"venue":venue.lower(),"symbol":_native_symbol(symbol,venue),"limit":limit})
    return [Candle(int(r["open_time_ms"]),float(r["open"]),float(r["high"]),float(r["low"]),float(r["close"]),float(r["volume"])) for r in out]


def latest_microstructure(symbol:str, venue:str="binance")->dict|None:
    out=rows("""
      SELECT * FROM market.microstructure_features FINAL
      WHERE venue={venue:String} AND symbol={symbol:String}
      ORDER BY ingest_time_ms DESC LIMIT 1
    """,{"venue":venue.lower(),"symbol":_native_symbol(symbol,venue)})
    if not out:
        return None
    result = dict(out[0])
    value = result.get("event_time")
    if isinstance(value, datetime):
        result["event_time"] = value.astimezone(ZoneInfo(DISPLAY_TIMEZONE)).isoformat(timespec="milliseconds")
    return result
