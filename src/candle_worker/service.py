from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone

from src.common.clickhouse import insert, rows

INTERVAL_SECONDS = int(os.getenv("CANDLE_WORKER_INTERVAL_SECONDS", "15"))
RECOMPUTE_MINUTES = int(os.getenv("CANDLE_RECOMPUTE_MINUTES", "10"))
BACKFILL_DAYS = int(os.getenv("CANDLE_BACKFILL_DAYS", os.getenv("CLICKHOUSE_HOT_RETENTION_DAYS", "30")))

INSERT_COLUMNS = [
    "venue", "symbol", "open_time", "open_time_ms", "open", "high", "low", "close",
    "volume", "trade_count", "last_event_time_ms", "version_ms",
]


def _aggregate(start_ms: int, end_ms: int) -> list[dict]:
    return rows(
        """
        SELECT
            venue,
            symbol,
            toStartOfMinute(event_time) AS open_time,
            toUnixTimestamp(toStartOfMinute(event_time)) * 1000 AS open_time_ms,
            argMin(price, tuple(event_time_ms, event_id)) AS open,
            max(price) AS high,
            min(price) AS low,
            argMax(price, tuple(event_time_ms, event_id)) AS close,
            sum(ifNull(quantity, 0.0)) AS volume,
            count() AS trade_count,
            max(event_time_ms) AS last_event_time_ms,
            max(ingest_time_ms) AS version_ms
        FROM market.events_hot FINAL
        WHERE event_type = 'trade'
          AND price IS NOT NULL
          AND event_time_ms >= {start:Int64}
          AND event_time_ms < {end:Int64}
        GROUP BY venue, symbol, open_time
        ORDER BY venue, symbol, open_time
        """,
        {"start": start_ms, "end": end_ms},
    )


def materialize_window(start_ms: int, end_ms: int) -> int:
    data = _aggregate(start_ms, end_ms)
    if not data:
        return 0
    payload = [[r[c] for c in INSERT_COLUMNS] for r in data]
    insert("market.candles_1m", payload, column_names=INSERT_COLUMNS)
    return len(payload)


def backfill() -> int:
    if BACKFILL_DAYS <= 0:
        return 0
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = now - timedelta(days=BACKFILL_DAYS)
    total = 0
    cursor = start
    while cursor < now:
        nxt = min(cursor + timedelta(days=1), now)
        total += materialize_window(int(cursor.timestamp() * 1000), int(nxt.timestamp() * 1000))
        cursor = nxt
    return total


def main() -> None:
    try:
        n = backfill()
        print(f"candle-worker backfill rows={n}", flush=True)
    except Exception as exc:
        print(f"candle-worker backfill failed: {type(exc).__name__}: {exc}", flush=True)
    while True:
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - RECOMPUTE_MINUTES * 60_000
        try:
            n = materialize_window(start_ms, end_ms)
            print(f"candle-worker refreshed rows={n}", flush=True)
        except Exception as exc:
            print(f"candle-worker refresh failed: {type(exc).__name__}: {exc}", flush=True)
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
