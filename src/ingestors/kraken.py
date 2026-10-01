from __future__ import annotations

import asyncio
import json
import os
import random
from datetime import datetime
from typing import Any

import websockets

from src.common.events import MarketEvent, canonical_json, now_ms, stable_id
from src.common.kafka import create_consumer, create_producer
from src.common.markets import load_market_config

WS_URL = os.getenv("KRAKEN_WS_URL", "wss://ws.kraken.com/v2")


def source_config():
    return load_market_config().source("kraken")


def products() -> list[str]:
    return load_market_config().products_for("kraken")


def parse_time_ms(value: str | None) -> int:
    if not value:
        return now_ms()
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return now_ms()


def normalize(payload: dict[str, Any]) -> list[MarketEvent]:
    """Normalize Kraken WebSocket v2 payload into a list of MarketEvent."""
    channel = payload.get("channel")
    msg_type = payload.get("type")
    items = payload.get("data")
    if not channel or not isinstance(items, list):
        return []

    events: list[MarketEvent] = []

    if channel == "ticker":
        for item in items:
            symbol = str(item.get("symbol", "")).upper()
            if not symbol:
                continue
            tms = parse_time_ms(item.get("timestamp"))
            bid = float(item["bid"]) if item.get("bid") is not None else None
            bid_qty = float(item["bid_qty"]) if item.get("bid_qty") is not None else None
            ask = float(item["ask"]) if item.get("ask") is not None else None
            ask_qty = float(item["ask_qty"]) if item.get("ask_qty") is not None else None
            last = float(item["last"]) if item.get("last") is not None else None
            source_id = stable_id("kraken", symbol, "ticker", str(tms), str(last))[:24]

            events.append(
                MarketEvent.build(
                    venue="kraken",
                    channel="ticker",
                    event_type="ticker",
                    symbol=symbol,
                    source_id=f"ticker:{source_id}",
                    event_time_ms=tms,
                    price=last,
                    bid_price=bid,
                    bid_quantity=bid_qty,
                    ask_price=ask,
                    ask_quantity=ask_qty,
                    raw=item,
                )
            )

    elif channel == "trade":
        for item in items:
            symbol = str(item.get("symbol", "")).upper()
            if not symbol:
                continue
            tms = parse_time_ms(item.get("timestamp"))
            trade_id = item.get("trade_id")
            price = float(item["price"]) if item.get("price") is not None else None
            qty = float(item["qty"]) if item.get("qty") is not None else None
            side = str(item.get("side", "")).lower()
            sid = str(trade_id) if trade_id is not None else stable_id("kraken", symbol, str(tms), str(price))[:24]

            events.append(
                MarketEvent.build(
                    venue="kraken",
                    channel="trade",
                    event_type="trade",
                    symbol=symbol,
                    source_id=f"trade:{sid}",
                    event_time_ms=tms,
                    sequence_id=int(trade_id) if isinstance(trade_id, int) or (isinstance(trade_id, str) and trade_id.isdigit()) else None,
                    price=price,
                    quantity=qty,
                    side=side,
                    raw=item,
                )
            )

    elif channel == "book":
        event_type = "book_snapshot" if msg_type == "snapshot" else "book_delta"
        for item in items:
            symbol = str(item.get("symbol", "")).upper()
            if not symbol:
                continue
            bids = [[str(x.get("price", "")), str(x.get("qty", ""))] for x in item.get("bids", [])]
            asks = [[str(x.get("price", "")), str(x.get("qty", ""))] for x in item.get("asks", [])]
            raw_id = stable_id(symbol, canonical_json(bids), canonical_json(asks))[:24]
            checksum = item.get("checksum")

            events.append(
                MarketEvent.build(
                    venue="kraken",
                    channel="book",
                    event_type=event_type,
                    symbol=symbol,
                    source_id=f"{event_type}:{raw_id}",
                    event_time_ms=now_ms(),
                    sequence_id=int(checksum) if checksum is not None and isinstance(checksum, int) else None,
                    bids_json=canonical_json(bids),
                    asks_json=canonical_json(asks),
                    raw=item,
                )
            )

    return events


async def resync_loop(reconnect_event: asyncio.Event) -> None:
    consumer = await create_consumer(
        "market.control.resync",
        group_id=f"kraken-resync-{os.getpid()}",
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    try:
        async for message in consumer:
            try:
                command = json.loads(message.value)
                allowed = set(load_market_config().products_for("kraken"))
                if command.get("venue") == "kraken" and command.get("symbol") in allowed:
                    reconnect_event.set()
            except Exception as exc:
                print(f"kraken resync command failed: {exc}", flush=True)
    finally:
        await consumer.stop()


async def config_watch_loop(reconnect_event: asyncio.Event) -> None:
    last: str | None = None
    while True:
        try:
            cfg = load_market_config()
            fp = cfg.fingerprint("kraken")
            if last is None:
                last = fp
            elif fp != last:
                last = fp
                print("kraken markets.yaml changed; reconnecting with new selection", flush=True)
                reconnect_event.set()
            await asyncio.sleep(cfg.reload_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"kraken market config error: {type(exc).__name__}: {exc}", flush=True)
            await asyncio.sleep(5)


async def websocket_loop(producer, reconnect_event: asyncio.Event) -> None:
    attempt = 0
    while True:
        cfg = load_market_config()
        src = cfg.source("kraken")
        selected = cfg.products_for("kraken")
        if not src.enabled or not selected:
            await asyncio.sleep(cfg.reload_seconds)
            continue
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_size=8 * 1024 * 1024,
            ) as ws:
                channels = {x.lower() for x in src.channels}
                # Subscribe to channels supported by Kraken v2
                if "ticker" in channels or "book_ticker" in channels:
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "params": {
                            "channel": "ticker",
                            "symbol": selected,
                        },
                    }))
                if "trade" in channels or "matches" in channels:
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "params": {
                            "channel": "trade",
                            "symbol": selected,
                        },
                    }))
                if "book" in channels or "depth" in channels or "level2" in channels:
                    limit = max(10, min(100, src.snapshot_limit))
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "params": {
                            "channel": "book",
                            "symbol": selected,
                            "depth": limit,
                        },
                    }))

                attempt = 0
                reconnect_event.clear()
                print(f"kraken connected: subscribed to {selected} on {channels}", flush=True)

                while True:
                    recv_task = asyncio.create_task(ws.recv())
                    resync_task = asyncio.create_task(reconnect_event.wait())
                    done, pending = await asyncio.wait({recv_task, resync_task}, return_when=asyncio.FIRST_COMPLETED)
                    for task in pending:
                        task.cancel()
                    if resync_task in done and reconnect_event.is_set():
                        if not recv_task.done():
                            recv_task.cancel()
                        await ws.close(code=1000, reason="book resync or market config change")
                        break
                    raw = recv_task.result()
                    data = json.loads(raw)
                    events = normalize(data)
                    for event in events:
                        await producer.send_and_wait("market.raw", event.kafka_bytes(), key=event.kafka_key())
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            attempt += 1
            delay = min(60.0, (2 ** min(attempt, 6)) + random.random())
            print(f"kraken websocket error={type(exc).__name__}: {exc}; reconnect={delay:.1f}s", flush=True)
            await asyncio.sleep(delay)


async def main() -> None:
    producer = await create_producer()
    reconnect_event = asyncio.Event()
    try:
        await asyncio.gather(
            websocket_loop(producer, reconnect_event),
            resync_loop(reconnect_event),
            config_watch_loop(reconnect_event),
        )
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
