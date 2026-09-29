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

WS_URL = os.getenv("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")


def source_config():
    return load_market_config().source("coinbase")


def products() -> list[str]:
    return load_market_config().products_for("coinbase")


def parse_time_ms(value: str | None) -> int:
    if not value:
        return now_ms()
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return now_ms()


def normalize(data: dict[str, Any]) -> MarketEvent | None:
    typ = data.get("type")
    product = data.get("product_id")
    if not product:
        return None
    tms = parse_time_ms(data.get("time"))
    if typ == "snapshot":
        raw_id = stable_id(product, canonical_json(data.get("bids", [])), canonical_json(data.get("asks", [])))[:24]
        return MarketEvent.build(
            venue="coinbase", channel="level2", event_type="book_snapshot", symbol=product,
            source_id=f"snapshot:{raw_id}", event_time_ms=tms, raw=data,
            bids_json=canonical_json(data.get("bids", [])), asks_json=canonical_json(data.get("asks", [])),
        )
    if typ == "l2update":
        bids: list[list[str]] = []
        asks: list[list[str]] = []
        for side, price, qty in data.get("changes", []):
            (bids if side == "buy" else asks).append([price, qty])
        sid = str(data.get("sequence") or stable_id(product, data.get("time"), canonical_json(data.get("changes", [])))[:24])
        return MarketEvent.build(
            venue="coinbase", channel="level2", event_type="book_delta", symbol=product,
            source_id=sid, event_time_ms=tms, raw=data,
            sequence_id=int(data["sequence"]) if data.get("sequence") is not None else None,
            bids_json=canonical_json(bids), asks_json=canonical_json(asks),
        )
    if typ in {"match", "last_match"}:
        maker_side = str(data.get("side", "")).lower()
        taker_side = "buy" if maker_side == "sell" else "sell" if maker_side == "buy" else None
        return MarketEvent.build(
            venue="coinbase", channel="matches", event_type="trade", symbol=product,
            source_id=str(data.get("trade_id") or stable_id(product, data.get("time"), data.get("price"), data.get("size"))[:24]),
            event_time_ms=tms, raw=data,
            sequence_id=int(data["sequence"]) if data.get("sequence") is not None else None,
            price=float(data["price"]), quantity=float(data["size"]), side=taker_side,
        )
    if typ == "ticker":
        source_id = str(data.get("sequence") or stable_id(product, data.get("time"), data.get("price"))[:24])
        return MarketEvent.build(
            venue="coinbase", channel="ticker", event_type="ticker", symbol=product,
            source_id=source_id, event_time_ms=tms, raw=data,
            sequence_id=int(data["sequence"]) if data.get("sequence") is not None else None,
            price=float(data["price"]) if data.get("price") else None,
            quantity=float(data["last_size"]) if data.get("last_size") else None,
            bid_price=float(data["best_bid"]) if data.get("best_bid") else None,
            ask_price=float(data["best_ask"]) if data.get("best_ask") else None,
        )
    return None


async def resync_loop(reconnect_event: asyncio.Event) -> None:
    consumer = await create_consumer("market.control.resync", group_id="coinbase-resync-v4", auto_offset_reset="latest")
    try:
        async for message in consumer:
            try:
                command = json.loads(message.value)
                allowed = set(load_market_config().products_for("coinbase"))
                if command.get("venue") == "coinbase" and command.get("symbol") in allowed:
                    reconnect_event.set()
            except Exception as exc:
                print(f"coinbase resync command failed: {exc}", flush=True)
    finally:
        await consumer.stop()


async def config_watch_loop(reconnect_event: asyncio.Event) -> None:
    last: str | None = None
    while True:
        try:
            cfg = load_market_config()
            fp = cfg.fingerprint("coinbase")
            if last is None:
                last = fp
            elif fp != last:
                last = fp
                print("coinbase markets.yaml changed; reconnecting with new selection", flush=True)
                reconnect_event.set()
            await asyncio.sleep(cfg.reload_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"coinbase market config error: {type(exc).__name__}: {exc}", flush=True)
            await asyncio.sleep(5)


async def websocket_loop(producer, reconnect_event: asyncio.Event) -> None:
    attempt = 0
    while True:
        cfg = load_market_config()
        src = cfg.source("coinbase")
        selected = cfg.products_for("coinbase")
        if not src.enabled or not selected:
            await asyncio.sleep(cfg.reload_seconds)
            continue
        try:
            async with websockets.connect(
                WS_URL, ping_interval=20, ping_timeout=20, close_timeout=10,
                max_size=8 * 1024 * 1024,
            ) as ws:
                channels = [x.lower() for x in src.channels]
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "product_ids": selected,
                    "channels": channels,
                }))
                attempt = 0
                reconnect_event.clear()
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
                    event = normalize(data)
                    if event:
                        await producer.send_and_wait("market.raw", event.kafka_bytes(), key=event.kafka_key())
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            attempt += 1
            delay = min(60.0, (2 ** min(attempt, 6)) + random.random())
            print(f"coinbase websocket error={type(exc).__name__}: {exc}; reconnect={delay:.1f}s", flush=True)
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
