from __future__ import annotations

import asyncio
import json
import os
import random
from typing import Any

import httpx
import websockets

from src.common.events import MarketEvent, canonical_json, now_ms
from src.common.kafka import create_consumer, create_producer
from src.common.markets import load_market_config

WS_BASE = os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:9443/stream")
REST_BASE = os.getenv("BINANCE_REST_URL", "https://data-api.binance.vision")


def source_config():
    return load_market_config().source("binance")


def symbols() -> list[str]:
    return load_market_config().products_for("binance")


def _channels() -> set[str]:
    return {x.upper() for x in source_config().channels}


def stream_url() -> str:
    cfg = source_config()
    streams: list[str] = []
    for symbol in symbols():
        s = symbol.lower()
        ch = {x.upper() for x in cfg.channels}
        if "DEPTH" in ch:
            streams.append(f"{s}@depth@{cfg.depth_interval}")
        if "TRADE" in ch:
            streams.append(f"{s}@trade")
        if "BOOK_TICKER" in ch or "BOOKTICKER" in ch:
            streams.append(f"{s}@bookTicker")
    if not streams:
        raise RuntimeError("Binance enabled but no channels/products selected in config/markets.yaml")
    return f"{WS_BASE}?streams={'/'.join(streams)}"


def normalize(symbol: str, channel: str, data: dict[str, Any]) -> MarketEvent | None:
    symbol = symbol.upper()
    if data.get("e") == "depthUpdate":
        return MarketEvent.build(
            venue="binance", channel="depth", event_type="book_delta", symbol=symbol,
            source_id=f"{data.get('U')}:{data.get('u')}", event_time_ms=int(data.get("E") or now_ms()), raw=data,
            first_sequence_id=int(data["U"]), sequence_id=int(data["u"]),
            bids_json=canonical_json(data.get("b", [])), asks_json=canonical_json(data.get("a", [])),
        )
    if data.get("e") == "trade":
        return MarketEvent.build(
            venue="binance", channel="trade", event_type="trade", symbol=symbol,
            source_id=str(data.get("t")), event_time_ms=int(data.get("T") or data.get("E") or now_ms()), raw=data,
            sequence_id=int(data["t"]), price=float(data["p"]), quantity=float(data["q"]),
            side="sell" if bool(data.get("m")) else "buy",
        )
    if "b" in data and "a" in data and "B" in data and "A" in data and "u" in data:
        return MarketEvent.build(
            venue="binance", channel="bookticker", event_type="book_ticker", symbol=symbol,
            source_id=str(data.get("u")), event_time_ms=int(data.get("E") or now_ms()), raw=data,
            sequence_id=int(data["u"]), bid_price=float(data["b"]), bid_quantity=float(data["B"]),
            ask_price=float(data["a"]), ask_quantity=float(data["A"]),
        )
    return None


async def publish_snapshot(symbol: str, producer) -> None:
    cfg = source_config()
    if "DEPTH" not in {x.upper() for x in cfg.channels}:
        return
    limit = cfg.snapshot_limit
    url = f"{REST_BASE}/api/v3/depth"
    async with httpx.AsyncClient(timeout=10) as http:
        response = await http.get(url, params={"symbol": symbol, "limit": limit})
        response.raise_for_status()
        data = response.json()
    seq = int(data["lastUpdateId"])
    event = MarketEvent.build(
        venue="binance", channel="depth", event_type="book_snapshot", symbol=symbol,
        source_id=f"snapshot:{seq}", event_time_ms=now_ms(), raw=data,
        sequence_id=seq, bids_json=canonical_json(data.get("bids", [])), asks_json=canonical_json(data.get("asks", [])),
        metadata_json=canonical_json({"snapshot_source": "rest", "limit": limit}),
    )
    await producer.send_and_wait("market.raw", event.kafka_bytes(), key=event.kafka_key())


async def publish_snapshot_with_retry(symbol: str, producer, attempts: int = 5) -> None:
    for attempt in range(1, attempts + 1):
        try:
            await publish_snapshot(symbol, producer)
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if attempt >= attempts:
                print(f"binance snapshot failed symbol={symbol}: {type(exc).__name__}: {exc}", flush=True)
                return
            await asyncio.sleep(min(10.0, 0.5 * (2 ** (attempt - 1)) + random.random()))


async def resync_loop(producer) -> None:
    consumer = await create_consumer("market.control.resync", group_id="binance-resync-v4", auto_offset_reset="latest")
    try:
        async for message in consumer:
            try:
                command = json.loads(message.value)
                current = load_market_config()
                allowed = set(current.products_for("binance"))
                if command.get("venue") == "binance" and command.get("symbol") in allowed:
                    await publish_snapshot_with_retry(command["symbol"], producer)
            except Exception as exc:
                print(f"binance resync command failed: {exc}", flush=True)
    finally:
        await consumer.stop()


async def config_watch_loop(reconnect_event: asyncio.Event) -> None:
    last: str | None = None
    while True:
        try:
            cfg = load_market_config()
            fp = cfg.fingerprint("binance")
            if last is None:
                last = fp
            elif fp != last:
                last = fp
                print("binance markets.yaml changed; reconnecting with new selection", flush=True)
                reconnect_event.set()
            await asyncio.sleep(cfg.reload_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"binance market config error: {type(exc).__name__}: {exc}", flush=True)
            await asyncio.sleep(5)


async def websocket_loop(producer, reconnect_event: asyncio.Event) -> None:
    attempt = 0
    while True:
        cfg = load_market_config()
        if not cfg.source("binance").enabled or not cfg.products_for("binance"):
            await asyncio.sleep(cfg.reload_seconds)
            continue
        try:
            async with websockets.connect(
                stream_url(), ping_interval=20, ping_timeout=20, close_timeout=10,
                max_size=8 * 1024 * 1024,
            ) as ws:
                attempt = 0
                reconnect_event.clear()
                await asyncio.sleep(0.15)
                if "DEPTH" in _channels():
                    for symbol in symbols():
                        asyncio.create_task(publish_snapshot_with_retry(symbol, producer))
                while True:
                    recv_task = asyncio.create_task(ws.recv())
                    cfg_task = asyncio.create_task(reconnect_event.wait())
                    done, pending = await asyncio.wait({recv_task, cfg_task}, return_when=asyncio.FIRST_COMPLETED)
                    for task in pending:
                        task.cancel()
                    if cfg_task in done and reconnect_event.is_set():
                        if not recv_task.done():
                            recv_task.cancel()
                        await ws.close(code=1000, reason="market config changed")
                        break
                    raw = recv_task.result()
                    msg = json.loads(raw)
                    stream = msg.get("stream", "")
                    data = msg.get("data", msg)
                    native = data.get("s") or stream.split("@", 1)[0]
                    event = normalize(str(native), stream, data)
                    if event:
                        await producer.send_and_wait("market.raw", event.kafka_bytes(), key=event.kafka_key())
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            attempt += 1
            delay = min(60.0, (2 ** min(attempt, 6)) + random.random())
            print(f"binance websocket error={type(exc).__name__}: {exc}; reconnect={delay:.1f}s", flush=True)
            await asyncio.sleep(delay)


async def main() -> None:
    producer = await create_producer()
    reconnect_event = asyncio.Event()
    try:
        await asyncio.gather(
            websocket_loop(producer, reconnect_event),
            resync_loop(producer),
            config_watch_loop(reconnect_event),
        )
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
