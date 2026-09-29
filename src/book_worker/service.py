from __future__ import annotations

import asyncio
import json
import os

from src.book_worker.book_manager import BookState
from src.common.config import settings
from src.common.events import MarketEvent, canonical_json
from src.common.kafka import create_consumer, create_producer


async def main() -> None:
    s = settings()
    input_topic = os.getenv("BOOK_INPUT_TOPIC", "market.cleaned")
    output_topic = os.getenv("BOOK_OUTPUT_TOPIC", "market.microstructure")
    group = os.getenv("BOOK_GROUP_ID", "book-worker-v4")
    consumer = await create_consumer(input_topic, group_id=group, auto_offset_reset="earliest", enable_auto_commit=False)
    producer = await create_producer()
    books: dict[tuple[str, str], BookState] = {}
    resync_pending: set[tuple[str, str]] = set()
    try:
        async for msg in consumer:
            try:
                event = MarketEvent.model_validate_json(msg.value)
            except Exception as exc:
                await producer.send_and_wait(
                    "market.deadletter",
                    json.dumps({"component":"book-worker","error":str(exc)}).encode(),
                )
                await consumer.commit()
                continue
            key = (event.venue, event.symbol)
            book = books.setdefault(key, BookState(
                venue=event.venue, symbol=event.symbol, max_levels=s.book_max_levels,
                feature_levels=s.book_feature_levels, trade_window_ms=s.book_trade_window_seconds * 1000,
                dedup_events=s.book_dedup_events,
            ))
            feature, reason = book.handle(event)
            if event.event_type == "book_snapshot":
                resync_pending.discard(key)
            needs_resync = reason == "awaiting_snapshot" or (reason and reason.startswith("sequence_gap"))
            if needs_resync and event.venue in {"binance", "coinbase"} and key not in resync_pending:
                command = canonical_json({
                    "venue": event.venue, "symbol": event.symbol, "reason": reason,
                    "source_event_id": event.event_id,
                })
                await producer.send_and_wait(
                    "market.control.resync", command.encode(), key=f"{event.venue}:{event.symbol}".encode()
                )
                resync_pending.add(key)
            if feature:
                await producer.send_and_wait(output_topic, feature.kafka_bytes(), key=feature.kafka_key())
            # Commit only after all side effects for this input event succeeded.
            # A crash before this point replays the event; deterministic IDs + dedup make replay safe.
            await consumer.commit()
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
