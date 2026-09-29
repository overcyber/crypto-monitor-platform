from __future__ import annotations

import json
from typing import Any, AsyncIterator

from .config import settings


async def create_producer():
    from aiokafka import AIOKafkaProducer
    s = settings()
    producer = AIOKafkaProducer(
        bootstrap_servers=s.kafka_bootstrap,
        acks="all",
        enable_idempotence=True,
        compression_type="gzip",
        request_timeout_ms=30_000,
        retry_backoff_ms=250,
    )
    await producer.start()
    return producer


async def create_consumer(
    *topics: str,
    group_id: str,
    auto_offset_reset: str = "earliest",
    enable_auto_commit: bool = True,
):
    from aiokafka import AIOKafkaConsumer
    s = settings()
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=s.kafka_bootstrap,
        group_id=group_id,
        enable_auto_commit=enable_auto_commit,
        auto_offset_reset=auto_offset_reset,
        isolation_level="read_committed",
        max_poll_records=1000,
    )
    await consumer.start()
    return consumer


async def iter_json(consumer) -> AsyncIterator[tuple[Any, dict[str, Any]]]:
    async for message in consumer:
        try:
            yield message, json.loads(message.value)
        except Exception:
            continue
