from __future__ import annotations

import hashlib
import time
from typing import Any

import orjson
from pydantic import BaseModel, ConfigDict, Field


SCHEMA_VERSION = 1


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def canonical_json(value: Any) -> str:
    return orjson.dumps(value, option=orjson.OPT_SORT_KEYS).decode()


def stable_id(*parts: object) -> str:
    raw = "\x1f".join("" if p is None else str(p) for p in parts).encode("utf-8", "replace")
    return hashlib.sha256(raw).hexdigest()


class MarketEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: int = SCHEMA_VERSION
    event_id: str
    source_id: str
    venue: str
    channel: str
    event_type: str
    symbol: str
    sequence_id: int | None = None
    first_sequence_id: int | None = None
    event_time_ms: int
    ingest_time_ms: int = Field(default_factory=now_ms)
    price: float | None = None
    quantity: float | None = None
    side: str | None = None
    bid_price: float | None = None
    bid_quantity: float | None = None
    ask_price: float | None = None
    ask_quantity: float | None = None
    bids_json: str = "[]"
    asks_json: str = "[]"
    raw_payload: str
    metadata_json: str = "{}"

    @classmethod
    def build(
        cls,
        *,
        venue: str,
        channel: str,
        event_type: str,
        symbol: str,
        source_id: str,
        event_time_ms: int,
        raw: Any,
        **kwargs: Any,
    ) -> "MarketEvent":
        event_id = stable_id(venue.lower(), channel.lower(), symbol.upper(), event_type.lower(), source_id)
        return cls(
            event_id=event_id,
            source_id=str(source_id),
            venue=venue.lower(),
            channel=channel.lower(),
            event_type=event_type.lower(),
            symbol=symbol.upper(),
            event_time_ms=int(event_time_ms),
            raw_payload=raw if isinstance(raw, str) else canonical_json(raw),
            **kwargs,
        )

    def kafka_bytes(self) -> bytes:
        return orjson.dumps(self.model_dump())

    def kafka_key(self) -> bytes:
        return f"{self.venue}:{self.symbol}".encode()


class MicrostructureEvent(BaseModel):
    feature_id: str
    source_event_id: str
    venue: str
    symbol: str
    event_time_ms: int
    ingest_time_ms: int = Field(default_factory=now_ms)
    best_bid: float | None = None
    best_ask: float | None = None
    mid: float | None = None
    spread: float | None = None
    spread_bps: float | None = None
    microprice: float | None = None
    microprice_offset_bps: float | None = None
    imbalance_l1: float | None = None
    imbalance_l5: float | None = None
    imbalance_l10: float | None = None
    imbalance_l20: float | None = None
    weighted_imbalance: float | None = None
    bid_depth_10bps: float | None = None
    ask_depth_10bps: float | None = None
    ofi: float | None = None
    tfi_60s: float | None = None
    metadata_json: str = "{}"

    def kafka_bytes(self) -> bytes:
        return orjson.dumps(self.model_dump())

    def kafka_key(self) -> bytes:
        return f"{self.venue}:{self.symbol}".encode()
