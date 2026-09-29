from __future__ import annotations

import json
import math
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from src.common.events import MarketEvent, MicrostructureEvent, canonical_json, now_ms, stable_id


def _levels(raw: str) -> list[tuple[float, float]]:
    try:
        return [(float(p), float(q)) for p, q, *_ in json.loads(raw or "[]")]
    except Exception:
        return []


def imbalance(bid: float, ask: float) -> float | None:
    total = bid + ask
    return (bid - ask) / total if total > 0 else None


@dataclass(slots=True)
class BookState:
    venue: str
    symbol: str
    max_levels: int = 1000
    feature_levels: int = 20
    trade_window_ms: int = 60_000
    dedup_events: int = 50_000
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    last_sequence: int | None = None
    ready: bool = False
    buffered: list[MarketEvent] = field(default_factory=list)
    trades: deque[tuple[int, float]] = field(default_factory=deque)
    prev_bbo: tuple[float, float, float, float] | None = None
    last_ofi: float | None = None
    seen_event_ids: set[str] = field(default_factory=set)
    seen_event_order: deque[str] = field(default_factory=deque)

    def _seen(self, event_id: str) -> bool:
        if event_id in self.seen_event_ids:
            return True
        self.seen_event_ids.add(event_id)
        self.seen_event_order.append(event_id)
        while len(self.seen_event_order) > self.dedup_events:
            old = self.seen_event_order.popleft()
            self.seen_event_ids.discard(old)
        return False

    def _trim(self) -> None:
        if len(self.bids) > self.max_levels:
            keep = set(sorted(self.bids, reverse=True)[: self.max_levels])
            self.bids = {p: q for p, q in self.bids.items() if p in keep}
        if len(self.asks) > self.max_levels:
            keep = set(sorted(self.asks)[: self.max_levels])
            self.asks = {p: q for p, q in self.asks.items() if p in keep}

    def _apply_levels(self, target: dict[float, float], levels: list[tuple[float, float]]) -> None:
        for price, qty in levels:
            if qty <= 0:
                target.pop(price, None)
            else:
                target[price] = qty
        self._trim()

    def snapshot(self, event: MarketEvent) -> tuple[bool, str | None]:
        self.bids.clear(); self.asks.clear()
        self._apply_levels(self.bids, _levels(event.bids_json))
        self._apply_levels(self.asks, _levels(event.asks_json))
        self.last_sequence = event.sequence_id
        self.ready = True
        self.prev_bbo = None
        if self.venue == "binance" and self.last_sequence is not None:
            buffered = sorted(self.buffered, key=lambda e: (e.sequence_id or 0, e.ingest_time_ms))
            self.buffered.clear()
            for delta in buffered:
                if delta.sequence_id is not None and delta.sequence_id <= self.last_sequence:
                    continue
                ok, reason = self.delta(delta)
                if not ok:
                    return False, reason
        else:
            self.buffered.clear()
        return True, None

    def delta(self, event: MarketEvent) -> tuple[bool, str | None]:
        if not self.ready:
            if len(self.buffered) < 20_000:
                self.buffered.append(event)
            return False, "awaiting_snapshot"

        if self.venue == "binance" and event.sequence_id is not None and self.last_sequence is not None:
            if event.sequence_id <= self.last_sequence:
                return True, "stale"
            first = event.first_sequence_id if event.first_sequence_id is not None else event.sequence_id
            expected = self.last_sequence + 1
            if first > expected or event.sequence_id < expected:
                self.ready = False
                self.buffered = [event]
                return False, f"sequence_gap expected={expected} got={first}:{event.sequence_id}"

        self._apply_levels(self.bids, _levels(event.bids_json))
        self._apply_levels(self.asks, _levels(event.asks_json))
        if event.sequence_id is not None:
            self.last_sequence = event.sequence_id
        return True, None

    def add_trade(self, event: MarketEvent) -> None:
        if event.quantity is None:
            return
        signed = float(event.quantity) if event.side == "buy" else -float(event.quantity) if event.side == "sell" else 0.0
        self.trades.append((event.event_time_ms, signed))
        cutoff = event.event_time_ms - self.trade_window_ms
        while self.trades and self.trades[0][0] < cutoff:
            self.trades.popleft()

    def _bbo_and_ofi(self) -> tuple[tuple[float, float, float, float] | None, float | None]:
        if not self.bids or not self.asks:
            return None, None
        bp = max(self.bids); ap = min(self.asks)
        bq = self.bids[bp]; aq = self.asks[ap]
        current = (bp, bq, ap, aq)
        if self.prev_bbo is None:
            self.prev_bbo = current
            return current, None
        pbp, pbq, pap, paq = self.prev_bbo
        bid_term = (bq if bp >= pbp else 0.0) - (pbq if bp <= pbp else 0.0)
        ask_term = -(aq if ap <= pap else 0.0) + (paq if ap >= pap else 0.0)
        ofi = bid_term + ask_term
        self.prev_bbo = current
        self.last_ofi = ofi
        return current, ofi

    def feature(self, source_event: MarketEvent) -> MicrostructureEvent | None:
        if not self.ready or not self.bids or not self.asks:
            return None
        bbo, ofi = self._bbo_and_ofi()
        if bbo is None:
            return None
        bp, bq, ap, aq = bbo
        mid = (bp + ap) / 2.0
        spread = ap - bp
        total = bq + aq
        micro = (ap * bq + bp * aq) / total if total else mid

        bids = sorted(self.bids.items(), reverse=True)[: self.feature_levels]
        asks = sorted(self.asks.items())[: self.feature_levels]

        def level_imb(n: int) -> float | None:
            b = sum(q for _, q in bids[:n]); a = sum(q for _, q in asks[:n])
            return imbalance(b, a)

        wb = sum(q / (i + 1) for i, (_, q) in enumerate(bids))
        wa = sum(q / (i + 1) for i, (_, q) in enumerate(asks))
        bid_floor = mid * (1 - 10 / 10_000)
        ask_ceil = mid * (1 + 10 / 10_000)
        db10 = sum(q for p, q in bids if p >= bid_floor)
        da10 = sum(q for p, q in asks if p <= ask_ceil)
        tfi = sum(v for _, v in self.trades)

        fid = stable_id("micro", self.venue, self.symbol, source_event.event_id)
        try:
            source_meta = json.loads(source_event.metadata_json or "{}")
        except Exception:
            source_meta = {}
        return MicrostructureEvent(
            feature_id=fid,
            source_event_id=source_event.event_id,
            venue=self.venue,
            symbol=self.symbol,
            event_time_ms=source_event.event_time_ms,
            best_bid=bp, best_ask=ap, mid=mid, spread=spread,
            spread_bps=(spread / mid * 10_000.0) if mid else None,
            microprice=micro,
            microprice_offset_bps=((micro - mid) / mid * 10_000.0) if mid else None,
            imbalance_l1=imbalance(bq, aq),
            imbalance_l5=level_imb(5), imbalance_l10=level_imb(10), imbalance_l20=level_imb(20),
            weighted_imbalance=imbalance(wb, wa), bid_depth_10bps=db10, ask_depth_10bps=da10,
            ofi=ofi, tfi_60s=tfi,
            metadata_json=canonical_json({"last_sequence": self.last_sequence, "book_levels": [len(self.bids), len(self.asks)], "source_metadata": source_meta}),
        )

    def handle(self, event: MarketEvent) -> tuple[MicrostructureEvent | None, str | None]:
        if self._seen(event.event_id):
            return None, None
        if event.event_type == "trade":
            self.add_trade(event)
            return None, None
        if event.event_type == "book_snapshot":
            ok, reason = self.snapshot(event)
            if not ok:
                return None, reason
            return self.feature(event), None
        if event.event_type == "book_delta":
            ok, reason = self.delta(event)
            if not ok:
                return None, reason
            if reason == "stale":
                return None, None
            return self.feature(event), None
        if event.event_type == "book_ticker":
            # BBO-only fallback; does not mutate deeper book if a real book is available.
            if not self.ready and event.bid_price and event.ask_price:
                self.bids = {event.bid_price: event.bid_quantity or 0.0}
                self.asks = {event.ask_price: event.ask_quantity or 0.0}
                self.ready = True
                return self.feature(event), None
        return None, None
