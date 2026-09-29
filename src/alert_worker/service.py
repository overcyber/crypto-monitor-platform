from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from src.common.events import now_ms, stable_id
from src.common.kafka import create_consumer, create_producer

CONFIG_PATH = Path(os.getenv("ALERT_CONFIG", "/opt/app/config/alerts.yaml"))
RELOAD_SECONDS = max(1, int(os.getenv("ALERT_CONFIG_RELOAD_SECONDS", "5")))


@dataclass(frozen=True, slots=True)
class Rule:
    id: str
    enabled: bool
    source: str
    field: str
    operator: str
    value: float
    cooldown_seconds: int = 60
    venue: str | None = None
    symbol: str | None = None
    event_type: str | None = None

    @classmethod
    def from_mapping(cls, obj: dict[str, Any]) -> "Rule":
        return cls(
            id=str(obj["id"]),
            enabled=bool(obj.get("enabled", True)),
            source=str(obj.get("source", "market")).lower(),
            field=str(obj["field"]),
            operator=str(obj.get("operator", "gte")).lower(),
            value=float(obj["value"]),
            cooldown_seconds=max(0, int(obj.get("cooldown_seconds", 60))),
            venue=str(obj["venue"]).lower() if obj.get("venue") else None,
            symbol=str(obj["symbol"]).upper() if obj.get("symbol") else None,
            event_type=str(obj["event_type"]).lower() if obj.get("event_type") else None,
        )


def compare(value: float, operator: str, threshold: float) -> bool:
    if operator == "gt":
        return value > threshold
    if operator == "gte":
        return value >= threshold
    if operator == "lt":
        return value < threshold
    if operator == "lte":
        return value <= threshold
    if operator == "abs_gt":
        return abs(value) > threshold
    if operator == "abs_gte":
        return abs(value) >= threshold
    raise ValueError(f"unsupported alert operator: {operator}")


class RuleSet:
    def __init__(self, path: Path = CONFIG_PATH) -> None:
        self.path = path
        self.rules: list[Rule] = []
        self._mtime_ns: int | None = None
        self._last_check = 0.0

    def reload_if_needed(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_check < RELOAD_SECONDS:
            return
        self._last_check = now
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            self.rules = []
            self._mtime_ns = None
            return
        if not force and self._mtime_ns == stat.st_mtime_ns:
            return
        raw = yaml.safe_load(self.path.read_text()) or {}
        rules = []
        for item in raw.get("rules", []):
            try:
                rule = Rule.from_mapping(item)
                if rule.enabled:
                    rules.append(rule)
            except Exception as exc:
                print(f"alert-worker ignoring invalid rule={item!r}: {exc}", flush=True)
        self.rules = rules
        self._mtime_ns = stat.st_mtime_ns
        print(f"alert-worker loaded rules={len(rules)} path={self.path}", flush=True)


def _source_for_topic(topic: str) -> str:
    return "microstructure" if topic.endswith("microstructure") else "market"


def evaluate(rule: Rule, topic: str, event: dict[str, Any]) -> dict[str, Any] | None:
    source = _source_for_topic(topic)
    if source != rule.source:
        return None
    venue = str(event.get("venue") or "").lower()
    symbol = str(event.get("symbol") or "").upper()
    event_type = str(event.get("event_type") or "").lower()
    if rule.venue and venue != rule.venue:
        return None
    if rule.symbol and symbol != rule.symbol:
        return None
    if rule.event_type and event_type != rule.event_type:
        return None
    raw = event.get(rule.field)
    if raw is None:
        return None
    try:
        observed = float(raw)
    except (TypeError, ValueError):
        return None
    if not compare(observed, rule.operator, rule.value):
        return None
    source_id = str(event.get("event_id") or event.get("feature_id") or event.get("source_event_id") or "")
    event_time_ms = int(event.get("event_time_ms") or now_ms())
    alert_id = stable_id("alert", rule.id, venue, symbol, source_id)
    return {
        "alert_id": alert_id,
        "rule_id": rule.id,
        "source_topic": topic,
        "source_id": source_id,
        "venue": venue,
        "symbol": symbol,
        "event_type": event_type or source,
        "field": rule.field,
        "operator": rule.operator,
        "threshold": rule.value,
        "observed": observed,
        "event_time_ms": event_time_ms,
        "created_at_ms": now_ms(),
        "details_json": json.dumps({"source": source}, separators=(",", ":")),
    }


async def main() -> None:
    rules = RuleSet()
    rules.reload_if_needed(force=True)
    consumer = await create_consumer(
        "market.cleaned", "market.microstructure",
        group_id="alert-worker-v4", auto_offset_reset="latest", enable_auto_commit=False,
    )
    producer = await create_producer()
    cooldown_until: dict[tuple[str, str, str], int] = {}
    try:
        async for msg in consumer:
            rules.reload_if_needed()
            try:
                event = json.loads(msg.value)
            except Exception as exc:
                await producer.send_and_wait(
                    "market.deadletter",
                    json.dumps({"component": "alert-worker", "error": str(exc)}).encode(),
                )
                await consumer.commit()
                continue
            for rule in rules.rules:
                alert = evaluate(rule, msg.topic, event)
                if not alert:
                    continue
                key = (rule.id, alert["venue"], alert["symbol"])
                current = now_ms()
                if current < cooldown_until.get(key, 0):
                    continue
                cooldown_until[key] = current + rule.cooldown_seconds * 1000
                payload = json.dumps(alert, separators=(",", ":")).encode()
                await producer.send_and_wait("market.alerts", payload, key=f"{alert['venue']}:{alert['symbol']}".encode())
                print(
                    f"ALERT rule={rule.id} venue={alert['venue']} symbol={alert['symbol']} "
                    f"{rule.field}={alert['observed']}",
                    flush=True,
                )
            # Commit after every rule evaluation/publication for this source event.
            # Duplicate alerts after a crash keep a deterministic alert_id and are collapsed in ClickHouse FINAL.
            await consumer.commit()
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
