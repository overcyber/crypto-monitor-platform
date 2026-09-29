from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class PriceState:
    anchor_price: float | None = None
    last_price: float | None = None
    above_high: bool = False
    below_low: bool = False
    last_percent_alert_ms: int = 0


def evaluate_price(rule: dict[str, Any], state: PriceState, price: float, now_ms: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    price = float(price)
    cooldown_ms = max(0, int(rule.get("cooldown_seconds", 60))) * 1000

    high = rule.get("high")
    if high is not None:
        high = float(high)
        hit = price >= high
        if hit and not state.above_high:
            out.append({"kind": "high", "price": price, "threshold": high})
        state.above_high = hit

    low = rule.get("low")
    if low is not None:
        low = float(low)
        hit = price <= low
        if hit and not state.below_low:
            out.append({"kind": "low", "price": price, "threshold": low})
        state.below_low = hit

    if state.anchor_price is None or state.anchor_price <= 0:
        state.anchor_price = price
    else:
        change_pct = (price / state.anchor_price - 1.0) * 100.0
        up = rule.get("percent_up")
        down = rule.get("percent_down")
        can_fire = now_ms - state.last_percent_alert_ms >= cooldown_ms
        if up is not None and change_pct >= float(up) and can_fire:
            out.append({"kind": "percent_up", "price": price, "change_pct": change_pct, "anchor": state.anchor_price})
            state.anchor_price = price
            state.last_percent_alert_ms = now_ms
        elif down is not None and change_pct <= -float(down) and can_fire:
            out.append({"kind": "percent_down", "price": price, "change_pct": change_pct, "anchor": state.anchor_price})
            state.anchor_price = price
            state.last_percent_alert_ms = now_ms

    state.last_price = price
    return out
