from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True, slots=True)
class Candle:
    open_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
