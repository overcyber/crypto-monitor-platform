from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

DEFAULT_PATH = "/opt/app/config/markets.yaml"


@dataclass(frozen=True, slots=True)
class SourceConfig:
    name: str
    enabled: bool
    quote: str
    products: tuple[str, ...]
    channels: tuple[str, ...]
    depth_interval: str = "100ms"
    snapshot_limit: int = 1000


UNSUPPORTED_ASSETS: dict[str, set[str]] = {
    "binance": {"XMR"},
    "coinbase": {"XMR"},
}


@dataclass(frozen=True, slots=True)
class MarketConfig:
    version: int
    reload_seconds: float
    watchlist: tuple[str, ...]
    sources: dict[str, SourceConfig]

    def source(self, name: str) -> SourceConfig:
        key = name.strip().lower()
        if key not in self.sources:
            raise KeyError(f"source not configured: {name}")
        return self.sources[key]

    def products_for(self, name: str) -> list[str]:
        source = self.source(name)
        if not source.enabled:
            return []
        if source.products:
            return list(source.products)
        unsupported = UNSUPPORTED_ASSETS.get(name.lower(), set())
        assets = [a for a in self.watchlist if a.upper() not in unsupported]
        if name.lower() == "binance":
            return [f"{asset}{source.quote}".upper().replace("-", "").replace("/", "") for asset in assets]
        if name.lower() == "coinbase":
            return [f"{asset}-{source.quote}".upper() for asset in assets]
        if name.lower() == "kraken":
            return [f"{asset}/{source.quote}".upper() for asset in assets]
        return list(assets)

    def resolve_product(self, name: str, symbol: str) -> str:
        """Resolve a base asset (BTC) or exchange-native product to the configured product."""
        venue = name.strip().lower()
        raw = symbol.strip().upper()
        products = self.products_for(venue)
        if raw in products:
            return raw
        source = self.source(venue)
        if venue == "binance":
            candidate = f"{raw}{source.quote}".replace("-", "").replace("/", "")
        elif venue == "coinbase":
            candidate = f"{raw}-{source.quote}"
        elif venue == "kraken":
            candidate = f"{raw}/{source.quote}" if "/" not in raw else raw
            if candidate not in products:
                clean_raw = raw.replace("-", "").replace("/", "")
                for p in products:
                    if p.replace("/", "").replace("-", "") == clean_raw:
                        return p
        else:
            candidate = raw
        if candidate in products:
            return candidate
        return candidate if candidate else raw

    def fingerprint(self, name: str) -> str:
        src = self.source(name)
        payload = "|".join([
            str(self.version), str(src.enabled), src.quote,
            ",".join(self.products_for(name)), ",".join(src.channels),
            src.depth_interval, str(src.snapshot_limit),
        ])
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def config_path() -> Path:
    return Path(os.getenv("MARKET_CONFIG", DEFAULT_PATH))


def _clean_list(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        value = [x.strip() for x in value.split(",")]
    if not isinstance(value, list):
        raise ValueError("expected a YAML list")
    return tuple(str(x).strip().upper() for x in value if str(x).strip())


def load_market_config(path: str | Path | None = None) -> MarketConfig:
    p = Path(path) if path is not None else config_path()
    if not p.exists():
        # Backward-compatible fallback for old .env deployments.
        binance = tuple(x.strip().upper() for x in os.getenv("BINANCE_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",") if x.strip())
        coinbase = tuple(x.strip().upper() for x in os.getenv("COINBASE_PRODUCTS", "BTC-USD,ETH-USD,SOL-USD").split(",") if x.strip())
        return MarketConfig(
            version=1,
            reload_seconds=5.0,
            watchlist=(),
            sources={
                "binance": SourceConfig("binance", True, "USDT", binance, ("DEPTH", "TRADE", "BOOK_TICKER")),
                "coinbase": SourceConfig("coinbase", True, "USD", coinbase, ("LEVEL2", "MATCHES", "TICKER")),
                "kraken": SourceConfig("kraken", False, "USD", (), ("TICKER", "TRADE", "BOOK")),
            },
        )

    raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError("markets.yaml root must be a mapping")

    watchlist = _clean_list(raw.get("watchlist", []))
    reload_seconds = max(1.0, float(raw.get("reload_seconds", 5)))
    source_raw = raw.get("sources", {}) or {}
    if not isinstance(source_raw, dict):
        raise ValueError("sources must be a mapping")

    defaults = {
        "binance": {"quote": "USDT", "channels": ["depth", "trade", "book_ticker"]},
        "coinbase": {"quote": "USD", "channels": ["level2", "matches", "ticker"]},
        "kraken": {"quote": "USD", "channels": ["ticker", "trade", "book"]},
    }
    sources: dict[str, SourceConfig] = {}
    for name in ("binance", "coinbase", "kraken"):
        cfg = dict(defaults[name])
        has_custom = name in source_raw
        cfg.update(source_raw.get(name, {}) or {})
        enabled = bool(cfg.get("enabled", has_custom if name == "kraken" else True))
        quote = str(cfg.get("quote", defaults[name]["quote"])).strip().upper()
        products = _clean_list(cfg.get("products", []))
        channels = _clean_list(cfg.get("channels", defaults[name]["channels"]))
        sources[name] = SourceConfig(
            name=name,
            enabled=enabled,
            quote=quote,
            products=products,
            channels=channels,
            depth_interval=str(cfg.get("depth_interval", "100ms")),
            snapshot_limit=max(5, min(5000, int(cfg.get("snapshot_limit", 1000)))),
        )

    result = MarketConfig(
        version=int(raw.get("version", 1)),
        reload_seconds=reload_seconds,
        watchlist=watchlist,
        sources=sources,
    )
    if not any(result.products_for(name) for name in sources):
        raise ValueError("markets.yaml selects no products; populate watchlist or sources.<exchange>.products")
    return result
