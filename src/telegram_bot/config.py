from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

MARKETS_PATH = Path(os.getenv("MARKET_CONFIG", "/opt/app/config/markets.yaml"))
TELEGRAM_PATH = Path(os.getenv("TELEGRAM_CONFIG", "/opt/app/config/telegram.yaml"))
ALERTS_PATH = Path(os.getenv("ALERT_CONFIG", "/opt/app/config/alerts.yaml"))


def _load(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text()) or {}


def _atomic_dump(path: Path, data: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True))
    os.replace(tmp, path)


def normalize_asset(asset: str) -> str:
    value = "".join(ch for ch in asset.upper().strip() if ch.isalnum())
    if not value or len(value) > 20:
        raise ValueError("ativo inválido")
    return value


def pair_for(asset: str, venue: str, quote: str) -> str:
    asset = normalize_asset(asset)
    venue = venue.lower()
    quote = normalize_asset(quote)
    return f"{asset}-{quote}" if venue == "coinbase" else f"{asset}{quote}"


def list_watchlist(path: Path = MARKETS_PATH) -> list[str]:
    return [str(x).upper() for x in _load(path).get("watchlist", [])]


def set_monitored(asset: str, enabled: bool, path: Path = MARKETS_PATH) -> list[str]:
    asset = normalize_asset(asset)
    data = _load(path)
    watch = [str(x).upper() for x in data.get("watchlist", [])]
    if enabled and asset not in watch:
        watch.append(asset)
    if not enabled:
        watch = [x for x in watch if x != asset]
    data["watchlist"] = watch

    # If explicit products are configured, keep them aligned too.
    for venue, cfg in (data.get("sources") or {}).items():
        products = cfg.get("products") or []
        if not products:
            continue
        quote = str(cfg.get("quote") or ("USD" if venue == "coinbase" else "USDT"))
        pair = pair_for(asset, venue, quote)
        normalized = [str(x).upper() for x in products]
        if enabled and pair not in normalized:
            normalized.append(pair)
        if not enabled:
            normalized = [x for x in normalized if x != pair]
        cfg["products"] = normalized

    _atomic_dump(path, data)
    return watch


def load_telegram(path: Path = TELEGRAM_PATH) -> dict[str, Any]:
    return _load(path)


def save_telegram(data: dict[str, Any], path: Path = TELEGRAM_PATH) -> None:
    _atomic_dump(path, data)


def configure_price_alert(
    asset: str,
    *,
    percent_up: float | None = None,
    percent_down: float | None = None,
    high: float | None = None,
    low: float | None = None,
    path: Path = TELEGRAM_PATH,
) -> dict[str, Any]:
    asset = normalize_asset(asset)
    data = _load(path)
    defaults = data.setdefault("defaults", {})
    venue = str(defaults.get("venue", "binance")).lower()
    quote = str(defaults.get("quote", "USDT")).upper()
    rules = data.setdefault("price_alerts", {})
    rule = rules.setdefault(asset, {})
    rule.setdefault("enabled", True)
    rule.setdefault("venue", venue)
    rule.setdefault("symbol", pair_for(asset, venue, quote))
    rule.setdefault("cooldown_seconds", int(defaults.get("cooldown_seconds", 60)))
    if percent_up is not None:
        rule["percent_up"] = float(percent_up)
    if percent_down is not None:
        rule["percent_down"] = float(percent_down)
    if high is not None:
        rule["high"] = float(high)
    if low is not None:
        rule["low"] = float(low)
    _atomic_dump(path, data)
    return rule


def clear_price_alert(asset: str, path: Path = TELEGRAM_PATH) -> bool:
    asset = normalize_asset(asset)
    data = _load(path)
    rules = data.setdefault("price_alerts", {})
    existed = rules.pop(asset, None) is not None
    _atomic_dump(path, data)
    return existed


def alert_severity(rule_id: str, path: Path = ALERTS_PATH) -> str:
    try:
        data = _load(path)
    except FileNotFoundError:
        return "warning"
    for rule in data.get("rules", []):
        if str(rule.get("id")) == str(rule_id):
            return str(rule.get("severity", "warning")).lower()
    return "warning"
