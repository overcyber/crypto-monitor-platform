from __future__ import annotations

import asyncio
import html
import json
import os
import time
from pathlib import Path
from typing import Any

import httpx

from src.common.kafka import create_consumer
from src.telegram_bot.config import (
    alert_severity,
    clear_price_alert,
    configure_price_alert,
    list_watchlist,
    load_telegram,
    set_monitored,
)
from src.telegram_bot.price_alerts import PriceState, evaluate_price

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
ENABLED = os.getenv("TELEGRAM_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
ALLOWED = {int(x.strip()) for x in os.getenv("TELEGRAM_ALLOWED_CHAT_IDS", "").split(",") if x.strip()}
STATE_PATH = Path(os.getenv("TELEGRAM_STATE", "/opt/app/data/telegram/state.json"))
POLL_TIMEOUT = max(5, int(os.getenv("TELEGRAM_POLL_TIMEOUT_SECONDS", "30")))
MONITOR_URL = os.getenv("MONITOR_API_INTERNAL_URL", "http://monitor-api:8081").rstrip("/")

HELP = """Comandos:
/status - estado do bot
/test - testar resposta do bot
/price BTC - cotação atual
/watchlist - moedas monitoradas
/monitor BTC - adicionar moeda
/unmonitor BTC - remover moeda
/setpct BTC 2.0 - alerta ±2%
/setpct BTC 2.0 3.0 - +2% / -3%
/setprice BTC 86000 82000 - teto/piso
/clearalert BTC - remover alertas de preço
/alerts - listar alertas de preço
/help - ajuda"""


def _state_load() -> dict[str, PriceState]:
    try:
        raw = json.loads(STATE_PATH.read_text())
    except Exception:
        return {}
    return {k: PriceState(**v) for k, v in raw.items()}


def _state_save(states: dict[str, PriceState]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps({k: vars(v) for k, v in states.items()}, separators=(",", ":")))
    os.replace(tmp, STATE_PATH)


def _allowed(chat_id: int) -> bool:
    return chat_id in ALLOWED


def _fmt_price(v: float) -> str:
    return f"${v:,.8f}".rstrip("0").rstrip(".")


class TelegramBot:
    def __init__(self) -> None:
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(POLL_TIMEOUT + 10.0))
        self.offset = 0
        self.states = _state_load()
        self.config_lock = asyncio.Lock()

    async def api(self, method: str, payload: dict[str, Any]) -> Any:
        r = await self.client.post(f"https://api.telegram.org/bot{TOKEN}/{method}", json=payload)
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            raise RuntimeError(data)
        return data.get("result")

    async def send(self, chat_id: int, text: str) -> None:
        await self.api("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True})

    async def broadcast(self, text: str) -> None:
        for chat_id in sorted(ALLOWED):
            try:
                await self.send(chat_id, text)
            except Exception as exc:
                print(f"telegram send failed chat={chat_id}: {exc}", flush=True)

    async def command_loop(self) -> None:
        while True:
            try:
                updates = await self.api("getUpdates", {
                    "offset": self.offset,
                    "timeout": POLL_TIMEOUT,
                    "allowed_updates": ["message"],
                })
                for update in updates:
                    self.offset = max(self.offset, int(update["update_id"]) + 1)
                    msg = update.get("message") or {}
                    text = str(msg.get("text") or "").strip()
                    chat_id = int((msg.get("chat") or {}).get("id") or 0)
                    if not text or not _allowed(chat_id):
                        continue
                    await self.handle_command(chat_id, text)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"telegram polling error: {exc}", flush=True)
                await asyncio.sleep(3)

    async def handle_command(self, chat_id: int, text: str) -> None:
        parts = text.split()
        cmd = parts[0].split("@", 1)[0].lower()
        try:
            if cmd in {"/help", "/start"}:
                await self.send(chat_id, f"<pre>{html.escape(HELP)}</pre>")
            elif cmd == "/status":
                await self.send(chat_id, f"✅ Telegram bot ativo\nKafka: market.cleaned + market.alerts\nWatchlist: {', '.join(list_watchlist()) or '-'}")
            elif cmd == "/test":
                await self.send(chat_id, "✅ Bot operacional")
            elif cmd == "/price":
                if len(parts) != 2:
                    raise ValueError("uso: /price BTC")
                asset = str(parts[1]).upper()
                r = await self.client.get(f"{MONITOR_URL}/v1/quote/{asset}", params={"venue": "binance"}, timeout=8.0)
                r.raise_for_status()
                q = r.json()
                await self.send(chat_id, f"💰 <b>{asset}</b> {_fmt_price(float(q['price']))}\n{html.escape(str(q.get('event_time','')))}\nage={q.get('age_seconds','-')}s")
            elif cmd == "/watchlist":
                await self.send(chat_id, "📊 " + (", ".join(list_watchlist()) or "watchlist vazia"))
            elif cmd in {"/monitor", "/unmonitor"}:
                if len(parts) != 2:
                    raise ValueError("uso: /monitor BTC ou /unmonitor BTC")
                async with self.config_lock:
                    watch = set_monitored(parts[1], cmd == "/monitor")
                action = "adicionada" if cmd == "/monitor" else "removida"
                await self.send(chat_id, f"✅ {parts[1].upper()} {action}. Watchlist: {', '.join(watch) or '-'}")
            elif cmd == "/setpct":
                if len(parts) not in {3, 4}:
                    raise ValueError("uso: /setpct BTC 2.0 [3.0]")
                up = float(parts[2]); down = float(parts[3]) if len(parts) == 4 else up
                async with self.config_lock:
                    set_monitored(parts[1], True)
                    rule = configure_price_alert(parts[1], percent_up=up, percent_down=down)
                    self.states.pop(str(parts[1]).upper(), None)
                    _state_save(self.states)
                await self.send(chat_id, f"✅ {parts[1].upper()}: +{up:g}% / -{down:g}% ({rule['symbol']})")
            elif cmd == "/setprice":
                if len(parts) != 4:
                    raise ValueError("uso: /setprice BTC 86000 82000")
                high, low = float(parts[2]), float(parts[3])
                if high <= low:
                    raise ValueError("o teto deve ser maior que o piso")
                async with self.config_lock:
                    set_monitored(parts[1], True)
                    rule = configure_price_alert(parts[1], high=high, low=low)
                    self.states.pop(str(parts[1]).upper(), None)
                    _state_save(self.states)
                await self.send(chat_id, f"✅ {parts[1].upper()}: ≥ {_fmt_price(high)} ou ≤ {_fmt_price(low)} ({rule['symbol']})")
            elif cmd == "/clearalert":
                if len(parts) != 2:
                    raise ValueError("uso: /clearalert BTC")
                async with self.config_lock:
                    existed = clear_price_alert(parts[1])
                    self.states.pop(str(parts[1]).upper(), None)
                    _state_save(self.states)
                await self.send(chat_id, "✅ removido" if existed else "ℹ️ não havia alerta")
            elif cmd == "/alerts":
                cfg = load_telegram(); rules = cfg.get("price_alerts") or {}
                if not rules:
                    await self.send(chat_id, "Sem alertas de preço.")
                else:
                    lines = []
                    for asset, rule in sorted(rules.items()):
                        lines.append(f"{asset}: +{rule.get('percent_up','-')}% / -{rule.get('percent_down','-')}% | high={rule.get('high','-')} low={rule.get('low','-')}")
                    await self.send(chat_id, "<pre>" + html.escape("\n".join(lines)) + "</pre>")
            else:
                await self.send(chat_id, f"Comando desconhecido.\n<pre>{html.escape(HELP)}</pre>")
        except Exception as exc:
            await self.send(chat_id, f"❌ {html.escape(str(exc))}")

    async def kafka_loop(self) -> None:
        consumer = await create_consumer(
            "market.cleaned", "market.alerts",
            group_id="telegram-bot-v4", auto_offset_reset="latest", enable_auto_commit=False,
        )
        try:
            async for msg in consumer:
                try:
                    event = json.loads(msg.value)
                    if msg.topic == "market.alerts":
                        await self.handle_critical_alert(event)
                    else:
                        await self.handle_price_event(event)
                    await consumer.commit()
                except Exception as exc:
                    print(f"telegram kafka event error: {exc}", flush=True)
        finally:
            await consumer.stop()

    async def handle_critical_alert(self, alert: dict[str, Any]) -> None:
        cfg = load_telegram()
        forwarding = cfg.get("forward_alerts") or {}
        if not forwarding.get("enabled", True):
            return
        allowed_sev = {str(x).lower() for x in forwarding.get("severities", ["critical"])}
        severity = alert_severity(str(alert.get("rule_id", "")))
        if severity not in allowed_sev:
            return
        text = (
            f"🚨 <b>{html.escape(severity.upper())}</b>\n"
            f"{html.escape(str(alert.get('rule_id','alert')))}\n"
            f"{html.escape(str(alert.get('symbol','')))} {html.escape(str(alert.get('field','')))}="
            f"{html.escape(str(alert.get('observed','')))} threshold={html.escape(str(alert.get('threshold','')))}"
        )
        await self.broadcast(text)

    async def handle_price_event(self, event: dict[str, Any]) -> None:
        price = event.get("price")
        if price is None:
            return
        venue = str(event.get("venue") or "").lower()
        symbol = str(event.get("symbol") or "").upper()
        cfg = load_telegram()
        rules = cfg.get("price_alerts") or {}
        now = int(time.time() * 1000)
        changed = False
        for asset, rule in rules.items():
            if not rule.get("enabled", True):
                continue
            if venue != str(rule.get("venue", "binance")).lower() or symbol != str(rule.get("symbol", "")).upper():
                continue
            key = str(asset).upper()
            state = self.states.setdefault(key, PriceState())
            notices = evaluate_price(rule, state, float(price), now)
            changed = True
            for notice in notices:
                kind = notice["kind"]
                if kind == "high":
                    text = f"📈 <b>{key}</b> atingiu {_fmt_price(notice['price'])} (alerta ≥ {_fmt_price(notice['threshold'])})"
                elif kind == "low":
                    text = f"📉 <b>{key}</b> caiu para {_fmt_price(notice['price'])} (alerta ≤ {_fmt_price(notice['threshold'])})"
                else:
                    sign = "+" if notice["change_pct"] >= 0 else ""
                    text = f"📊 <b>{key}</b> {sign}{notice['change_pct']:.2f}% → {_fmt_price(notice['price'])}"
                await self.broadcast(text)
        if changed:
            _state_save(self.states)

    async def close(self) -> None:
        await self.client.aclose()


async def main() -> None:
    if not ENABLED:
        print("telegram-bot disabled (TELEGRAM_ENABLED=false)", flush=True)
        while True:
            await asyncio.sleep(3600)
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required when TELEGRAM_ENABLED=true")
    if not ALLOWED:
        raise RuntimeError("TELEGRAM_ALLOWED_CHAT_IDS is required when TELEGRAM_ENABLED=true")
    bot = TelegramBot()
    await bot.broadcast("✅ Crypto Monitor Telegram bot iniciado")
    try:
        await asyncio.gather(bot.command_loop(), bot.kafka_loop())
    finally:
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())
