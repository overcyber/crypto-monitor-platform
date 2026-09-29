import tempfile
import unittest
from pathlib import Path

from src.common.markets import load_market_config


class MarketConfigTest(unittest.TestCase):
    def _write(self, text: str) -> Path:
        tmp = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
        tmp.write(text)
        tmp.close()
        self.addCleanup(lambda: Path(tmp.name).unlink(missing_ok=True))
        return Path(tmp.name)

    def test_watchlist_derives_exchange_products(self):
        p = self._write("""
version: 1
reload_seconds: 3
watchlist: [BTC, eth, SOL]
sources:
  binance:
    enabled: true
    quote: USDT
    products: []
    channels: [depth, trade, book_ticker]
  coinbase:
    enabled: true
    quote: USD
    products: []
    channels: [level2, matches, ticker]
""")
        cfg = load_market_config(p)
        self.assertEqual(cfg.watchlist, ("BTC", "ETH", "SOL"))
        self.assertEqual(cfg.products_for("binance"), ["BTCUSDT", "ETHUSDT", "SOLUSDT"])
        self.assertEqual(cfg.products_for("coinbase"), ["BTC-USD", "ETH-USD", "SOL-USD"])
        self.assertEqual(cfg.reload_seconds, 3.0)

    def test_explicit_products_override_watchlist(self):
        p = self._write("""
watchlist: [BTC, ETH]
sources:
  binance:
    enabled: true
    quote: USDT
    products: [PEPEUSDT, DOGEUSDT]
    channels: [trade, book_ticker]
  coinbase:
    enabled: false
    quote: USD
    products: []
    channels: [ticker]
""")
        cfg = load_market_config(p)
        self.assertEqual(cfg.products_for("binance"), ["PEPEUSDT", "DOGEUSDT"])
        self.assertEqual(cfg.products_for("coinbase"), [])

    def test_fingerprint_changes_when_selection_changes(self):
        p1 = self._write("""
watchlist: [BTC]
sources:
  binance: {enabled: true, quote: USDT, products: [], channels: [trade]}
  coinbase: {enabled: false, quote: USD, products: [], channels: [ticker]}
""")
        p2 = self._write("""
watchlist: [BTC, XRP]
sources:
  binance: {enabled: true, quote: USDT, products: [], channels: [trade]}
  coinbase: {enabled: false, quote: USD, products: [], channels: [ticker]}
""")
        self.assertNotEqual(load_market_config(p1).fingerprint("binance"), load_market_config(p2).fingerprint("binance"))

    def test_empty_selection_is_rejected(self):
        p = self._write("""
watchlist: []
sources:
  binance: {enabled: false, products: []}
  coinbase: {enabled: false, products: []}
""")
        with self.assertRaises(ValueError):
            load_market_config(p)
