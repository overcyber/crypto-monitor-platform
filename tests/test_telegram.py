import tempfile
import unittest
from pathlib import Path

import yaml

from src.telegram_bot.config import configure_price_alert, set_monitored
from src.telegram_bot.price_alerts import PriceState, evaluate_price


class TelegramConfigTest(unittest.TestCase):
    def test_monitor_add_remove(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'markets.yaml'
            p.write_text(yaml.safe_dump({'watchlist':['BTC'],'sources':{'binance':{'quote':'USDT','products':[]}}}))
            self.assertEqual(set_monitored('ETH', True, p), ['BTC','ETH'])
            self.assertEqual(set_monitored('BTC', False, p), ['ETH'])

    def test_price_config(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'telegram.yaml'
            p.write_text(yaml.safe_dump({'defaults':{'venue':'binance','quote':'USDT'},'price_alerts':{}}))
            r=configure_price_alert('BTC', percent_up=2, percent_down=3, high=86000, low=82000, path=p)
            self.assertEqual(r['symbol'],'BTCUSDT')
            self.assertEqual(r['percent_up'],2.0)
            self.assertEqual(r['low'],82000.0)

    def test_price_config_xmr_auto_kraken(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'telegram.yaml'
            p.write_text(yaml.safe_dump({'defaults':{'venue':'binance','quote':'USDT'},'price_alerts':{}}))
            r=configure_price_alert('XMR', percent_up=2.5, path=p)
            self.assertEqual(r['venue'],'kraken')
            self.assertEqual(r['symbol'],'XMR/USD')
            self.assertEqual(r['percent_up'],2.5)


class PriceEngineTest(unittest.TestCase):
    def test_percent_reanchors_after_alert(self):
        s=PriceState()
        rule={'percent_up':2,'percent_down':3,'cooldown_seconds':0}
        self.assertEqual(evaluate_price(rule,s,100,1000),[])
        out=evaluate_price(rule,s,102,2000)
        self.assertEqual(out[0]['kind'],'percent_up')
        self.assertEqual(s.anchor_price,102)

    def test_level_alert_is_crossing_based(self):
        s=PriceState()
        rule={'high':110,'low':90,'cooldown_seconds':0}
        evaluate_price(rule,s,100,1000)
        self.assertEqual(evaluate_price(rule,s,111,2000)[0]['kind'],'high')
        self.assertEqual(evaluate_price(rule,s,112,3000),[])
        evaluate_price(rule,s,100,4000)
        self.assertEqual(evaluate_price(rule,s,89,5000)[0]['kind'],'low')


if __name__ == '__main__':
    unittest.main()

class TelegramScriptsRegressionTest(unittest.TestCase):
    def test_scripts_no_invalid_escaped_fstring(self):
        root=Path(__file__).resolve().parents[1]
        discover=(root/'scripts/telegram-discover.sh').read_text()
        test=(root/'scripts/telegram-test.sh').read_text()
        self.assertNotIn('f"chat_id=', discover)
        self.assertIn('/getMe', discover)
        self.assertIn('/getMe', test)
        self.assertIn('format(cid', discover)
