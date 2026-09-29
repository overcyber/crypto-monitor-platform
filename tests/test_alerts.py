import unittest
from src.alert_worker.service import Rule, compare, evaluate


class AlertTest(unittest.TestCase):
    def test_comparators(self):
        self.assertTrue(compare(10, "gte", 10))
        self.assertTrue(compare(-12, "abs_gte", 10))
        self.assertFalse(compare(9, "gt", 10))

    def test_market_rule(self):
        rule = Rule(id="large", enabled=True, source="market", venue="binance", symbol="BTCUSDT", event_type="trade", field="quantity", operator="gte", value=5.0)
        alert = evaluate(rule, "market.cleaned", {"event_id":"e1","venue":"binance","symbol":"BTCUSDT","event_type":"trade","quantity":6,"event_time_ms":123})
        self.assertIsNotNone(alert)
        self.assertEqual(alert["rule_id"], "large")

    def test_microstructure_rule_does_not_match_market_topic(self):
        rule = Rule(id="ofi", enabled=True, source="microstructure", field="ofi", operator="abs_gte", value=20)
        self.assertIsNone(evaluate(rule, "market.cleaned", {"ofi":30}))
