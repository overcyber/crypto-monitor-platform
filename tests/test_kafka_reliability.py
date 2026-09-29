import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

class KafkaReliabilityTest(unittest.TestCase):
    def test_derived_workers_commit_after_side_effects(self):
        kafka = (ROOT / "src/common/kafka.py").read_text()
        book = (ROOT / "src/book_worker/service.py").read_text()
        alerts = (ROOT / "src/alert_worker/service.py").read_text()
        self.assertIn("enable_auto_commit: bool = True", kafka)
        self.assertIn("enable_auto_commit=False", book)
        self.assertIn("await consumer.commit()", book)
        self.assertIn("enable_auto_commit=False", alerts)
        self.assertIn("await consumer.commit()", alerts)

    def test_book_worker_requests_snapshot_after_restart_or_gap(self):
        book = (ROOT / "src/book_worker/service.py").read_text()
        coinbase = (ROOT / "src/ingestors/coinbase.py").read_text()
        binance = (ROOT / "src/ingestors/binance.py").read_text()
        self.assertIn('reason == "awaiting_snapshot"', book)
        self.assertIn('market.control.resync', book)
        self.assertIn('coinbase-resync-v4', coinbase)
        self.assertIn('reconnect_event.set()', coinbase)
        self.assertIn('binance-resync-v4', binance)

if __name__ == "__main__":
    unittest.main()
