import unittest
from src.monitor_api.main import app as monitor
from src.analytics_api.main import app as analytics

class ApiSeparationTest(unittest.TestCase):
    def test_monitor_has_no_analytics(self):
        paths={r.path for r in monitor.routes};self.assertIn('/v1/quote/{symbol}',paths);self.assertIn('/v1/alerts',paths);self.assertNotIn('/v1/analyze/{symbol}',paths);self.assertNotIn('/v1/regime/{symbol}',paths)
    def test_analytics_has_no_monitor_quote(self):
        paths={r.path for r in analytics.routes};self.assertIn('/v1/analyze/{symbol}',paths);self.assertNotIn('/v1/quote/{symbol}',paths);self.assertNotIn('/v1/stream/{symbol}',paths)
