import unittest, math
from src.analytics_api.models import Candle
from src.analytics_api.indicators import *
from src.analytics_api.regime import analyze_regime

class AnalyticsTest(unittest.TestCase):
    def setUp(self):
        self.cs=[Candle(i*3600000,100+i,102+i,99+i,101+i,1000+i) for i in range(100)]
    def test_indicators(self):
        c=[x.close for x in self.cs];self.assertGreater(rsi_wilder(c),50);self.assertIsNotNone(macd(c)['histogram']);self.assertIsNotNone(bollinger(c)['upper']);self.assertGreater(atr_wilder(self.cs),0)
    def test_volatility_estimators(self):
        v=volatility_suite(self.cs);self.assertEqual(set(v),{'realized_pct_annualized','ewma_pct_annualized','parkinson_pct_annualized','garman_klass_pct_annualized','rogers_satchell_pct_annualized'});self.assertTrue(all(x is not None for x in v.values()))
    def test_correlation_returns(self):
        b=[Candle(x.open_time_ms,x.open*2,x.high*2,x.low*2,x.close*2,x.volume) for x in self.cs]
        self.assertAlmostEqual(correlation(self.cs,b)['pearson'],1.0,places=9)
    def test_regime_descriptive(self):
        r=analyze_regime(self.cs,'1h');self.assertIn('observed',r);self.assertIn('change_points',r)

class RegimeCoverageTest(unittest.TestCase):
    def test_insufficient_regime_reports_coverage(self):
        cs=[Candle(i*60000,100,101,99,100.5,10) for i in range(4)]
        r=analyze_regime(cs,'1h')['observed']
        self.assertEqual(r['regime'],'insufficient_data')
        self.assertEqual(r['available_candles'],4)
        self.assertEqual(r['required_candles'],60)
        self.assertEqual(r['missing_candles'],56)
