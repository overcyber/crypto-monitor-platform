import unittest
from src.ingestors.binance import normalize as bn
from src.ingestors.coinbase import normalize as cb

class IngestorTest(unittest.TestCase):
    def test_binance_depth(self):
        e=bn('BTCUSDT','x',{'e':'depthUpdate','E':100,'s':'BTCUSDT','U':10,'u':11,'b':[['1','2']],'a':[['2','3']]})
        self.assertEqual(e.event_type,'book_delta');self.assertEqual(e.first_sequence_id,10);self.assertEqual(e.sequence_id,11)
    def test_binance_trade_aggressor(self):
        e=bn('BTCUSDT','x',{'e':'trade','E':100,'T':101,'s':'BTCUSDT','t':3,'p':'10','q':'2','m':True})
        self.assertEqual(e.side,'sell');self.assertEqual(e.price,10)
    def test_binance_book_ticker(self):
        e=bn('BTCUSDT','x',{'u':4,'s':'BTCUSDT','b':'9','B':'1','a':'10','A':'2'})
        self.assertEqual(e.event_type,'book_ticker')
    def test_coinbase_l2(self):
        e=cb({'type':'l2update','product_id':'BTC-USD','time':'2026-01-01T00:00:00Z','changes':[['buy','10','2'],['sell','11','3']]})
        self.assertEqual(e.event_type,'book_delta');self.assertIn('10',e.bids_json);self.assertIn('11',e.asks_json)
    def test_coinbase_trade_inverts_maker_side(self):
        e=cb({'type':'match','product_id':'BTC-USD','time':'2026-01-01T00:00:00Z','trade_id':7,'price':'10','size':'1','side':'sell'})
        self.assertEqual(e.side,'buy')
