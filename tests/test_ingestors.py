import unittest
from src.ingestors.binance import normalize as bn
from src.ingestors.coinbase import normalize as cb
from src.ingestors.kraken import normalize as kr

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
    def test_kraken_ticker(self):
        events = kr({'channel': 'ticker', 'type': 'snapshot', 'data': [{'symbol': 'XMR/USD', 'bid': 540.0, 'bid_qty': 2.0, 'ask': 541.0, 'ask_qty': 1.5, 'last': 540.5, 'timestamp': '2026-10-01T12:00:00Z'}]})
        self.assertEqual(len(events), 1)
        e = events[0]
        self.assertEqual(e.venue, 'kraken')
        self.assertEqual(e.symbol, 'XMR/USD')
        self.assertEqual(e.event_type, 'ticker')
        self.assertEqual(e.price, 540.5)
        self.assertEqual(e.bid_price, 540.0)
        self.assertEqual(e.ask_price, 541.0)
    def test_kraken_trade(self):
        events = kr({'channel': 'trade', 'type': 'update', 'data': [{'symbol': 'XMR/USD', 'side': 'buy', 'price': 541.2, 'qty': 0.5, 'trade_id': 12345, 'timestamp': '2026-10-01T12:00:01Z'}]})
        self.assertEqual(len(events), 1)
        e = events[0]
        self.assertEqual(e.venue, 'kraken')
        self.assertEqual(e.symbol, 'XMR/USD')
        self.assertEqual(e.event_type, 'trade')
        self.assertEqual(e.price, 541.2)
        self.assertEqual(e.quantity, 0.5)
        self.assertEqual(e.side, 'buy')
        self.assertEqual(e.sequence_id, 12345)
    def test_kraken_book(self):
        events = kr({'channel': 'book', 'type': 'snapshot', 'data': [{'symbol': 'XMR/USD', 'bids': [{'price': 540.0, 'qty': 1.0}], 'asks': [{'price': 541.0, 'qty': 2.0}], 'checksum': 999}]})
        self.assertEqual(len(events), 1)
        e = events[0]
        self.assertEqual(e.venue, 'kraken')
        self.assertEqual(e.symbol, 'XMR/USD')
        self.assertEqual(e.event_type, 'book_snapshot')
        self.assertIn('540.0', e.bids_json)
        self.assertIn('541.0', e.asks_json)
