import unittest
from src.book_worker.book_manager import BookState
from src.common.events import MarketEvent

def ev(kind,source,seq=None,first=None,bids='[]',asks='[]',venue='binance'):
    return MarketEvent.build(venue=venue,channel='depth',event_type=kind,symbol='BTCUSDT',source_id=source,event_time_ms=seq or 1,raw={},sequence_id=seq,first_sequence_id=first,bids_json=bids,asks_json=asks)

class BookTest(unittest.TestCase):
    def test_snapshot_delta_delete(self):
        b=BookState('binance','BTCUSDT');b.handle(ev('book_snapshot','s',10,bids='[["99","2"]]',asks='[["101","3"]]'))
        f,r=b.handle(ev('book_delta','d',11,11,bids='[["100","4"],["99","0"]]'))
        self.assertIsNone(r);self.assertNotIn(99.0,b.bids);self.assertEqual(b.last_sequence,11);self.assertIsNotNone(f)
    def test_gap_requests_resync(self):
        b=BookState('binance','BTCUSDT');b.handle(ev('book_snapshot','s',10,bids='[["99","2"]]',asks='[["101","3"]]'))
        f,r=b.handle(ev('book_delta','d',14,14,bids='[["100","1"]]'))
        self.assertIsNone(f);self.assertTrue(r.startswith('sequence_gap'));self.assertFalse(b.ready)
    def test_overlap_sequence_is_valid(self):
        b=BookState('binance','BTCUSDT');b.handle(ev('book_snapshot','s',10,bids='[["99","2"]]',asks='[["101","3"]]'))
        f,r=b.handle(ev('book_delta','d',12,9,bids='[["100","1"]]'))
        self.assertIsNone(r);self.assertEqual(b.last_sequence,12)
    def test_microprice_pressure(self):
        b=BookState('coinbase','BTC-USD');f,_=b.handle(ev('book_snapshot','s',None,bids='[["99","10"]]',asks='[["101","1"]]',venue='coinbase'))
        self.assertGreater(f.microprice,100)
    def test_ofi_is_sequential(self):
        b=BookState('coinbase','BTC-USD');b.handle(ev('book_snapshot','s',None,bids='[["99","2"]]',asks='[["101","2"]]',venue='coinbase'))
        f,_=b.handle(ev('book_delta','d',None,bids='[["99","4"]]',venue='coinbase'))
        self.assertGreater(f.ofi,0)

    def test_duplicate_event_is_idempotent(self):
        b=BookState('coinbase','BTC-USD',dedup_events=100)
        snap=ev('book_snapshot','s',None,bids='[["99","2"]]',asks='[["101","2"]]',venue='coinbase')
        b.handle(snap)
        trade=MarketEvent.build(venue='coinbase',channel='matches',event_type='trade',symbol='BTC-USD',source_id='trade-1',event_time_ms=1000,raw={},price=100,quantity=3,side='buy')
        b.handle(trade); b.handle(trade)
        self.assertEqual(sum(v for _,v in b.trades),3.0)
