import unittest
from src.common.events import MarketEvent

class EventsTest(unittest.TestCase):
    def test_stable_id(self):
        a=MarketEvent.build(venue='X',channel='Trade',event_type='Trade',symbol='btcusd',source_id='42',event_time_ms=1,raw={'a':1})
        b=MarketEvent.build(venue='x',channel='trade',event_type='trade',symbol='BTCUSD',source_id='42',event_time_ms=999,raw={'a':2})
        self.assertEqual(a.event_id,b.event_id)
    def test_roundtrip(self):
        e=MarketEvent.build(venue='x',channel='c',event_type='trade',symbol='BTCUSD',source_id='1',event_time_ms=1,raw={},price=1.0)
        self.assertEqual(MarketEvent.model_validate_json(e.kafka_bytes()).model_dump(),e.model_dump())
