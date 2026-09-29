from __future__ import annotations
import argparse
import asyncio
import json
from src.common.events import MarketEvent
from src.book_worker.book_manager import BookState
from src.analytics_api.models import Candle
from src.analytics_api.indicators import rsi_wilder,macd,bollinger,atr_wilder,volatility_suite


def offline()->dict:
    checks=[]
    e=MarketEvent.build(venue="x",channel="trade",event_type="trade",symbol="BTCUSD",source_id="1",event_time_ms=1,raw={"x":1},price=100,quantity=1,side="buy")
    checks.append(("event_roundtrip",MarketEvent.model_validate_json(e.kafka_bytes()).event_id==e.event_id))
    book=BookState("binance","BTCUSDT")
    snap=MarketEvent.build(venue="binance",channel="depth",event_type="book_snapshot",symbol="BTCUSDT",source_id="s:10",event_time_ms=10,raw={},sequence_id=10,bids_json='[["99","2"]]',asks_json='[["101","3"]]')
    feature,reason=book.handle(snap);checks.append(("book_snapshot",feature is not None and reason is None))
    delta=MarketEvent.build(venue="binance",channel="depth",event_type="book_delta",symbol="BTCUSDT",source_id="11:11",event_time_ms=11,raw={},first_sequence_id=11,sequence_id=11,bids_json='[["100","4"]]',asks_json='[]')
    feature,reason=book.handle(delta);checks.append(("book_delta_sequence",feature is not None and reason is None and book.last_sequence==11))
    candles=[Candle(i*3600000,100+i,102+i,99+i,101+i,float(1000+i)) for i in range(80)]; closes=[c.close for c in candles]
    checks += [("rsi",rsi_wilder(closes) is not None),("macd",macd(closes)["macd"] is not None),("bollinger",bollinger(closes)["middle"] is not None),("atr",atr_wilder(candles) is not None),("volatility",volatility_suite(candles)["realized_pct_annualized"] is not None)]
    return {"ok":all(v for _,v in checks),"checks":[{"name":n,"ok":v} for n,v in checks]}

async def live()->dict:
    import httpx
    out={}
    async with httpx.AsyncClient(timeout=5) as c:
        for name,url in {"monitor":"http://monitor-api:8081/health","analytics":"http://analytics-api:8082/health","replay":"http://replay-api:8084/health","flink":"http://flink-jobmanager:8081/overview","lakekeeper":"http://lakekeeper:8181/health","clickhouse":"http://clickhouse:8123/ping"}.items():
            try:r=await c.get(url);out[name]={"ok":r.is_success,"status":r.status_code,"body":r.text[:300]}
            except Exception as exc:out[name]={"ok":False,"error":str(exc)}
    return {"ok":all(v["ok"] for v in out.values()),"services":out}

def main():
    ap=argparse.ArgumentParser();ap.add_argument("--live",action="store_true");args=ap.parse_args();result=offline();
    if args.live:result["live"]=asyncio.run(live());result["ok"]=result["ok"] and result["live"]["ok"]
    print(json.dumps(result,indent=2));raise SystemExit(0 if result["ok"] else 1)

if __name__=="__main__":main()
