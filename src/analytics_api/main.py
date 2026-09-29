from __future__ import annotations

import asyncio
from fastapi import FastAPI, HTTPException, Query

from src.common.clickhouse import rows
from src.common.config import settings
from .data import candles, latest_microstructure
from .indicators import atr_wilder, bollinger, correlation, macd, rsi_wilder, volatility_suite, volume_anomaly
from .regime import analyze_regime

app=FastAPI(title="Crypto Analytics API",version="4.3.1",description="Analytics-only endpoint. Monitoring routes intentionally live on :8081.")

async def _candles(symbol:str,venue:str,interval:str,limit:int):
    try:return await asyncio.to_thread(candles,symbol,venue,interval,limit)
    except ValueError as exc:raise HTTPException(400,str(exc)) from exc
    except Exception as exc:raise HTTPException(503,str(exc)) from exc

@app.get("/health")
async def health():
    try:r=await asyncio.to_thread(rows,"SELECT 1 AS ok");return {"status":"ok","service":"analytics-api","clickhouse":bool(r)}
    except Exception as exc:return {"status":"degraded","service":"analytics-api","clickhouse":False,"error":str(exc)}

@app.get("/v1/indicators/{symbol}")
async def indicators(symbol:str,venue:str="binance",interval:str="1h",bars:int=Query(300,ge=60,le=5000)):
    cs=await _candles(symbol,venue,interval,bars);closes=[c.close for c in cs]
    if len(cs)<30:raise HTTPException(422,"insufficient trade history")
    atr=atr_wilder(cs)
    return {"symbol":symbol.upper(),"venue":venue,"interval":interval,"bars":len(cs),"rsi14":rsi_wilder(closes),"macd":macd(closes),"bollinger":bollinger(closes),"atr14":atr,"atr14_pct":atr/cs[-1].close*100 if atr is not None and cs[-1].close else None,"volume_anomaly":volume_anomaly([c.volume for c in cs])}

@app.get("/v1/volatility/{symbol}")
async def volatility(symbol:str,venue:str="binance",interval:str="1h",bars:int=Query(300,ge=60,le=5000)):
    cs=await _candles(symbol,venue,interval,bars)
    return {"symbol":symbol.upper(),"venue":venue,"interval":interval,"bars":len(cs),"volatility":volatility_suite(cs,interval)}

@app.get("/v1/regime/{symbol}")
async def regime(symbol:str,venue:str="binance",interval:str="1h",bars:int=Query(300,ge=60,le=5000)):
    cs=await _candles(symbol,venue,interval,bars)
    return {"symbol":symbol.upper(),"venue":venue,"interval":interval,"bars":len(cs),**analyze_regime(cs,interval)}

@app.get("/v1/microstructure/{symbol}")
async def microstructure(symbol:str,venue:str="binance"):
    try:r=await asyncio.to_thread(latest_microstructure,symbol,venue)
    except Exception as exc:raise HTTPException(503,str(exc)) from exc
    if not r:raise HTTPException(404,"microstructure not available")
    return r

@app.get("/v1/correlation")
async def correlations(benchmark:str="BTCUSDT",symbols:str="ETHUSDT,SOLUSDT",venue:str="binance",interval:str="1h",bars:int=Query(300,ge=60,le=2000)):
    base=await _candles(benchmark,venue,interval,bars);out={}
    for sym in [x.strip().upper() for x in symbols.split(',') if x.strip()]:
        other=await _candles(sym,venue,interval,bars);out[sym]=correlation(base,other)
    return {"benchmark":benchmark.upper(),"venue":venue,"interval":interval,"correlations":out}

@app.get("/v1/analyze/{symbol}")
async def analyze(symbol:str,venue:str="binance",interval:str="1h",bars:int=Query(300,ge=60,le=5000)):
    cs=await _candles(symbol,venue,interval,bars);closes=[c.close for c in cs];atr=atr_wilder(cs)
    try:micro=await asyncio.to_thread(latest_microstructure,symbol,venue)
    except Exception:micro=None
    return {"symbol":symbol.upper(),"venue":venue,"interval":interval,"bars":len(cs),"technical":{"rsi14":rsi_wilder(closes),"macd":macd(closes),"bollinger":bollinger(closes),"atr14":atr,"atr14_pct":atr/cs[-1].close*100 if cs and atr is not None and cs[-1].close else None,"volume_anomaly":volume_anomaly([c.volume for c in cs])},"volatility":volatility_suite(cs,interval),"regime":analyze_regime(cs,interval),"microstructure":micro,"note":"Descriptive analytics only; no order execution or price prediction."}
