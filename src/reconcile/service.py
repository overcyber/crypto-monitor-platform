from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.common.clickhouse import insert as ch_insert, rows as ch_rows
from src.common.config import settings
from src.common.iceberg import raw_table
from src.common.kafka import create_producer

STAGE = Path(os.getenv("RECONCILE_STAGE_DB", "/opt/app/data/reconcile/stage.sqlite"))


def _stage_raw(start_ms: int, end_ms: int) -> tuple[dict[tuple[str,str,str], dict[str,float]], int]:
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan
    STAGE.parent.mkdir(parents=True, exist_ok=True)
    if STAGE.exists(): STAGE.unlink()
    db=sqlite3.connect(STAGE)
    try:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=NORMAL")
        db.execute("CREATE TABLE raw(event_id TEXT, venue TEXT, symbol TEXT, event_type TEXT, price REAL, quantity REAL, ingest_time_ms INTEGER)")
        db.execute("CREATE INDEX idx_raw_key ON raw(venue,symbol,event_type)")
        db.execute("CREATE INDEX idx_raw_event ON raw(event_id)")
        table=raw_table()
        filt=And(GreaterThanOrEqual("ingest_time_ms", start_ms), LessThan("ingest_time_ms", end_ms))
        scan=table.scan(row_filter=filt, selected_fields=("event_id","venue","symbol","event_type","price","quantity","event_time_ms","ingest_time_ms"))
        total=0
        for batch in scan.to_arrow_batch_reader(dictionary_columns=("venue","symbol","event_type")):
            vals=[]
            for r in batch.to_pylist():
                vals.append((r.get("event_id"),r.get("venue"),r.get("symbol"),r.get("event_type"),r.get("price"),r.get("quantity"),r.get("ingest_time_ms")))
            db.executemany("INSERT INTO raw VALUES(?,?,?,?,?,?,?)",vals); total+=len(vals); db.commit()
        raw_rows={tuple(r[:3]):int(r[3]) for r in db.execute("SELECT venue,symbol,event_type,count(*) FROM raw GROUP BY venue,symbol,event_type")}
        agg={}
        q="""
        WITH dedup AS (
          SELECT event_id, max(venue) venue, max(symbol) symbol, max(event_type) event_type,
                 max(price) price, max(quantity) quantity
          FROM raw GROUP BY event_id
        )
        SELECT venue,symbol,event_type,count(*) unique_events,
               sum(coalesce(quantity,0.0)) quantity,
               sum(coalesce(price,0.0)*coalesce(quantity,0.0)) notional
        FROM dedup GROUP BY venue,symbol,event_type
        """
        for venue,symbol,event_type,unique_events,qty,notional in db.execute(q):
            key=(venue,symbol,event_type);agg[key]={"rows":raw_rows.get(key,0),"unique":int(unique_events),"quantity":float(qty or 0),"notional":float(notional or 0)}
        return agg,total
    finally:
        db.close()


def _hot(start_ms:int,end_ms:int)->dict[tuple[str,str,str],dict[str,float]]:
    q="""
    SELECT venue,symbol,event_type,count() rows,uniqExact(event_id) unique_events,
           sum(ifNull(quantity,0.0)) quantity,
           sum(ifNull(price,0.0)*ifNull(quantity,0.0)) notional
    FROM market.events_hot FINAL
    WHERE ingest_time_ms >= {start:Int64} AND ingest_time_ms < {end:Int64}
    GROUP BY venue,symbol,event_type
    """
    out={}
    for r in ch_rows(q,{"start":start_ms,"end":end_ms}):
        out[(r["venue"],r["symbol"],r["event_type"]) ]={"rows":int(r["rows"]),"unique":int(r["unique_events"]),"quantity":float(r["quantity"] or 0),"notional":float(r["notional"] or 0)}
    return out


def reconcile_once(start_ms:int,end_ms:int)->dict[str,Any]:
    s=settings(); raw,total=_stage_raw(start_ms,end_ms); hot=_hot(start_ms,end_ms); run_id=str(uuid.uuid4()); now=datetime.now(timezone.utc)
    keys=sorted(set(raw)|set(hot)); result=[]
    for key in keys:
        rr=raw.get(key,{"rows":0,"unique":0,"quantity":0.,"notional":0.}); hh=hot.get(key,{"rows":0,"unique":0,"quantity":0.,"notional":0.})
        count_delta=abs(rr["unique"]-hh["unique"])
        quantity_pct=(abs(rr["quantity"]-hh["quantity"])/max(abs(rr["quantity"]),1e-12)) if rr["quantity"] else (0. if hh["quantity"]==0 else float("inf"))
        notional_pct=(abs(rr["notional"]-hh["notional"])/max(abs(rr["notional"]),1e-12)) if rr["notional"] else (0. if hh["notional"]==0 else float("inf"))
        status="ok" if (count_delta<=s.reconcile_tolerance_count and quantity_pct<=s.reconcile_tolerance_volume_pct and notional_pct<=s.reconcile_tolerance_notional_pct) else "mismatch"
        result.append({"key":key,"raw":rr,"hot":hh,"count_delta":count_delta,"quantity_delta_pct":quantity_pct,"notional_delta_pct":notional_pct,"status":status})
    columns=["run_id","window_start","window_end","venue","symbol","event_type","raw_rows","raw_unique_events","raw_quantity","raw_notional","hot_rows","hot_unique_events","hot_quantity","hot_notional","status","details_json","created_at"]
    data=[]
    ws=datetime.fromtimestamp(start_ms/1000,timezone.utc);we=datetime.fromtimestamp(end_ms/1000,timezone.utc)
    for x in result:
        v,sym,typ=x["key"];rr=x["raw"];hh=x["hot"]
        data.append([run_id,ws,we,v,sym,typ,rr["rows"],rr["unique"],rr["quantity"],rr["notional"],hh["rows"],hh["unique"],hh["quantity"],hh["notional"],x["status"],json.dumps({"count_delta":x["count_delta"],"quantity_delta_pct":x["quantity_delta_pct"],"notional_delta_pct":x["notional_delta_pct"],"time_basis":"ingest_time_ms"}),now])
    # Always persist a run heartbeat so the UI can distinguish an empty window from a dead reconciler.
    summary_status = "ok" if result else "empty"
    data.append([run_id,ws,we,"system","*","reconcile_run",total,total,0.0,0.0,0,0,0.0,0.0,summary_status,json.dumps({"groups":len(result),"mismatches":sum(x["status"]!="ok" for x in result),"time_basis":"ingest_time_ms"}),now])
    ch_insert("market.reconciliation_results", data, column_names=columns)
    return {"run_id":run_id,"start_ms":start_ms,"end_ms":end_ms,"raw_scanned_rows":total,"groups":len(result),"mismatches":sum(x["status"]!="ok" for x in result),"results":result}


async def main()->None:
    s=settings();producer=await create_producer()
    try:
        while True:
            end_ms=int(time.time()*1000)-s.reconcile_settle_delay_seconds*1000
            start_ms=end_ms-s.reconcile_lookback_minutes*60_000
            try:
                report=await asyncio.to_thread(reconcile_once,start_ms,end_ms)
                payload=json.dumps(report,separators=(",",":"),default=str).encode()
                await producer.send_and_wait("market.reconcile.results",payload,key=report["run_id"].encode())
                print(f"reconcile run={report['run_id']} groups={report['groups']} mismatches={report['mismatches']}",flush=True)
            except Exception as exc:
                print(f"reconcile failed: {type(exc).__name__}: {exc}",flush=True)
            await asyncio.sleep(s.reconcile_interval_seconds)
    finally: await producer.stop()

if __name__=="__main__":asyncio.run(main())
