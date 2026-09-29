from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from src.common.config import settings
from src.common.events import MarketEvent
from src.common.iceberg import raw_table
from src.common.kafka import create_producer

app = FastAPI(title="Deterministic Replay API", version="4.1.1")
ROOT = Path(os.getenv("REPLAY_STAGE_DIR", "/opt/app/data/replay"))
ROOT.mkdir(parents=True, exist_ok=True)
JOBS_DB = ROOT / "jobs.sqlite"
JOBS: dict[str, dict[str, Any]] = {}


class ReplayRequest(BaseModel):
    start_ms: int
    end_ms: int
    venue: str | None = None
    symbol: str | None = None
    speed: float = Field(default=0.0, ge=0.0, le=10000.0)
    max_events: int | None = None


def _db() -> sqlite3.Connection:
    db = sqlite3.connect(JOBS_DB)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS replay_jobs(
            job_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            updated_at_ms INTEGER NOT NULL
        )
        """
    )
    return db


def _persist(job: dict[str, Any]) -> None:
    with _db() as db:
        db.execute(
            "INSERT OR REPLACE INTO replay_jobs(job_id,status,payload_json,updated_at_ms) VALUES(?,?,?,?)",
            (job["job_id"], job["status"], json.dumps(job, separators=(",", ":"), default=str), int(time.time() * 1000)),
        )


def _load_job(job_id: str) -> dict[str, Any] | None:
    if job_id in JOBS:
        return JOBS[job_id]
    with _db() as db:
        row = db.execute("SELECT payload_json FROM replay_jobs WHERE job_id=?", (job_id,)).fetchone()
    return json.loads(row[0]) if row else None


def _mark_interrupted_jobs() -> None:
    with _db() as db:
        rows = db.execute("SELECT job_id,payload_json FROM replay_jobs WHERE status IN ('queued','materializing','replaying')").fetchall()
        for job_id, payload in rows:
            job = json.loads(payload)
            job["status"] = "interrupted"
            job["error"] = "replay-api restarted before the job completed"
            job["completed_at_ms"] = int(time.time() * 1000)
            db.execute(
                "UPDATE replay_jobs SET status=?,payload_json=?,updated_at_ms=? WHERE job_id=?",
                (job["status"], json.dumps(job, separators=(",", ":")), int(time.time() * 1000), job_id),
            )


def _materialize(job_id: str, req: ReplayRequest) -> tuple[Path, int]:
    from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan

    path = ROOT / f"{job_id}.sqlite"
    db = sqlite3.connect(path)
    fields = (
        "schema_version", "event_id", "source_id", "venue", "channel", "event_type", "symbol",
        "sequence_id", "first_sequence_id", "event_time_ms", "ingest_time_ms", "price", "quantity",
        "side", "bid_price", "bid_quantity", "ask_price", "ask_quantity", "bids_json", "asks_json",
        "raw_payload", "metadata_json",
    )
    db.execute(
        "CREATE TABLE events(seq INTEGER PRIMARY KEY AUTOINCREMENT,event_time_ms INTEGER,ingest_time_ms INTEGER,event_id TEXT,payload BLOB)"
    )
    filt = And(GreaterThanOrEqual("event_time_ms", req.start_ms), LessThan("event_time_ms", req.end_ms))
    if req.venue:
        filt = And(filt, EqualTo("venue", req.venue.lower()))
    if req.symbol:
        filt = And(filt, EqualTo("symbol", req.symbol.upper()))
    maximum = min(req.max_events or settings().replay_max_events, settings().replay_max_events)
    count = 0
    scan = raw_table().scan(row_filter=filt, selected_fields=fields)
    for batch in scan.to_arrow_batch_reader(dictionary_columns=("venue", "channel", "event_type", "symbol")):
        vals = []
        for row in batch.to_pylist():
            if count >= maximum:
                break
            event = MarketEvent.model_validate(row)
            # Replay events preserve event_id/source identity. The job id is carried only as metadata.
            try:
                meta = json.loads(event.metadata_json or "{}")
            except Exception:
                meta = {}
            meta["replay_job_id"] = job_id
            event = event.model_copy(update={"metadata_json": json.dumps(meta, separators=(",", ":"))})
            vals.append((event.event_time_ms, event.ingest_time_ms, event.event_id, event.kafka_bytes()))
            count += 1
        db.executemany("INSERT INTO events(event_time_ms,ingest_time_ms,event_id,payload) VALUES(?,?,?,?)", vals)
        db.commit()
        if count >= maximum:
            break
    db.execute("CREATE INDEX idx_replay_order ON events(event_time_ms,ingest_time_ms,event_id,seq)")
    db.commit()
    db.close()
    return path, count


async def _run(job_id: str, req: ReplayRequest) -> None:
    producer = None
    path: Path | None = None
    job = JOBS[job_id]
    try:
        job.update(status="materializing")
        _persist(job)
        path, count = await asyncio.to_thread(_materialize, job_id, req)
        job.update(status="replaying", events=count, sent=0)
        _persist(job)
        producer = await create_producer()
        db = sqlite3.connect(path)
        prev = None
        sent = 0
        try:
            cur = db.execute("SELECT event_time_ms,payload FROM events ORDER BY event_time_ms,ingest_time_ms,event_id,seq")
            while True:
                batch = cur.fetchmany(settings().replay_batch_size)
                if not batch:
                    break
                for event_ms, payload in batch:
                    if req.speed > 0 and prev is not None and event_ms > prev:
                        await asyncio.sleep((event_ms - prev) / 1000 / req.speed)
                    event = MarketEvent.model_validate_json(payload)
                    await producer.send_and_wait("market.replay", payload, key=event.kafka_key())
                    prev = event_ms
                    sent += 1
                job["sent"] = sent
                _persist(job)
        finally:
            db.close()
        job.update(status="completed", completed_at_ms=int(time.time() * 1000))
        _persist(job)
    except Exception as exc:
        job.update(status="failed", error=f"{type(exc).__name__}: {exc}", completed_at_ms=int(time.time() * 1000))
        _persist(job)
    finally:
        if producer:
            await producer.stop()
        if path:
            try:
                path.unlink()
            except OSError:
                pass


@app.on_event("startup")
def startup() -> None:
    _mark_interrupted_jobs()


@app.get("/health")
async def health():
    return {"status": "ok", "service": "replay-api", "jobs_db": str(JOBS_DB)}


@app.post("/v1/replay", status_code=202)
async def start_replay(req: ReplayRequest):
    if req.end_ms <= req.start_ms:
        raise HTTPException(400, "end_ms must be greater than start_ms")
    job_id = str(uuid.uuid4())
    job = {
        "job_id": job_id,
        "status": "queued",
        "request": req.model_dump(),
        "created_at_ms": int(time.time() * 1000),
    }
    JOBS[job_id] = job
    _persist(job)
    asyncio.create_task(_run(job_id, req))
    return job


@app.get("/v1/replay/{job_id}")
async def replay_status(job_id: str):
    job = _load_job(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    return job
