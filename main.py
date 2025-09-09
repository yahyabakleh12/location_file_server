# main.py
# JSON ingest → write body to file → store path in DB → strict FIFO forwarder
# (pass-through, delete-on-success, rotating file logs, SQLite lock hardening,
#  drop-missing-files, write-serialization & retry-on-locked, performance toggles)

import os
import json
import uuid
import asyncio
import contextlib
import datetime as dt
import logging
from logging.handlers import RotatingFileHandler
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import httpx
from sqlalchemy import (
    Column, Integer, String, Text, DateTime, select, func, update
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.exc import OperationalError

# -------------------------------------------------
# Configuration (env vars)
# -------------------------------------------------
UPSTREAM_URL = os.getenv("UPSTREAM_URL", "http://10.11.5.100:18007/post")
DB_URL = os.getenv("DB_URL", "sqlite+aiosqlite:///./queue.db")

QUEUE_DIR = Path(os.getenv("QUEUE_DIR", "./queue_files")).resolve()

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1"))
RETRY_BASE_SECONDS = int(os.getenv("RETRY_BASE_SECONDS", "5"))
RETRY_MAX_SECONDS  = int(os.getenv("RETRY_MAX_SECONDS", "300"))
FORWARD_LOOP_SLEEP_IDLE = float(os.getenv("FORWARD_LOOP_SLEEP_IDLE", "0.5"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "60"))

HEADERS_TO_COPY = (
    os.getenv("HEADERS_TO_COPY", "x-request-id,x-trace-id").lower().split(",")
    if os.getenv("HEADERS_TO_COPY") is not None else []
)

UPSTREAM_VERIFY = os.getenv("UPSTREAM_VERIFY", "true").lower()
if UPSTREAM_VERIFY in ("false", "0", "no"):
    HTTPX_VERIFY = False
else:
    HTTPX_VERIFY = UPSTREAM_VERIFY if os.path.exists(UPSTREAM_VERIFY) else True

UPSTREAM_HEADERS_JSON = os.getenv("UPSTREAM_HEADERS_JSON")
STATIC_UPSTREAM_HEADERS = {}
if UPSTREAM_HEADERS_JSON:
    try:
        STATIC_UPSTREAM_HEADERS = json.loads(UPSTREAM_HEADERS_JSON)
    except Exception:
        pass

DEFAULT_ACCEPT = os.getenv("UPSTREAM_ACCEPT", "application/json")

# ---------- Performance toggles ----------
ATOMIC_FSYNC = os.getenv("ATOMIC_FSYNC", "true").lower() not in ("0", "false", "no")
SQLITE_SYNC  = os.getenv("SQLITE_SYNC", "FULL").upper()  # FULL (safer) or NORMAL (faster)

# ------------- Logging config (file + console, rotating) -------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_DIR = Path(os.getenv("LOG_DIR", "./logs")).resolve()
LOG_FILENAME = os.getenv("LOG_FILENAME", "gateway.log")
LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024)))  # 10 MB
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))

def setup_logging():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger("gateway")
    log.setLevel(LOG_LEVEL)
    fmt = logging.Formatter("%(asctime)s %(levelname)s [%(process)d] %(name)s: %(message)s")
    file_path = LOG_DIR / LOG_FILENAME

    fh = RotatingFileHandler(file_path, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT, encoding="utf-8")
    fh.setFormatter(fmt); fh.setLevel(LOG_LEVEL)
    ch = logging.StreamHandler(); ch.setFormatter(fmt); ch.setLevel(LOG_LEVEL)

    log.handlers.clear(); log.addHandler(fh); log.addHandler(ch)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "httpx"):
        lg = logging.getLogger(name); lg.setLevel(LOG_LEVEL); lg.handlers.clear(); lg.addHandler(fh); lg.addHandler(ch)
    log.info("Logging initialized → file=%s, level=%s, rotate=%s bytes x %s",
             file_path, LOG_LEVEL, LOG_MAX_BYTES, LOG_BACKUP_COUNT)
    return log

log = setup_logging()
if UPSTREAM_HEADERS_JSON and not STATIC_UPSTREAM_HEADERS:
    log.warning("Failed to parse UPSTREAM_HEADERS_JSON; ignoring: %s", UPSTREAM_HEADERS_JSON)
log.info("Perf toggles: ATOMIC_FSYNC=%s, SQLITE_SYNC=%s", ATOMIC_FSYNC, SQLITE_SYNC)

# -------------------------------------------------
# Database
# -------------------------------------------------
Base = declarative_base()

class Message(Base):
    __tablename__ = "messages"
    id = Column(Integer, primary_key=True, autoincrement=True)
    message_uuid = Column(String(36), unique=True, nullable=False)
    content_type = Column(String(200), nullable=False, default="application/json")
    headers_json = Column(Text, nullable=False, default="{}")
    file_path = Column(Text, nullable=False)
    file_size = Column(Integer, nullable=False, default=0)
    received_at = Column(DateTime, nullable=False, default=func.now())
    status = Column(String(20), nullable=False, default="queued")   # queued|sending|sent|error
    attempts = Column(Integer, nullable=False, default=0)
    last_error = Column(Text, nullable=True)
    next_attempt_at = Column(DateTime, nullable=False, default=func.now())
    sent_at = Column(DateTime, nullable=True)

engine = create_async_engine(
    DB_URL,
    future=True,
    echo=False,
    connect_args={"timeout": 60},   # wait longer on file locks
)
Session = async_sessionmaker(engine, expire_on_commit=False)

# Single-writer serialization inside this process
DB_WRITE_LOCK = asyncio.Lock()

# -------------------------------------------------
# Helpers
# -------------------------------------------------
def now_utc() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=None)

def compute_next_backoff(attempts: int) -> int:
    delay = RETRY_BASE_SECONDS * (2 ** max(0, attempts - 1))
    return min(delay, RETRY_MAX_SECONDS)

def ensure_queue_dir():
    QUEUE_DIR.mkdir(parents=True, exist_ok=True)
    log.info("Queue directory: %s", str(QUEUE_DIR))

def atomic_write_bytes(target: Path, data: bytes):
    """Write bytes atomically: write to temp, optional fsync, then replace to final path."""
    tmp = target.with_suffix(target.suffix + f".tmp-{uuid.uuid4().hex}")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        if ATOMIC_FSYNC:
            os.fsync(f.fileno())
    os.replace(tmp, target)

async def enable_sqlite_durability():
    """SQLite: WAL + configurable synchronous + busy_timeout for durability & friendly locking."""
    if DB_URL.startswith("sqlite+aiosqlite"):
        async with engine.begin() as conn:
            await conn.exec_driver_sql("PRAGMA journal_mode=WAL;")
            await conn.exec_driver_sql(f"PRAGMA synchronous={SQLITE_SYNC};")
            await conn.exec_driver_sql("PRAGMA busy_timeout=60000;")  # 60s
            await conn.exec_driver_sql("PRAGMA wal_autocheckpoint=1000;")
        log.info("SQLite durability: WAL + synchronous=%s + busy_timeout=60s", SQLITE_SYNC)

async def recover_stuck_messages_on_startup():
    """Reset any 'sending' rows to 'queued' after an unclean shutdown (with retries if DB is locked)."""
    for attempt in range(10):
        try:
            async with Session() as session:
                async with DB_WRITE_LOCK:
                    await session.execute(
                        update(Message)
                        .where(Message.status == "sending")
                        .values(status="queued", next_attempt_at=now_utc(), last_error="Recovered after restart")
                    )
                    await session.commit()
            log.info("Recovery complete: 'sending' → 'queued'")
            return
        except OperationalError as e:
            if "locked" in str(e).lower():
                delay = 0.5 * (attempt + 1)
                log.warning("Recover retry %s: DB locked; sleeping %.1fs", attempt + 1, delay)
                await asyncio.sleep(delay); continue
            raise
    log.error("Startup recovery failed due to persistent DB lock")

# -------------------------------------------------
# Forwarder (fresh session per cycle; DB writes serialized)
# -------------------------------------------------
async def forward_once(client: httpx.AsyncClient) -> bool:
    # 1) select and mark as 'sending' (inside write lock)
    async with Session() as session:
        msgs = []
        try:
            async with DB_WRITE_LOCK:
                res = await session.execute(
                    select(Message)
                    .where(Message.status.in_(("queued", "error")), Message.next_attempt_at <= now_utc())
                    .order_by(Message.id.asc())
                    .limit(BATCH_SIZE)
                )
                msgs = res.scalars().all()
                if not msgs:
                    await session.rollback()
                    return False
                for m in msgs:
                    m.status = "sending"
                await session.commit()
        except OperationalError as e:
            if "locked" in str(e).lower():
                log.debug("forward_once: locked during select/mark; will idle")
                return False
            raise

    # 2) send each message (network outside lock; DB updates inside short lock)
    for msg in msgs:
        headers = {
            "Content-Type": msg.content_type or "application/json",
            "Accept": DEFAULT_ACCEPT,
            "Idempotency-Key": msg.message_uuid,
        }
        headers.update(STATIC_UPSTREAM_HEADERS)
        try:
            inbound = json.loads(msg.headers_json or "{}")
            headers.update(inbound)
        except Exception:
            pass

        file_path = Path(msg.file_path)

        # read payload file before taking DB lock
        if not file_path.exists():
            # drop non-existent file rows
            async with Session() as session:
                async with DB_WRITE_LOCK:
                    try:
                        dbmsg = await session.get(Message, msg.id)
                        if dbmsg:
                            await session.delete(dbmsg); await session.commit()
                        log.error("File missing for id=%s: %s — dropped row", msg.id, file_path)
                    except OperationalError as e:
                        if "locked" in str(e).lower():
                            await asyncio.sleep(0.2)
                        else:
                            raise
            continue

        body = file_path.read_bytes()

        try:
            resp = await client.post(
                UPSTREAM_URL,
                content=body,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
        except (httpx.RequestError, httpx.HTTPError) as e:
            # mark error with backoff
            async with Session() as session:
                async with DB_WRITE_LOCK:
                    try:
                        dbmsg = await session.get(Message, msg.id)
                        if dbmsg:
                            dbmsg.status = "error"
                            dbmsg.attempts += 1
                            dbmsg.last_error = f"Network error: {repr(e)}"
                            dbmsg.next_attempt_at = now_utc() + dt.timedelta(seconds=compute_next_backoff(dbmsg.attempts))
                            await session.commit()
                        log.error("Network error for id=%s: %r", msg.id, e)
                    except OperationalError as oe:
                        if "locked" in str(oe).lower():
                            await asyncio.sleep(0.2)
                        else:
                            raise
            continue

        if 200 <= resp.status_code < 300:
            # success → delete file then delete row (DB op under lock)
            try:
                file_path.unlink(missing_ok=True)
            except Exception as fe:
                log.warning("Sent id=%s but file delete failed (%s): %r", msg.id, file_path, fe)
            async with Session() as session:
                async with DB_WRITE_LOCK:
                    try:
                        dbmsg = await session.get(Message, msg.id)
                        if dbmsg:
                            await session.delete(dbmsg); await session.commit()
                        log.info("Sent & deleted id=%s ✓", msg.id)
                    except OperationalError as oe:
                        if "locked" in str(oe).lower():
                            # fallback: mark sent if delete collides; retry next cycle if still there
                            await session.rollback()
                            if dbmsg:
                                dbmsg.status = "sent"; dbmsg.sent_at = now_utc()
                                dbmsg.last_error = "DB delete deferred due to lock"
                                await session.commit()
                            log.warning("Row delete deferred for id=%s due to lock", msg.id)
                        else:
                            raise
        else:
            # upstream error → mark error with backoff
            async with Session() as session:
                async with DB_WRITE_LOCK:
                    try:
                        dbmsg = await session.get(Message, msg.id)
                        if dbmsg:
                            dbmsg.status = "error"
                            dbmsg.attempts += 1
                            dbmsg.last_error = f"HTTP {resp.status_code}: {resp.text[:500]}"
                            dbmsg.next_attempt_at = now_utc() + dt.timedelta(seconds=compute_next_backoff(dbmsg.attempts))
                            await session.commit()
                        log.warning("Upstream HTTP %s for id=%s; will retry", resp.status_code, msg.id)
                    except OperationalError as oe:
                        if "locked" in str(oe).lower():
                            await asyncio.sleep(0.2)
                        else:
                            raise
    return True

async def forward_loop():
    async with httpx.AsyncClient(http2=True, timeout=REQUEST_TIMEOUT, verify=HTTPX_VERIFY) as client:
        log.info("Forwarder started → %s (verify=%s)", UPSTREAM_URL, HTTPX_VERIFY)
        while True:
            try:
                processed = await forward_once(client)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("forward_once failed")
                await asyncio.sleep(1)
                continue
            if not processed:
                await asyncio.sleep(FORWARD_LOOP_SLEEP_IDLE)

# -------------------------------------------------
# FastAPI app (lifespan)
# -------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_queue_dir()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await enable_sqlite_durability()
    await recover_stuck_messages_on_startup()

    task = asyncio.create_task(forward_loop())
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

app = FastAPI(
    title="JSON Ingest → File Store → Forward (lock-safe, perf-toggle)",
    version="1.8",
    lifespan=lifespan,
)

# -------------------------------------------------
# Schemas
# -------------------------------------------------
class IngestResponse(BaseModel):
    message_id: int
    message_uuid: str
    status: str
    file_path: str
    file_size: int

# -------------------------------------------------
# Endpoints
# -------------------------------------------------
@app.post("/ingest", response_model=IngestResponse)
async def ingest(req: Request):
    """
    Accept JSON **as-is** (raw body), write it to a file atomically, and enqueue.
    """
    body = await req.body()
    if not body:
        raise HTTPException(status_code=400, detail="Empty body")

    content_type = req.headers.get("content-type", "application/json")

    selected_headers = {}
    if HEADERS_TO_COPY:
        for k, v in req.headers.items():
            lk = k.lower()
            if lk in HEADERS_TO_COPY:
                selected_headers[lk] = v

    message_uuid = str(uuid.uuid4())
    filename = f"{message_uuid}.json"
    target_path = QUEUE_DIR / filename

    try:
        atomic_write_bytes(target_path, body)
    except Exception as e:
        log.error("Atomic write failed for %s: %r", target_path, e)
        raise HTTPException(status_code=500, detail="Failed to persist payload")

    file_size = target_path.stat().st_size

    # INSERT with retries if database is locked
    for attempt in range(8):
        try:
            async with Session() as session:
                async with DB_WRITE_LOCK:
                    msg = Message(
                        message_uuid=message_uuid,
                        content_type=content_type,
                        headers_json=json.dumps(selected_headers),
                        file_path=str(target_path),
                        file_size=file_size,
                        status="queued",
                        attempts=0,
                        next_attempt_at=now_utc(),
                    )
                    session.add(msg)
                    await session.commit()
                    await session.refresh(msg)
                    log.info("Ingested id=%s file=%s size=%s", msg.id, target_path.name, file_size)
                    return IngestResponse(
                        message_id=msg.id,
                        message_uuid=msg.message_uuid,
                        status=msg.status,
                        file_path=msg.file_path,
                        file_size=file_size,
                    )
        except OperationalError as e:
            if "locked" in str(e).lower():
                backoff = min(0.25 * (attempt + 1), 2.0)
                log.warning("INSERT retry %s (DB locked); sleeping %.2fs", attempt + 1, backoff)
                await asyncio.sleep(backoff)
                continue
            raise

    # If we couldn't insert after retries, remove the file to avoid orphans
    try:
        Path(target_path).unlink(missing_ok=True)
    except Exception:
        pass
    raise HTTPException(status_code=503, detail="Database busy, try again")

@app.get("/stats")
async def stats():
    async with Session() as session:
        row = (await session.execute(
            select(
                func.count().filter(Message.status == "queued"),
                func.count().filter(Message.status == "sending"),
                func.count().filter(Message.status == "error"),
                func.count().filter(Message.status == "sent"),
            )
        )).one()
        return {
            "queued": row[0],
            "sending": row[1],
            "error": row[2],
            "sent": row[3],
            "upstream_url": UPSTREAM_URL,
            "delete_on_success": True,
            "pass_through": True,
            "queue_dir": str(QUEUE_DIR),
            "log_file": str((LOG_DIR / LOG_FILENAME)),
            "atomic_fsync": ATOMIC_FSYNC,
            "sqlite_sync": SQLITE_SYNC,
        }

@app.get("/healthz")
async def healthz():
    return JSONResponse({"ok": True})

@app.post("/debug/forward-once")
async def debug_forward_once():
    async with httpx.AsyncClient(http2=True, timeout=REQUEST_TIMEOUT, verify=HTTPX_VERIFY) as client:
        ok = await forward_once(client)
        return {"attempted": ok}
