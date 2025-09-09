# main.py
# JSON ingest → write body to file → store path in DB → strict FIFO forwarder
# (pass-through, delete-on-success, rotating file logs, SQLite lock hardening, drop-missing-files)

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
UPSTREAM_URL = os.getenv("UPSTREAM_URL", "http://10.11.5.100:18007/post")  # ← set your second server URL
DB_URL = os.getenv("DB_URL", "sqlite+aiosqlite:///./queue.db")

# Directory for storing JSON bodies as files
QUEUE_DIR = Path(os.getenv("QUEUE_DIR", "./queue_files")).resolve()

# Keep 1 to guarantee strict global ordering (one in flight).
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1"))

# Retry / polling tunables
RETRY_BASE_SECONDS = int(os.getenv("RETRY_BASE_SECONDS", "5"))
RETRY_MAX_SECONDS  = int(os.getenv("RETRY_MAX_SECONDS", "300"))
FORWARD_LOOP_SLEEP_IDLE = float(os.getenv("FORWARD_LOOP_SLEEP_IDLE", "0.5"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "60"))  # larger for big base64 payloads

# Optionally propagate selected inbound headers (comma-separated, case-insensitive)
HEADERS_TO_COPY = (
    os.getenv("HEADERS_TO_COPY", "x-request-id,x-trace-id").lower().split(",")
    if os.getenv("HEADERS_TO_COPY") is not None
    else []
)

# TLS verification (use `UPSTREAM_VERIFY=false` only for testing self-signed)
UPSTREAM_VERIFY = os.getenv("UPSTREAM_VERIFY", "true").lower()
if UPSTREAM_VERIFY in ("false", "0", "no"):
    HTTPX_VERIFY = False
else:
    HTTPX_VERIFY = UPSTREAM_VERIFY if os.path.exists(UPSTREAM_VERIFY) else True

# Static upstream headers (e.g., Authorization)
# Example: {"Authorization":"Bearer XYZ","X-API-Key":"abc"}
UPSTREAM_HEADERS_JSON = os.getenv("UPSTREAM_HEADERS_JSON")
STATIC_UPSTREAM_HEADERS = {}
if UPSTREAM_HEADERS_JSON:
    try:
        STATIC_UPSTREAM_HEADERS = json.loads(UPSTREAM_HEADERS_JSON)
    except Exception:
        pass

DEFAULT_ACCEPT = os.getenv("UPSTREAM_ACCEPT", "application/json")

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

    # Rotating file handler
    file_path = LOG_DIR / LOG_FILENAME
    fh = RotatingFileHandler(file_path, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT, encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(LOG_LEVEL)

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    ch.setLevel(LOG_LEVEL)

    # Avoid duplicate handlers if reloaded
    log.handlers.clear()
    log.addHandler(fh)
    log.addHandler(ch)

    # Also capture uvicorn & httpx into the same file
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "httpx"):
        lg = logging.getLogger(name)
        lg.setLevel(LOG_LEVEL)
        lg.handlers.clear()
        lg.addHandler(fh)
        lg.addHandler(ch)

    log.info("Logging initialized → file=%s, level=%s, rotate=%s bytes x %s",
             file_path, LOG_LEVEL, LOG_MAX_BYTES, LOG_BACKUP_COUNT)
    return log

log = setup_logging()

if UPSTREAM_HEADERS_JSON and not STATIC_UPSTREAM_HEADERS:
    log.warning("Failed to parse UPSTREAM_HEADERS_JSON; ignoring: %s", UPSTREAM_HEADERS_JSON)

# -------------------------------------------------
# Database (SQLite by default; swap DB_URL for MySQL/Postgres if needed)
# -------------------------------------------------
Base = declarative_base()

class Message(Base):
    __tablename__ = "messages"
    id = Column(Integer, primary_key=True, autoincrement=True)       # arrival/FIFO key
    message_uuid = Column(String(36), unique=True, nullable=False)   # for Idempotency-Key & filename
    content_type = Column(String(200), nullable=False, default="application/json")
    headers_json = Column(Text, nullable=False, default="{}")        # selected inbound headers (lower-cased)
    file_path = Column(Text, nullable=False)                         # absolute/normalized path to JSON file
    file_size = Column(Integer, nullable=False, default=0)
    received_at = Column(DateTime, nullable=False, default=func.now())
    status = Column(String(20), nullable=False, default="queued")    # queued|sending|sent|error
    attempts = Column(Integer, nullable=False, default=0)
    last_error = Column(Text, nullable=True)
    next_attempt_at = Column(DateTime, nullable=False, default=func.now())
    sent_at = Column(DateTime, nullable=True)

# Add connect_args timeout so SQLite waits on locks
engine = create_async_engine(
    DB_URL,
    future=True,
    echo=False,
    connect_args={"timeout": 30},  # seconds to wait on DB locks
)
Session = async_sessionmaker(engine, expire_on_commit=False)

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
    """Write bytes atomically: write to temp, flush+fsync, then replace to final path."""
    tmp = target.with_suffix(target.suffix + f".tmp-{uuid.uuid4().hex}")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, target)

async def enable_sqlite_durability():
    """SQLite: WAL + synchronous=FULL + busy_timeout for crash-safety and friendlier locking."""
    if DB_URL.startswith("sqlite+aiosqlite"):
        async with engine.begin() as conn:
            await conn.exec_driver_sql("PRAGMA journal_mode=WAL;")
            await conn.exec_driver_sql("PRAGMA synchronous=FULL;")
            await conn.exec_driver_sql("PRAGMA busy_timeout=30000;")  # 30s
            await conn.exec_driver_sql("PRAGMA wal_autocheckpoint=1000;")
        log.info("SQLite durability set: WAL + synchronous=FULL + busy_timeout=30s")

async def recover_stuck_messages_on_startup():
    """Reset any 'sending' rows to 'queued' after an unclean shutdown (with retries if DB is locked)."""
    for attempt in range(10):
        try:
            async with Session() as session:
                await session.execute(
                    update(Message)
                    .where(Message.status == "sending")
                    .values(status="queued", next_attempt_at=now_utc(), last_error="Recovered after restart")
                )
                await session.commit()
            log.info("Recovery complete: 'sending' → 'queued'")
            return
        except OperationalError as e:
            if "database is locked" in str(e).lower():
                delay = 0.5 * (attempt + 1)
                log.warning("Recover retry %s: DB locked; sleeping %.1fs", attempt + 1, delay)
                await asyncio.sleep(delay)
                continue
            raise
    log.error("Startup recovery failed due to persistent DB lock")

# -------------------------------------------------
# Forwarder (single worker keeps global order)
# -------------------------------------------------
async def forward_once(session: AsyncSession, client: httpx.AsyncClient) -> bool:
    stmt = (
        select(Message)
        .where(Message.status.in_(("queued", "error")), Message.next_attempt_at <= now_utc())
        .order_by(Message.id.asc())
        .limit(BATCH_SIZE)
    )
    res = await session.execute(stmt)
    msgs = res.scalars().all()
    if not msgs:
        return False

    for m in msgs:
        m.status = "sending"
    await session.commit()

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
        try:
            if not file_path.exists():
                # NEW: drop rows whose payload file is missing (non-retryable)
                log.error("File not found for id=%s: %s — dropping row", msg.id, file_path)
                try:
                    await session.delete(msg)
                    await session.commit()
                except Exception as e2:
                    await session.rollback()
                    msg.status = "error"
                    msg.last_error = f"Drop failed after missing file: {repr(e2)}"
                    msg.next_attempt_at = now_utc() + dt.timedelta(seconds=compute_next_backoff(msg.attempts + 1))
                    await session.commit()
                continue

            body = file_path.read_bytes()  # exact bytes we wrote
            log.debug("Sending id=%s → %s (file=%s, size=%s)", msg.id, UPSTREAM_URL, file_path.name, len(body))
            resp = await client.post(
                UPSTREAM_URL,
                content=body,  # RAW BYTES FORWARD (no re-serialization)
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            if 200 <= resp.status_code < 300:
                # Delete file first, then DB row
                try:
                    file_path.unlink(missing_ok=True)
                except Exception as e:
                    log.warning("Sent id=%s but file delete failed (%s): %r", msg.id, file_path, e)
                try:
                    await session.delete(msg)
                    await session.commit()
                    log.info("Sent & deleted id=%s ✓", msg.id)
                except Exception as e:
                    await session.rollback()
                    msg.status = "sent"
                    msg.sent_at = now_utc()
                    msg.last_error = f"DB delete failed: {repr(e)}"
                    await session.commit()
                    log.warning("Sent id=%s, but DB delete failed; left as 'sent'. err=%r", msg.id, e)
            else:
                msg.status = "error"
                msg.attempts += 1
                msg.last_error = f"HTTP {resp.status_code}: {resp.text[:500]}"
                msg.next_attempt_at = now_utc() + dt.timedelta(seconds=compute_next_backoff(msg.attempts))
                await session.commit()
                log.warning("Upstream HTTP %s for id=%s; will retry; body=%s",
                            resp.status_code, msg.id, resp.text[:200])
        except (httpx.RequestError, httpx.HTTPError) as e:
            msg.status = "error"
            msg.attempts += 1
            msg.last_error = f"Network error: {repr(e)}"
            msg.next_attempt_at = now_utc() + dt.timedelta(seconds=compute_next_backoff(msg.attempts))
            await session.commit()
            log.error("Network error for id=%s: %r", msg.id, e)

    return True

async def forward_loop():
    async with Session() as session:
        async with httpx.AsyncClient(http2=True, timeout=REQUEST_TIMEOUT, verify=HTTPX_VERIFY) as client:
            log.info("Forwarder started → %s (verify=%s)", UPSTREAM_URL, HTTPX_VERIFY)
            while True:
                processed = await forward_once(session, client)
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
    title="JSON Ingest → File Store → Forward (pass-through, delete-on-success, logs, lock-hardened)",
    version="1.6",
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
    Accept JSON **as-is** (raw body), write it to a file atomically, and enqueue for ordered forwarding.
    We do NOT parse or re-serialize the JSON; the bytes are stored and resent unmodified.
    """
    body = await req.body()
    if not body:
        raise HTTPException(status_code=400, detail="Empty body")

    content_type = req.headers.get("content-type", "application/json")

    # Keep selected inbound headers (lower-cased)
    selected_headers = {}
    if HEADERS_TO_COPY:
        for k, v in req.headers.items():
            lk = k.lower()
            if lk in HEADERS_TO_COPY:
                selected_headers[lk] = v

    # Prepare file path (use UUID to avoid collisions; simple flat dir)
    message_uuid = str(uuid.uuid4())
    filename = f"{message_uuid}.json"
    target_path = QUEUE_DIR / filename

    # Atomic write of payload bytes
    try:
        atomic_write_bytes(target_path, body)
    except Exception as e:
        log.error("Atomic write failed for %s: %r", target_path, e)
        raise HTTPException(status_code=500, detail="Failed to persist payload")

    file_size = target_path.stat().st_size

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
    async with Session() as session:
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

@app.get("/stats")
async def stats():
    """Note: 'sent' will usually be 0 because rows are deleted after success."""
    async with Session() as session:
        row = (await session.execute(
            select(
                func.count().filter(Message.status == "queued"),
                func.count().filter(Message.status == "sending"),
                func.count().filter(Message.status == "error"),
                func.count().filter(Message.status == "sent"),
            )
        )).one()
        info = {
            "queued": row[0],
            "sending": row[1],
            "error": row[2],
            "sent": row[3],
            "upstream_url": UPSTREAM_URL,
            "delete_on_success": True,
            "pass_through": True,
            "queue_dir": str(QUEUE_DIR),
            "log_file": str((LOG_DIR / LOG_FILENAME)),
        }
        return info

@app.get("/healthz")
async def healthz():
    return JSONResponse({"ok": True})

# Optional: one-shot manual nudge during debugging
@app.post("/debug/forward-once")
async def debug_forward_once():
    async with Session() as session:
        async with httpx.AsyncClient(http2=True, timeout=REQUEST_TIMEOUT, verify=HTTPX_VERIFY) as client:
            ok = await forward_once(session, client)
            return {"attempted": ok}
