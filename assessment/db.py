"""
Database layer for the Assessment API.

Provides async SQLAlchemy persistence for task records.
Supports SQLite (default) and PostgreSQL via the ASSESSMENT_DATABASE_URL
environment variable.

For PostgreSQL the database is created automatically on first startup if
it does not already exist — no manual ``CREATE DATABASE`` step is needed.
"""

from __future__ import annotations

import hashlib
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import Column, DateTime, Integer, String, Text, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from .config import settings
from .models import (
    DocumentStatus,
    PipelineStage,
    QuestionResult,
    RagflowContext,
    TaskRecord,
    TaskState,
    TaskStatus,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQLAlchemy declarative base & table
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    pass


class TaskRow(Base):
    """Single-table representation of a TaskRecord."""

    __tablename__ = "tasks"

    task_id = Column(String(64), primary_key=True)

    # TaskStatus fields
    state = Column(String(32), nullable=False, default=TaskState.PENDING.value)
    pipeline_stage = Column(String(32), nullable=False, default=PipelineStage.IDLE.value)
    progress_message = Column(Text, nullable=False, default="")
    total_questions = Column(Integer, nullable=False, default=0)
    questions_processed = Column(Integer, nullable=False, default=0)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    # JSON-serialised blobs
    ragflow_json = Column(Text, nullable=False, default="{}")
    questions_json = Column(Text, nullable=False, default="[]")
    results_json = Column(Text, nullable=False, default="[]")
    document_statuses_json = Column(Text, nullable=False, default="[]")


class TaskEventRow(Base):
    """Append-only task event/audit trail."""

    __tablename__ = "task_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(64), nullable=False, index=True)
    event_type = Column(String(64), nullable=False, default="status_update")
    state = Column(String(32), nullable=True)
    pipeline_stage = Column(String(32), nullable=True)
    message = Column(Text, nullable=False, default="")
    error = Column(Text, nullable=True)
    payload_json = Column(Text, nullable=False, default="{}")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)


# ---------------------------------------------------------------------------
# Engine / session factory
# ---------------------------------------------------------------------------

_engine = create_async_engine(
    settings.database_url,
    echo=False,
    # SQLite needs special handling for async
    **({"connect_args": {"check_same_thread": False}} if settings.database_url.startswith("sqlite") else {}),
)

async_session_factory = async_sessionmaker(_engine, expire_on_commit=False)


def _is_postgres() -> bool:
    return settings.database_url.startswith("postgresql")


def _task_lock_key(task_id: str) -> int:
    """Return a stable positive bigint key for per-task advisory locks."""
    digest = hashlib.sha256(task_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False) & 0x7FFF_FFFF_FFFF_FFFF


def _cleanup_lock_id() -> int:
    """Internally derived, stable advisory lock ID for cleanup coordination.

    We avoid configuration by hashing a fixed namespace string to a
    positive 63-bit integer, suitable for PostgreSQL advisory locks.
    """
    ns = b"assessment:task_cleanup_v1"
    d = hashlib.sha256(ns).digest()
    return int.from_bytes(d[:8], byteorder="big", signed=False) & 0x7FFF_FFFF_FFFF_FFFF


@asynccontextmanager
async def db_task_lock(task_id: str):
    """Cross-pod per-task lock.

    Uses PostgreSQL session-level advisory locks when running against PG.
    For SQLite and other engines this is a no-op.
    """
    if not _is_postgres():
        yield
        return

    lock_key = _task_lock_key(task_id)
    conn = await _engine.connect()
    try:
        await conn.execute(text("SELECT pg_advisory_lock(:id)"), {"id": lock_key})
        yield
    finally:
        try:
            await conn.execute(text("SELECT pg_advisory_unlock(:id)"), {"id": lock_key})
        finally:
            await conn.close()


async def _ensure_pg_database() -> None:
    """Create the PostgreSQL database if it does not already exist.

    Connects to the server's default ``postgres`` maintenance database and
    issues ``CREATE DATABASE`` inside ``AUTOCOMMIT`` mode (required by PG
    for DDL statements that cannot run inside a transaction block).
    """
    parsed = urlparse(settings.database_url.replace("+asyncpg", ""))
    db_name = parsed.path.lstrip("/")
    if not db_name:
        return

    # Build a URL that points to the default "postgres" database.
    maint_url = settings.database_url.rsplit("/", 1)[0] + "/postgres"
    tmp_engine = create_async_engine(maint_url, isolation_level="AUTOCOMMIT")
    try:
        async with tmp_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :db"),
                {"db": db_name},
            )
            if result.scalar() is None:
                # Database does not exist – create it.
                # Note: db_name is from our own config, not user input,
                # and CREATE DATABASE does not support parameter binding.
                await conn.execute(text(f'CREATE DATABASE "{db_name}"'))
                logger.info("Created PostgreSQL database '%s'", db_name)
            else:
                logger.info("PostgreSQL database '%s' already exists", db_name)
    finally:
        await tmp_engine.dispose()


async def init_db() -> None:
    """Create the database (PostgreSQL) and tables if they don't exist.

    Call once at application startup.
    """
    if settings.database_url.startswith("postgresql"):
        await _ensure_pg_database()

    bootstrap_mode = settings.database_bootstrap_mode.strip().lower()
    if bootstrap_mode not in {"create", "recreate"}:
        raise ValueError(
            "Invalid ASSESSMENT_DATABASE_BOOTSTRAP_MODE. "
            "Expected 'create' or 'recreate'."
        )
    is_pg = settings.database_url.startswith("postgresql")
    if (
        is_pg
        and bootstrap_mode == "recreate"
        and not settings.database_allow_destructive_recreate
    ):
        raise ValueError(
            "Refusing destructive bootstrap on PostgreSQL. "
            "Set ASSESSMENT_DATABASE_ALLOW_DESTRUCTIVE_RECREATE=true only for controlled teardown scenarios."
        )

    async with _engine.begin() as conn:
        if bootstrap_mode == "recreate":
            logger.warning(
                "Database bootstrap mode is 'recreate': dropping and recreating all assessment tables."
            )
            await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables ensured at %s", settings.database_url)


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def _task_record_from_row(row: TaskRow) -> TaskRecord:
    status = TaskStatus(
        task_id=row.task_id,
        state=TaskState(row.state),
        pipeline_stage=PipelineStage(row.pipeline_stage),
        progress_message=row.progress_message or "",
        total_questions=row.total_questions,
        questions_processed=row.questions_processed,
        created_at=row.created_at,
        updated_at=row.updated_at,
        error=row.error,
    )
    ragflow = RagflowContext(**json.loads(row.ragflow_json))
    questions = json.loads(row.questions_json)
    results = [QuestionResult(**r) for r in json.loads(row.results_json)]
    doc_statuses = [DocumentStatus(**ds) for ds in json.loads(row.document_statuses_json or "[]")]
    # Sync RAGFlow resource IDs into the status so API responses include them
    status.dataset_id = ragflow.dataset_id or None
    status.dataset_ids = ragflow.dataset_ids or ([ragflow.dataset_id] if ragflow.dataset_id else [])
    status.chat_id = ragflow.chat_id or None
    status.session_id = ragflow.session_id or None
    status.document_ids = ragflow.document_ids or []
    status.document_statuses = doc_statuses
    return TaskRecord(
        task_id=row.task_id,
        status=status,
        ragflow=ragflow,
        questions=questions,
        results=results,
        document_statuses=doc_statuses,
    )


def _row_from_task_record(record: TaskRecord) -> dict[str, Any]:
    """Return a flat dict suitable for INSERT / UPDATE."""
    s = record.status
    return {
        "task_id": record.task_id,
        "state": s.state.value,
        "pipeline_stage": s.pipeline_stage.value,
        "progress_message": s.progress_message,
        "total_questions": s.total_questions,
        "questions_processed": s.questions_processed,
        "error": s.error,
        "created_at": s.created_at,
        "updated_at": s.updated_at,
        "ragflow_json": record.ragflow.model_dump_json(),
        "questions_json": json.dumps(record.questions),
        "results_json": json.dumps([r.model_dump() for r in record.results], default=str),
        "document_statuses_json": json.dumps([ds.model_dump() for ds in record.document_statuses], default=str),
    }


# ---------------------------------------------------------------------------
# CRUD operations  (used by services.py)
# ---------------------------------------------------------------------------

async def db_save_task(record: TaskRecord) -> None:
    """Insert or fully replace a task record in the database."""
    data = _row_from_task_record(record)
    async with async_session_factory() as session:
        async with session.begin():
            existing = await session.get(TaskRow, record.task_id)
            if existing is None:
                session.add(TaskRow(**data))
            else:
                for key, value in data.items():
                    setattr(existing, key, value)


async def db_get_task(task_id: str) -> TaskRecord | None:
    """Fetch a single task by ID, or *None*."""
    async with async_session_factory() as session:
        row = await session.get(TaskRow, task_id)
        if row is None:
            return None
        return _task_record_from_row(row)


async def db_list_tasks(page: int = 1, page_size: int = 50) -> tuple[list[TaskStatus], int]:
    """Return the status of every task (paginated)."""
    from sqlalchemy import select, func

    async with async_session_factory() as session:
        # 1. Count total
        count_stmt = select(func.count()).select_from(TaskRow)
        total = (await session.execute(count_stmt)).scalar() or 0

        # 2. Fetch page
        stmt = (
            select(TaskRow)
            .order_by(TaskRow.created_at.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        result = await session.execute(stmt)
        rows = result.scalars().all()
        return [_task_record_from_row(r).status for r in rows], total


async def db_add_task_event(
    task_id: str,
    *,
    event_type: str = "status_update",
    state: TaskState | None = None,
    pipeline_stage: PipelineStage | None = None,
    message: str = "",
    error: str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    """Append a task event row."""
    row = TaskEventRow(
        task_id=task_id,
        event_type=event_type,
        state=state.value if state else None,
        pipeline_stage=pipeline_stage.value if pipeline_stage else None,
        message=message or "",
        error=error,
        payload_json=json.dumps(payload or {}, default=str),
    )
    async with async_session_factory() as session:
        async with session.begin():
            session.add(row)


async def db_list_task_events(
    task_id: str,
    page: int = 1,
    page_size: int = 100,
) -> tuple[list[dict[str, Any]], int]:
    """Return paginated task events ordered newest-first."""
    from sqlalchemy import func, select

    async with async_session_factory() as session:
        count_stmt = select(func.count()).select_from(TaskEventRow).where(TaskEventRow.task_id == task_id)
        total = (await session.execute(count_stmt)).scalar() or 0

        stmt = (
            select(TaskEventRow)
            .where(TaskEventRow.task_id == task_id)
            .order_by(TaskEventRow.created_at.desc(), TaskEventRow.id.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        result = await session.execute(stmt)
        rows = result.scalars().all()

        events: list[dict[str, Any]] = []
        for row in rows:
            events.append(
                {
                    "id": row.id,
                    "task_id": row.task_id,
                    "event_type": row.event_type,
                    "state": TaskState(row.state) if row.state else None,
                    "pipeline_stage": PipelineStage(row.pipeline_stage) if row.pipeline_stage else None,
                    "message": row.message or "",
                    "error": row.error,
                    "payload": json.loads(row.payload_json or "{}"),
                    "created_at": row.created_at,
                }
            )
        return events, total


async def db_purge_old_tasks(retention_days: int) -> int:
    """Delete task rows whose ``created_at`` is older than *retention_days*.

    Returns the number of rows deleted.  Does nothing when
    *retention_days* is ``0`` or negative.

    When running on **PostgreSQL**, a ``pg_try_advisory_xact_lock`` is
    acquired so that only one pod in a horizontally-scaled deployment
    performs the cleanup at any given time.  Other pods skip the cycle
    gracefully.
    """
    if retention_days <= 0:
        return 0

    from sqlalchemy import delete

    is_pg = settings.database_url.startswith("postgresql")

    cutoff = datetime.utcnow() - timedelta(days=retention_days)
    async with async_session_factory() as session:
        async with session.begin():
            # On PostgreSQL, try to acquire a transaction-level advisory
            # lock.  If another pod already holds it we skip this cycle.
            if is_pg:
                lock_result = await session.execute(
                    text("SELECT pg_try_advisory_xact_lock(:id)"),
                    {"id": _cleanup_lock_id()},
                )
                acquired = lock_result.scalar()
                if not acquired:
                    logger.debug(
                        "Cleanup lock held by another instance – skipping this cycle"
                    )
                    return 0

            # Purge event rows first (safe even without FK constraints).
            await session.execute(
                delete(TaskEventRow).where(TaskEventRow.created_at < cutoff)
            )
            result = await session.execute(
                delete(TaskRow).where(TaskRow.created_at < cutoff)
            )
            deleted: int = result.rowcount  # type: ignore[assignment]
    if deleted:
        logger.info("Purged %d task(s) older than %d day(s)", deleted, retention_days)
    return deleted


# ---------------------------------------------------------------------------
# Query helpers for existing data lookups
# ---------------------------------------------------------------------------

async def db_find_tasks_by_dataset_id(dataset_id: str) -> list[TaskStatus]:
    """Find tasks that reference the given RAGFlow ``dataset_id``.

    Implementation detail: we perform a coarse SQL LIKE filter on the
    JSON blob and then confirm matches by parsing JSON to avoid false
    positives. Works on both SQLite and PostgreSQL.
    """
    from sqlalchemy import select

    needle = f"%{dataset_id}%"
    matches: list[TaskStatus] = []
    async with async_session_factory() as session:
        stmt = select(TaskRow).where(TaskRow.ragflow_json.like(needle))
        result = await session.execute(stmt)
        rows = result.scalars().all()
        for row in rows:
            try:
                rag = RagflowContext(**json.loads(row.ragflow_json or "{}"))
            except Exception:
                continue
            if rag.dataset_id == dataset_id or (dataset_id in (rag.dataset_ids or [])):
                matches.append(_task_record_from_row(row).status)
    return matches


async def db_find_document_by_hash(file_hash: str) -> list[dict[str, Any]]:
    """Find documents by their content hash across tasks.

    Returns list of dicts: {"task_id", "document_id", "dataset_id"}
    for each occurrence where ``ragflow.file_hashes[hash] == document_id``.
    """
    from sqlalchemy import select

    needle = f"%{file_hash}%"
    found: list[dict[str, Any]] = []
    async with async_session_factory() as session:
        stmt = select(TaskRow).where(TaskRow.ragflow_json.like(needle))
        result = await session.execute(stmt)
        rows = result.scalars().all()
        for row in rows:
            try:
                rag = RagflowContext(**json.loads(row.ragflow_json or "{}"))
            except Exception:
                continue
            doc_id = (rag.file_hashes or {}).get(file_hash)
            if doc_id:
                found.append({
                    "task_id": row.task_id,
                    "document_id": doc_id,
                    "dataset_id": rag.dataset_id or (rag.dataset_ids[0] if rag.dataset_ids else None),
                })
    return found
