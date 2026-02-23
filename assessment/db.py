"""
Database layer for the Assessment API.

Provides async SQLAlchemy persistence for task records.
Supports SQLite (default) and PostgreSQL via the ASSESSMENT_DATABASE_URL
environment variable.

For PostgreSQL the database is created automatically on first startup if
it does not already exist — no manual ``CREATE DATABASE`` step is needed.
"""

from __future__ import annotations

import json
import logging
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

    async with _engine.begin() as conn:
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


# Advisory-lock ID used by PostgreSQL to ensure only one pod runs the
# cleanup job at a time.  The value is arbitrary but must be consistent
# across all instances.
_CLEANUP_ADVISORY_LOCK_ID = 738_291_046  # arbitrary 32-bit constant


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
                    {"id": _CLEANUP_ADVISORY_LOCK_ID},
                )
                acquired = lock_result.scalar()
                if not acquired:
                    logger.debug(
                        "Cleanup lock held by another instance – skipping this cycle"
                    )
                    return 0

            result = await session.execute(
                delete(TaskRow).where(TaskRow.created_at < cutoff)
            )
            deleted: int = result.rowcount  # type: ignore[assignment]
    if deleted:
        logger.info("Purged %d task(s) older than %d day(s)", deleted, retention_days)
    return deleted
