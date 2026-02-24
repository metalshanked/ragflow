"""
Core service layer: task store, Excel I/O, and async assessment pipeline.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import logging
import math
import uuid
from datetime import datetime
from typing import Any

import openpyxl

from .config import settings
from .models import (
    DocumentStatus,
    DocumentUploadResponse,
    PipelineStage,
    QuestionResult,
    RagflowContext,
    Reference,
    SessionCreateResponse,
    TaskEvent,
    TaskRecord,
    TaskState,
    TaskStatus,
)
from .db import (
    db_add_task_event,
    db_get_task,
    db_list_task_events,
    db_list_tasks,
    db_save_task,
    db_task_lock,
    db_find_tasks_by_dataset_id,
    db_find_document_by_hash,
)
from .observability import openinference_attributes, set_span_attributes, start_span
from .ragflow_client import RagflowClient

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Task access — always reads from the database so that every pod in a
# horizontally-scaled deployment sees the latest state.
# ---------------------------------------------------------------------------


def _sync_ragflow_ids(record: TaskRecord) -> None:
    """Copy RAGFlow resource IDs and document statuses from the record into the status object."""
    record.status.dataset_id = record.ragflow.dataset_id or None
    record.status.dataset_ids = (
        record.ragflow.dataset_ids
        or ([record.ragflow.dataset_id] if record.ragflow.dataset_id else [])
    )
    record.status.chat_id = record.ragflow.chat_id or None
    record.status.session_id = record.ragflow.session_id or None
    record.status.document_ids = record.ragflow.document_ids or []
    record.status.document_statuses = record.document_statuses


async def get_task(task_id: str) -> TaskRecord | None:
    """Fetch a task record from the database."""
    record = await db_get_task(task_id)
    if record is not None:
        _sync_ragflow_ids(record)
    return record


async def list_tasks(page: int = 1, page_size: int = 50) -> tuple[list[TaskStatus], int]:
    """List all tasks from the database (paginated)."""
    return await db_list_tasks(page, page_size)  # db layer syncs ragflow IDs


async def list_task_events(task_id: str, page: int = 1, page_size: int = 100) -> tuple[list[TaskEvent], int]:
    """List task events from the database (paginated)."""
    raw_events, total = await db_list_task_events(task_id, page, page_size)
    return [TaskEvent(**e) for e in raw_events], total


# ---------------------------------------------------------------------------
# Read-only queries for existing data
# ---------------------------------------------------------------------------

async def find_tasks_by_dataset_id(dataset_id: str) -> list[TaskStatus]:
    """Return task statuses that reference the given RAGFlow dataset ID."""
    tasks = await db_find_tasks_by_dataset_id(dataset_id)
    # Ensure RAGFlow IDs synced into status for API
    return tasks


async def find_document_by_hash(file_hash: str) -> list[dict[str, Any]]:
    """Return occurrences of a document content hash across tasks."""
    return await db_find_document_by_hash(file_hash)


async def _update_status(
    record: TaskRecord,
    *,
    state: TaskState | None = None,
    stage: PipelineStage | None = None,
    message: str | None = None,
    error: str | None = None,
    questions_processed: int | None = None,
) -> None:
    prev_state = record.status.state
    prev_stage = record.status.pipeline_stage
    prev_message = record.status.progress_message
    prev_error = record.status.error
    prev_processed = record.status.questions_processed

    s = record.status
    if state is not None:
        s.state = state
    if stage is not None:
        s.pipeline_stage = stage
    if message is not None:
        s.progress_message = message
    if error is not None:
        s.error = error or None  # normalise empty string to None
    if questions_processed is not None:
        s.questions_processed = questions_processed
    s.updated_at = datetime.utcnow()
    # Sync RAGFlow resource IDs into the status so they appear in API responses
    _sync_ragflow_ids(record)
    await db_save_task(record)

    if (
        prev_state != s.state
        or prev_stage != s.pipeline_stage
        or prev_message != s.progress_message
        or prev_error != s.error
        or prev_processed != s.questions_processed
    ):
        try:
            await db_add_task_event(
                record.task_id,
                event_type="status_update",
                state=s.state,
                pipeline_stage=s.pipeline_stage,
                message=s.progress_message,
                error=s.error,
                payload={
                    "total_questions": s.total_questions,
                    "questions_processed": s.questions_processed,
                },
            )
        except Exception:
            logger.exception("Failed to append task event for %s", record.task_id)


# ---------------------------------------------------------------------------
# Excel helpers
# ---------------------------------------------------------------------------

def _resolve_column_index(col: str) -> int:
    """Convert a column specifier to a 0-based index.

    Accepts:
      - A single letter ("A", "B", …) → converted via openpyxl
      - A 1-based integer string ("1", "2", …) → converted to 0-based
    """
    col = col.strip()
    if col.isdigit():
        idx = int(col) - 1
        if idx < 0:
            raise ValueError(f"Column number must be >= 1, got '{col}'")
        return idx
    # Treat as Excel column letter(s)
    from openpyxl.utils import column_index_from_string
    return column_index_from_string(col.upper()) - 1


def parse_questions_excel(
    file_bytes: bytes,
    question_id_column: str | None = None,
    question_column: str | None = None,
    vendor_response_column: str | None = None,
    vendor_comment_column: str | None = None,
) -> list[dict[str, Any]]:
    """
    Read an Excel file and extract questions.

    By default column A = Question_Serial_No and column B = Question,
    but callers can override these via *question_id_column* and
    *question_column* (letter like ``"C"`` or 1-based number like ``"3"``).

    If *vendor_response_column* and *vendor_comment_column* are provided,
    those values are also extracted.

    If the first row of the resolved columns contains a non-numeric,
    non-empty string it is treated as a header and skipped.
    """
    id_col = _resolve_column_index(question_id_column or settings.question_id_column)
    q_col = _resolve_column_index(question_column or settings.question_column)
    v_res_col = _resolve_column_index(vendor_response_column or settings.vendor_response_column)
    v_com_col = _resolve_column_index(vendor_comment_column or settings.vendor_comment_column)
    max_col = max(id_col, q_col, v_res_col, v_com_col) + 1  # max_col is 1-based in openpyxl

    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True)
    ws = wb.active
    questions: list[dict[str, Any]] = []

    rows = list(ws.iter_rows(min_row=1, max_col=max_col, values_only=True))
    if not rows:
        wb.close()
        return questions

    # Detect header row: if the question column in row 1 looks like a header
    # (non-empty string that is not purely numeric), skip it.
    start = 0
    first_q = rows[0][q_col] if len(rows[0]) > q_col else None
    if first_q is not None and isinstance(first_q, str) and not first_q.strip().replace(".", "", 1).isdigit():
        start = 1  # skip header

    for idx, row in enumerate(rows[start:]):
        serial = row[id_col] if len(row) > id_col and row[id_col] is not None else idx + 1
        question_text = str(row[q_col]).strip() if len(row) > q_col and row[q_col] else ""
        if not question_text:
            continue
        
        vendor_response = str(row[v_res_col]).strip() if len(row) > v_res_col and row[v_res_col] is not None else ""
        vendor_comment = str(row[v_com_col]).strip() if len(row) > v_com_col and row[v_com_col] is not None else ""
        
        questions.append({
            "serial_no": serial, 
            "question": question_text,
            "vendor_response": vendor_response,
            "vendor_comment": vendor_comment
        })
    wb.close()
    return questions


def build_results_excel(results: list[QuestionResult]) -> bytes:
    """
    Build an Excel workbook with columns:
    Question_Serial_No | Question | Vendor_Response | Vendor_Comment | AI_Response | Details | References
    """
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Assessment Results"
    headers = [
        "Question_Serial_No",
        "Question",
        "Vendor_Response",
        "Vendor_Comment",
        "AI_Response",
        "Details",
        "References",
    ]
    ws.append(headers)
    for r in results:
        ref_texts = []
        for ref in r.references:
            parts = [ref.document_name]
            if ref.document_type:
                parts.append(f"[{ref.document_type.upper()}]")
            if ref.page_number is not None:
                parts.append(f"Page {ref.page_number}")
            elif ref.chunk_index is not None:
                parts.append(f"Chunk/Row {ref.chunk_index}")
            if ref.snippet:
                parts.append(ref.snippet[:120])
            ref_texts.append(" | ".join(parts))
        ws.append([
            r.question_serial_no,
            r.question,
            r.vendor_response,
            r.vendor_comment,
            r.ai_response,
            r.details,
            "\n".join(ref_texts),
        ])

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


# ---------------------------------------------------------------------------
# Shared question-processing helper
# ---------------------------------------------------------------------------

# How often to persist progress to the database during question processing.
# A value of 1 means every question triggers a DB write (original behaviour);
# higher values reduce DB round-trips at the cost of slightly staler progress.
_PROGRESS_BATCH_SIZE = 5


async def _process_questions(
    *,
    record: TaskRecord,
    questions: list[dict[str, Any]],
    client: RagflowClient,
    chat_id: str,
    session_id: str,
    process_vendor_response: bool | None = None,
    only_cited_references: bool | None = None,
) -> int:
    """Process all *questions* concurrently and return the count of failures.

    Results are appended to ``record.results`` and progress is persisted
    to the database in batches of ``_PROGRESS_BATCH_SIZE`` (and always on
    the final question) to reduce DB round-trips.
    """
    with start_span(
        "assessment.process_questions",
        span_kind="CHAIN",
        attributes={
            "task.id": record.task_id,
            "assessment.questions.count": len(questions),
            "assessment.chat_id": chat_id,
            "assessment.session_id": session_id,
        },
    ):
        await _update_status(record, message="Processing questions...")
        semaphore = asyncio.Semaphore(settings.max_concurrent_questions)
        total = len(questions)

        do_process_vendor = process_vendor_response if process_vendor_response is not None else settings.process_vendor_response
        do_only_cited = only_cited_references if only_cited_references is not None else settings.only_cited_references

        async def _process_one(q: dict[str, Any]) -> QuestionResult:
            async with semaphore:
                with start_span(
                    "assessment.process_question",
                    span_kind="CHAIN",
                    attributes={
                        "task.id": record.task_id,
                        "assessment.question.serial_no": str(q.get("serial_no", "")),
                        "input.value": str(q.get("question", "")),
                    },
                ) as q_span:
                    q_text = q["question"]
                    v_res = q.get("vendor_response", "")
                    v_com = q.get("vendor_comment", "")

                    if do_process_vendor and (v_res or v_com):
                        q_text = (
                            f"The vendor responded '{v_res}' with comments: '{v_com}'. "
                            f"Please verify if this is correct based on the documents. "
                            f"Question: {q_text}"
                        )

                    response = await client.ask(
                        chat_id, session_id, q_text, stream=False
                    )
                    answer_text = response.get("answer", "")
                    verdict, details = RagflowClient.parse_yes_no(answer_text)
                    raw_refs = RagflowClient.extract_references(response)

                    # Filter to only cited references when configured
                    if do_only_cited and raw_refs:
                        cited = RagflowClient.get_cited_indices(answer_text)
                        if cited:
                            raw_refs = [
                                r for i, r in enumerate(raw_refs) if i in cited
                            ]

                    refs = [Reference(**r) for r in raw_refs]

                    result = QuestionResult(
                        question_serial_no=q["serial_no"],
                        question=q["question"],
                        vendor_response=v_res,
                        vendor_comment=v_com,
                        ai_response=verdict,
                        details=details,
                        references=refs,
                    )
                    set_span_attributes(
                        q_span,
                        {
                            "output.value": answer_text,
                            "assessment.verdict": verdict,
                            "assessment.references.count": len(refs),
                        },
                    )

                    record.results.append(result)
                    processed = len(record.results)
                    # Batch DB writes: persist every N questions and on the last one.
                    if processed % _PROGRESS_BATCH_SIZE == 0 or processed == total:
                        await _update_status(
                            record,
                            questions_processed=processed,
                            message=f"Processed {processed}/{total} questions",
                        )
                    return result

        tasks = [_process_one(q) for q in questions]
        gathered = await asyncio.gather(*tasks, return_exceptions=True)

        failed_count = 0
        for i, res in enumerate(gathered):
            if isinstance(res, BaseException):
                failed_count += 1
                logger.error("Question %d failed: %s", i, res)
        if failed_count:
            logger.warning("%d out of %d questions failed", failed_count, total)

        return failed_count


# ---------------------------------------------------------------------------
# Async assessment pipeline
# ---------------------------------------------------------------------------

async def run_assessment(
    task_id: str,
    questions: list[dict[str, Any]],
    evidence_files: list[tuple[str, bytes]],
    dataset_name: str | None = None,
    chat_name: str | None = None,
    dataset_opts: dict | None = None,
    chat_opts: dict | None = None,
    process_vendor_response: bool | None = None,
    only_cited_references: bool | None = None,
) -> None:
    """
    Background coroutine that orchestrates the full assessment pipeline:

    1. Create dataset
    2. Upload evidence documents
    3. Parse documents and wait
    4. Create chat assistant linked to dataset
    5. Create session
    6. Ask each question (with concurrency)
    7. Collect results
    """
    with start_span(
        "assessment.run_assessment",
        span_kind="CHAIN",
        attributes={
            "task.id": task_id,
            "assessment.questions.count": len(questions),
            "assessment.evidence.count": len(evidence_files),
        },
    ):
        with openinference_attributes(
            session_id=task_id,
            metadata={
                "workflow": "single_call",
                "dataset_name": dataset_name or "",
                "chat_name": chat_name or "",
            },
        ):
            record = await get_task(task_id)
            if record is None:
                raise ValueError(f"Task {task_id} not found")
            client = RagflowClient()

            dataset_opts = dataset_opts or {}
            chat_opts = chat_opts or {}

            try:
                # -- 1. Create dataset ------------------------------------------------
                await _update_status(
                    record,
                    state=TaskState.UPLOADING,
                    stage=PipelineStage.DOCUMENT_UPLOAD,
                    message="Creating dataset...",
                )
                ds_name = dataset_name or f"{settings.default_chat_name_prefix}_{task_id[:8]}"
                dataset_id = await client.ensure_dataset(ds_name, **dataset_opts)
                record.ragflow.dataset_id = dataset_id
                record.ragflow.dataset_ids = [dataset_id]

                # -- 2. Upload evidence docs (parallel) --------------------------------
                await _update_status(record, message="Uploading evidence documents...")
                upload_sem = asyncio.Semaphore(settings.max_concurrent_questions)

                # 1. Pre-calculate hashes and filter out duplicates
                files_to_upload = []
                new_doc_hashes = {}  # temporary map for this batch: hash -> filename
                skipped_count = 0

                # We need to process files to find which are new vs existing in this session
                for fname, fbytes in evidence_files:
                    file_hash = hashlib.sha256(fbytes).hexdigest()
                    # For run_assessment, record.ragflow.file_hashes is empty initially,
                    # but we still check for duplicates within the batch.
                    if file_hash in new_doc_hashes:
                        skipped_count += 1
                        continue

                    new_doc_hashes[file_hash] = fname
                    files_to_upload.append((fname, fbytes, file_hash))

                if not files_to_upload and evidence_files:
                    raise RuntimeError(f"All {len(evidence_files)} evidence documents were duplicates of each other.")

                async def _upload_one(fname: str, fbytes: bytes, fhash: str) -> tuple[str, str]:
                    async with upload_sem:
                        doc_id = await client.upload_document(dataset_id, fname, fbytes)
                        return doc_id, fhash

                # Upload only new unique files
                uploaded_results = await asyncio.gather(
                    *(_upload_one(fn, fb, fh) for fn, fb, fh in files_to_upload)
                )

                doc_ids = []
                for doc_id, fhash in uploaded_results:
                    doc_ids.append(doc_id)
                    record.ragflow.file_hashes[fhash] = doc_id

                record.ragflow.document_ids = doc_ids

                if not doc_ids:
                    raise RuntimeError("No evidence documents were uploaded")

                # -- 3. Parse documents -----------------------------------------------
                await _update_status(
                    record,
                    state=TaskState.PARSING,
                    stage=PipelineStage.DOCUMENT_PARSING,
                    message="Parsing evidence documents...",
                )
                await client.start_parsing(dataset_id, doc_ids)
                doc_statuses_raw = await client.wait_for_parsing(dataset_id, doc_ids)
                record.document_statuses = [DocumentStatus(**ds) for ds in doc_statuses_raw]

                ok_ids = [ds["document_id"] for ds in doc_statuses_raw if ds["status"] == "success"]
                failed = [ds for ds in doc_statuses_raw if ds["status"] != "success"]
                if failed:
                    names = ", ".join(ds["document_name"] or ds["document_id"] for ds in failed)
                    logger.warning("Documents with parsing issues: %s", names)
                if not ok_ids:
                    raise RuntimeError(
                        "All documents failed to parse. "
                        + "; ".join(f"{ds['document_name'] or ds['document_id']}: {ds['message']}" for ds in failed)
                    )
                await _update_status(
                    record,
                    message=f"Parsing complete: {len(ok_ids)} succeeded, {len(failed)} failed",
                )

                # -- 4. Create chat assistant -----------------------------------------
                await _update_status(
                    record,
                    state=TaskState.PROCESSING,
                    stage=PipelineStage.CHAT_PROCESSING,
                    message="Creating chat assistant...",
                )
                c_name = chat_name or f"{settings.default_chat_name_prefix}_chat_{task_id[:8]}"
                chat_id = await client.ensure_chat(
                    c_name,
                    [dataset_id],
                    similarity_threshold=settings.default_similarity_threshold,
                    top_n=settings.default_top_n,
                    **chat_opts
                )
                record.ragflow.chat_id = chat_id

                # -- 5. Create session ------------------------------------------------
                session_id = await client.create_session(chat_id)
                record.ragflow.session_id = session_id

                # -- 6. Process questions concurrently --------------------------------
                failed_count = await _process_questions(
                    record=record,
                    questions=questions,
                    client=client,
                    chat_id=chat_id,
                    session_id=session_id,
                    process_vendor_response=process_vendor_response,
                    only_cited_references=only_cited_references,
                )

                # -- 7. Done ----------------------------------------------------------
                final_msg = "Assessment completed"
                if failed_count:
                    final_msg = f"Assessment completed with {failed_count} question failure(s)"
                await _update_status(
                    record,
                    state=TaskState.COMPLETED,
                    stage=PipelineStage.FINALIZING,
                    message=final_msg,
                )

            except Exception as exc:
                logger.exception("Assessment pipeline failed for task %s", task_id)
                await _update_status(
                    record,
                    state=TaskState.FAILED,
                    stage=PipelineStage.IDLE,
                    message="Pipeline failed",
                    error=str(exc),
                )
            finally:
                await client.close()


async def create_task(
    questions: list[dict[str, Any]],
    state: TaskState = TaskState.PENDING,
) -> TaskRecord:
    """Create a new task record and persist it to the database."""
    task_id = uuid.uuid4().hex
    status = TaskStatus(
        task_id=task_id,
        state=state,
        total_questions=len(questions),
    )
    record = TaskRecord(
        task_id=task_id,
        status=status,
        questions=questions,
    )
    await db_save_task(record)
    try:
        await db_add_task_event(
            task_id,
            event_type="task_created",
            state=status.state,
            pipeline_stage=status.pipeline_stage,
            message="Task created",
            payload={"total_questions": len(questions)},
        )
    except Exception:
        logger.exception("Failed to append task_created event for %s", task_id)
    return record


# ---------------------------------------------------------------------------
# Two-phase workflow helpers
# ---------------------------------------------------------------------------

async def create_session(
    questions: list[dict[str, Any]],
    dataset_name: str | None = None,
    dataset_opts: dict | None = None,
) -> SessionCreateResponse:
    """
    Phase 1: Create a task record and a RAGFlow dataset upfront.

    The caller can then upload evidence documents incrementally via
    ``add_documents_to_session`` and finally trigger the assessment
    with ``start_assessment_for_session``.
    """
    record = await create_task(questions, state=TaskState.AWAITING_DOCUMENTS)
    client = RagflowClient()
    dataset_id: str = ""
    dataset_opts = dataset_opts or {}
    try:
        ds_name = dataset_name or f"{settings.default_chat_name_prefix}_{record.task_id[:8]}"
        dataset_id = await client.ensure_dataset(ds_name, **dataset_opts)
        record.ragflow.dataset_id = dataset_id
        record.ragflow.dataset_ids = [dataset_id]
        await _update_status(
            record,
            state=TaskState.AWAITING_DOCUMENTS,
            stage=PipelineStage.IDLE,
            message="Session created. Upload evidence documents then start the assessment.",
        )
    except Exception as exc:
        logger.exception("Failed to create session for task %s", record.task_id)
        await _update_status(
            record,
            state=TaskState.FAILED,
            message="Session creation failed",
            error=str(exc),
        )
        raise
    finally:
        await client.close()

    return SessionCreateResponse(
        task_id=record.task_id,
        dataset_id=dataset_id,
    )


async def claim_session_start(task_id: str) -> TaskRecord:
    """Atomically validate + claim a session task for processing."""
    async with db_task_lock(task_id):
        record = await get_task(task_id)
        if record is None:
            raise ValueError(f"Task {task_id} not found")
        if record.status.state not in (TaskState.AWAITING_DOCUMENTS, TaskState.FAILED):
            raise ValueError(
                f"Cannot start assessment in state '{record.status.state.value}'. "
                f"Task must be in 'awaiting_documents' or 'failed' state."
            )
        if not record.ragflow.document_ids:
            raise ValueError("No evidence documents uploaded. Upload at least one document first.")

        # Reset stale run artifacts before queueing a new run.
        record.results.clear()
        record.document_statuses.clear()
        await _update_status(
            record,
            state=TaskState.PARSING,
            stage=PipelineStage.DOCUMENT_PARSING,
            message="Assessment queued. Starting document parsing...",
            questions_processed=0,
            error="",
        )
        return record


async def add_documents_to_session(
    task_id: str,
    files: list[tuple[str, bytes]],
) -> DocumentUploadResponse:
    """
    Phase 2 (repeatable): Upload one or more evidence documents to the
    dataset associated with an existing assessment session.

    This endpoint also accepts tasks in the ``FAILED`` state so that
    users can upload replacement or additional documents after a
    pipeline failure without losing the dataset or previously uploaded
    files.  The task is automatically moved back to
    ``AWAITING_DOCUMENTS`` so that the assessment can be re-started.
    """
    async with db_task_lock(task_id):
        record = await get_task(task_id)
        if record is None:
            raise ValueError(f"Task {task_id} not found")
        if record.status.state not in (TaskState.AWAITING_DOCUMENTS, TaskState.FAILED):
            raise ValueError(
                f"Cannot upload documents in state '{record.status.state.value}'. "
                f"Task must be in 'awaiting_documents' or 'failed' state."
            )
        dataset_id = record.ragflow.dataset_id
        if not dataset_id:
            raise ValueError("No dataset associated with this task")

        client = RagflowClient()
        try:
            upload_sem = asyncio.Semaphore(settings.max_concurrent_questions)

            # 1. Pre-calculate hashes and filter out duplicates
            files_to_upload = []
            new_doc_hashes = {}  # temporary map for this batch: hash -> filename
            skipped_count = 0

            # We need to process files to find which are new vs existing in this session
            for fname, fbytes in files:
                file_hash = hashlib.sha256(fbytes).hexdigest()
                if file_hash in record.ragflow.file_hashes:
                    # Already uploaded in this session.
                    skipped_count += 1
                    continue

                # Also check if we have duplicates within this batch
                if file_hash in new_doc_hashes:
                    skipped_count += 1
                    continue

                new_doc_hashes[file_hash] = fname
                files_to_upload.append((fname, fbytes, file_hash))

            if not files_to_upload and skipped_count > 0:
                return DocumentUploadResponse(
                    task_id=task_id,
                    dataset_id=dataset_id,
                    uploaded_document_ids=[],
                    total_documents=len(record.ragflow.document_ids),
                    message=f"All {skipped_count} document(s) were duplicates and skipped.",
                )

            async def _upload_one(fname: str, fbytes: bytes, fhash: str) -> tuple[str, str]:
                async with upload_sem:
                    doc_id = await client.upload_document(dataset_id, fname, fbytes)
                    return doc_id, fhash

            # Upload only new unique files
            uploaded_results = await asyncio.gather(
                *(_upload_one(fn, fb, fh) for fn, fb, fh in files_to_upload)
            )

            new_ids = []
            for doc_id, fhash in uploaded_results:
                new_ids.append(doc_id)
                record.ragflow.file_hashes[fhash] = doc_id

            record.ragflow.document_ids.extend(new_ids)

            # If the task was FAILED, move it back to AWAITING_DOCUMENTS so the
            # user can re-start the assessment after uploading new documents.
            new_state = (
                TaskState.AWAITING_DOCUMENTS
                if record.status.state == TaskState.FAILED
                else None
            )

            msg = f"Uploaded {len(new_ids)} document(s)."
            if skipped_count > 0:
                msg += f" Skipped {skipped_count} duplicate(s)."

            await _update_status(
                record,
                state=new_state,
                message=f"{len(record.ragflow.document_ids)} document(s) available. {msg}",
                error="" if new_state == TaskState.AWAITING_DOCUMENTS else None,
            )
        finally:
            await client.close()

        return DocumentUploadResponse(
            task_id=task_id,
            dataset_id=dataset_id,
            uploaded_document_ids=new_ids,
            total_documents=len(record.ragflow.document_ids),
            message=f"{msg} Total: {len(record.ragflow.document_ids)}.",
        )


async def run_assessment_for_session(
    task_id: str,
    chat_name: str | None = None,
    dataset_opts: dict | None = None,
    chat_opts: dict | None = None,
    process_vendor_response: bool | None = None,
    only_cited_references: bool | None = None,
) -> None:
    """
    Phase 3: Trigger the assessment pipeline for a session that already has
    its dataset and documents prepared.

    This picks up from step 3 (parsing) onward in ``run_assessment``.

    The function also accepts tasks in the ``FAILED`` state so that the
    user can retry after uploading replacement or additional documents.
    Previous results are cleared before the new run begins.
    """
    with start_span(
        "assessment.run_assessment_for_session",
        span_kind="CHAIN",
        attributes={"task.id": task_id},
    ):
        with openinference_attributes(
            session_id=task_id,
            metadata={"workflow": "two_phase_resume", "chat_name": chat_name or ""},
        ):
            record = await get_task(task_id)
            if record is None:
                raise ValueError(f"Task {task_id} not found")
            if record.status.state not in (
                TaskState.PARSING,
                TaskState.AWAITING_DOCUMENTS,
                TaskState.FAILED,
            ):
                raise ValueError(
                    f"Cannot start assessment in state '{record.status.state.value}'. "
                    f"Task must be in 'parsing', 'awaiting_documents' or 'failed' state."
                )
            if not record.ragflow.document_ids:
                raise ValueError("No evidence documents have been uploaded yet.")

            # Clear stale results from any previous (failed) run so that
            # the new run starts from a clean slate.
            record.results.clear()
            record.document_statuses.clear()
            record.status.questions_processed = 0
            record.status.error = None

            dataset_id = record.ragflow.dataset_id
            if not record.ragflow.dataset_ids and dataset_id:
                record.ragflow.dataset_ids = [dataset_id]
            doc_ids = record.ragflow.document_ids
            questions = record.questions
            client = RagflowClient()

            try:
                # -- Parse documents --------------------------------------------------
                await _update_status(
                    record,
                    state=TaskState.PARSING,
                    stage=PipelineStage.DOCUMENT_PARSING,
                    message="Parsing evidence documents...",
                )
                await client.start_parsing(dataset_id, doc_ids)
                doc_statuses_raw = await client.wait_for_parsing(dataset_id, doc_ids)
                record.document_statuses = [DocumentStatus(**ds) for ds in doc_statuses_raw]

                ok_ids = [ds["document_id"] for ds in doc_statuses_raw if ds["status"] == "success"]
                failed = [ds for ds in doc_statuses_raw if ds["status"] != "success"]
                if failed:
                    names = ", ".join(ds["document_name"] or ds["document_id"] for ds in failed)
                    logger.warning("Documents with parsing issues: %s", names)
                if not ok_ids:
                    raise RuntimeError(
                        "All documents failed to parse. "
                        + "; ".join(f"{ds['document_name'] or ds['document_id']}: {ds['message']}" for ds in failed)
                    )
                await _update_status(
                    record,
                    message=f"Parsing complete: {len(ok_ids)} succeeded, {len(failed)} failed",
                )

                # -- Create chat assistant --------------------------------------------
                await _update_status(
                    record,
                    state=TaskState.PROCESSING,
                    stage=PipelineStage.CHAT_PROCESSING,
                    message="Creating chat assistant...",
                )
                c_name = chat_name or f"{settings.default_chat_name_prefix}_chat_{task_id[:8]}"
                chat_opts = chat_opts or {}
                chat_id = await client.ensure_chat(
                    c_name,
                    [dataset_id],
                    similarity_threshold=settings.default_similarity_threshold,
                    top_n=settings.default_top_n,
                    **chat_opts
                )
                record.ragflow.chat_id = chat_id

                # -- Create session ---------------------------------------------------
                session_id = await client.create_session(chat_id)
                record.ragflow.session_id = session_id

                # -- Process questions concurrently -----------------------------------
                failed_count = await _process_questions(
                    record=record,
                    questions=questions,
                    client=client,
                    chat_id=chat_id,
                    session_id=session_id,
                    process_vendor_response=process_vendor_response,
                    only_cited_references=only_cited_references,
                )

                # -- Done -------------------------------------------------------------
                final_msg = "Assessment completed"
                if failed_count:
                    final_msg = f"Assessment completed with {failed_count} question failure(s)"
                await _update_status(
                    record,
                    state=TaskState.COMPLETED,
                    stage=PipelineStage.FINALIZING,
                    message=final_msg,
                )

            except Exception as exc:
                logger.exception("Assessment pipeline failed for task %s", task_id)
                await _update_status(
                    record,
                    state=TaskState.FAILED,
                    stage=PipelineStage.IDLE,
                    message="Pipeline failed",
                    error=str(exc),
                )
            finally:
                await client.close()


async def run_assessment_from_dataset(
    task_id: str,
    dataset_ids: list[str],
    chat_name: str | None = None,
    chat_opts: dict | None = None,
    process_vendor_response: bool | None = None,
    only_cited_references: bool | None = None,
) -> None:
    """
    Background coroutine for assessments against one or more **existing**
    RAGFlow datasets whose documents are already uploaded and parsed.

    Skips dataset creation, document upload, and parsing.  Starts directly
    from chat assistant creation → session → question processing.
    """
    with start_span(
        "assessment.run_assessment_from_dataset",
        span_kind="CHAIN",
        attributes={
            "task.id": task_id,
            "assessment.dataset.count": len(dataset_ids),
        },
    ):
        with openinference_attributes(
            session_id=task_id,
            metadata={"workflow": "from_dataset", "chat_name": chat_name or ""},
        ):
            record = await get_task(task_id)
            if record is None:
                raise ValueError(f"Task {task_id} not found")
            # Store the first dataset ID for backward-compatible status display;
            # all IDs are passed to the chat assistant.
            record.ragflow.dataset_id = dataset_ids[0] if dataset_ids else ""
            record.ragflow.dataset_ids = list(dataset_ids)
            client = RagflowClient()

            try:
                # -- Create chat assistant --------------------------------------------
                await _update_status(
                    record,
                    state=TaskState.PROCESSING,
                    stage=PipelineStage.CHAT_PROCESSING,
                    message="Creating chat assistant...",
                )
                c_name = chat_name or f"{settings.default_chat_name_prefix}_chat_{task_id[:8]}"
                chat_opts = chat_opts or {}
                chat_id = await client.ensure_chat(
                    c_name,
                    dataset_ids,
                    similarity_threshold=settings.default_similarity_threshold,
                    top_n=settings.default_top_n,
                    **chat_opts
                )
                record.ragflow.chat_id = chat_id

                # -- Create session ---------------------------------------------------
                session_id = await client.create_session(chat_id)
                record.ragflow.session_id = session_id

                # -- Process questions concurrently -----------------------------------
                questions = record.questions
                failed_count = await _process_questions(
                    record=record,
                    questions=questions,
                    client=client,
                    chat_id=chat_id,
                    session_id=session_id,
                    process_vendor_response=process_vendor_response,
                    only_cited_references=only_cited_references,
                )

                # -- Done -------------------------------------------------------------
                final_msg = "Assessment completed"
                if failed_count:
                    final_msg = f"Assessment completed with {failed_count} question failure(s)"
                await _update_status(
                    record,
                    state=TaskState.COMPLETED,
                    stage=PipelineStage.FINALIZING,
                    message=final_msg,
                )

            except Exception as exc:
                logger.exception("Assessment pipeline failed for task %s", task_id)
                await _update_status(
                    record,
                    state=TaskState.FAILED,
                    stage=PipelineStage.IDLE,
                    message="Pipeline failed",
                    error=str(exc),
                )
            finally:
                await client.close()


def get_paginated_results(
    record: TaskRecord,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    """Return a page of results from a task record."""
    total = len(record.results)
    total_pages = max(1, math.ceil(total / page_size))
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    end = start + page_size
    return {
        "task_id": record.task_id,
        "state": record.status.state,
        "total_questions": record.status.total_questions,
        "questions_processed": record.status.questions_processed,
        "results": record.results[start:end],
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "dataset_id": record.ragflow.dataset_id or None,
        "dataset_ids": record.ragflow.dataset_ids or (
            [record.ragflow.dataset_id] if record.ragflow.dataset_id else []
        ),
        "chat_id": record.ragflow.chat_id or None,
        "session_id": record.ragflow.session_id or None,
        "document_ids": record.ragflow.document_ids or [],
        "document_statuses": record.document_statuses,
    }
