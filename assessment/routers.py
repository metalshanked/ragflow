"""
FastAPI routers for the Assessment API.

Endpoints
---------
Single-call (legacy):
  POST   /api/v1/assessments                             - Upload everything & start

From existing dataset:
  POST   /api/v1/assessments/from-dataset                - Use existing RAGFlow dataset

Two-phase workflow:
  POST   /api/v1/assessments/sessions                    - Create session (questions + dataset)
  POST   /api/v1/assessments/sessions/{task_id}/documents - Upload evidence docs (repeatable)
  POST   /api/v1/assessments/sessions/{task_id}/start     - Trigger assessment

Proxy (RAGFlow resource passthrough):
  *      /api/v1/ragflow/{path}                          - Direct official RAGFlow API passthrough
  GET    /api/v1/proxy/image/{image_id}                   - Proxy RAGFlow chunk image
  GET    /api/v1/proxy/document/{document_id}             - Proxy RAGFlow document

Common:
  GET    /api/v1/assessments                              - List all tasks
  GET    /api/v1/assessments/{task_id}                    - Get task status
  GET    /api/v1/assessments/{task_id}/results             - Get results (JSON, paginated)
  GET    /api/v1/assessments/{task_id}/results/excel       - Download results as Excel
    POST   /api/v1/assessments/documents/upload            - Upload documents (standalone)
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import Response
from pydantic import BaseModel

from .auth import verify_jwt
from .config import settings
import json
from .models import (
    DocumentUploadResponse,
    SessionCreateResponse,
    TaskEventListResponse,
    TaskListResponse,
    TaskResultResponse,
    TaskState,
    TaskStatus,
)
from .ragflow_client import RagflowClient


class DocumentLookupResponse(BaseModel):
    class Match(BaseModel):
        task_id: str
        document_id: str
        dataset_id: Optional[str] = None

    matches: list[Match] = []
    total: int = 0
from .services import (
    add_documents_to_session,
    build_results_excel,
    claim_session_start,
    create_session,
    create_task,
    get_paginated_results,
    get_task,
    list_task_events,
    list_tasks,
    parse_questions_excel,
    run_assessment,
    run_assessment_for_session,
    run_assessment_from_dataset,
    find_tasks_by_dataset_id,
    find_document_by_hash,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1",
    tags=["assessment"],
    dependencies=[Depends(verify_jwt)],
)

_RAGFLOW_BASE_API = "/api/v1"
_PASSTHROUGH_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"]
_HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
}


def _extract_error_detail(resp: httpx.Response) -> str:
    """Best-effort extraction of an error message from a RAGFlow response."""
    try:
        body = resp.json()
    except Exception:
        text = resp.text.strip()
        return text[:500] if text else "RAGFlow request failed"

    if isinstance(body, dict):
        msg = body.get("message") or body.get("detail")
        if isinstance(msg, str) and msg.strip():
            return msg.strip()
    return str(body)[:500]


def _parse_ragflow_json_or_raise(resp: httpx.Response) -> dict[str, Any]:
    """Parse JSON body and enforce official RAGFlow success semantics."""
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=_extract_error_detail(resp))

    try:
        payload = resp.json()
    except Exception:
        raise HTTPException(
            status_code=502,
            detail="RAGFlow returned a non-JSON response.",
        )

    if isinstance(payload, dict):
        code = payload.get("code")
        if code not in (None, 0):
            raise HTTPException(status_code=502, detail=str(payload.get("message") or payload))
        return payload

    raise HTTPException(status_code=502, detail="RAGFlow returned an unexpected JSON payload.")


def _copy_passthrough_headers(headers: httpx.Headers) -> dict[str, str]:
    """Return downstream headers safe to relay to client."""
    forwarded: dict[str, str] = {}
    for key, value in headers.items():
        lowered = key.lower()
        if lowered in _HOP_BY_HOP_HEADERS or lowered == "content-type":
            continue
        forwarded[key] = value
    return forwarded


async def _request_ragflow_official(
    method: str,
    official_subpath: str,
    *,
    params: Any = None,
    content: bytes | None = None,
    json_body: Any = None,
    files: Any = None,
    extra_headers: dict[str, str] | None = None,
    timeout: float = 120.0,
) -> httpx.Response:
    """Send an authenticated request to RAGFlow official `/api/v1/*` APIs."""
    method = method.upper()
    clean_path = official_subpath.strip("/")
    url = f"{settings.ragflow_base_url.rstrip('/')}{_RAGFLOW_BASE_API}/{clean_path}"

    headers = {"Authorization": f"Bearer {settings.ragflow_api_key}"}
    if extra_headers:
        headers.update(extra_headers)

    try:
        async with httpx.AsyncClient(
            timeout=timeout,
            verify=RagflowClient._ssl_verify(),
        ) as client:
            return await client.request(
                method,
                url,
                headers=headers,
                params=params,
                content=content,
                json=json_body,
                files=files,
            )
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail="Cannot connect to RAGFlow server")
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="RAGFlow server timed out")
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Error communicating with RAGFlow: {exc}")


@router.api_route(
    "/ragflow/{ragflow_path:path}",
    methods=_PASSTHROUGH_METHODS,
)
async def ragflow_official_passthrough(ragflow_path: str, request: Request):
    """
    Direct passthrough to official RAGFlow `/api/v1/*` endpoints.

    Example:
      GET /api/v1/ragflow/datasets?page=1&page_size=20
      -> GET {ragflow_base_url}/api/v1/datasets?page=1&page_size=20
    """
    method = request.method.upper()
    body = await request.body()

    # Keep caller content negotiation headers, but always use configured
    # RAGFlow API key for upstream authentication.
    extra_headers: dict[str, str] = {}
    for header_name in ("content-type", "accept"):
        if header_name in request.headers:
            extra_headers[header_name] = request.headers[header_name]

    resp = await _request_ragflow_official(
        method,
        ragflow_path,
        params=list(request.query_params.multi_items()),
        content=body,
        extra_headers=extra_headers,
        timeout=300.0,
    )

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers={
            **_copy_passthrough_headers(resp.headers),
            "Content-Type": resp.headers.get("content-type", "application/octet-stream"),
        },
    )


# ===========================================================================
# Single-call workflow (legacy – upload everything at once)
# ===========================================================================

@router.post("/assessments", response_model=TaskStatus, status_code=202)
async def start_assessment(
    background_tasks: BackgroundTasks,
    questions_file: UploadFile = File(
        ..., description="Excel file with columns A=Question_Serial_No, B=Question"
    ),
    evidence_files: list[UploadFile] = File(
        ..., description="Evidence documents (PDF, PPTX, XLSX, DOCX, etc.)"
    ),
    dataset_name: Optional[str] = Form(None, description="Custom dataset name in RAGFlow"),
    chat_name: Optional[str] = Form(None, description="Custom chat assistant name in RAGFlow"),
    dataset_options: Optional[str] = Form(None, description="JSON string of additional options for dataset creation"),
    chat_options: Optional[str] = Form(None, description="JSON string of additional options for chat creation"),
    question_id_column: Optional[str] = Form(
        None,
        description="Column for Question Serial No (letter e.g. 'A' or 1-based number). Defaults to server setting.",
    ),
    question_column: Optional[str] = Form(
        None,
        description="Column for Question text (letter e.g. 'B' or 1-based number). Defaults to server setting.",
    ),
    vendor_response_column: Optional[str] = Form(
        None,
        description="Column for Vendor response (letter e.g. 'C' or 1-based number). Defaults to server setting.",
    ),
    vendor_comment_column: Optional[str] = Form(
        None,
        description="Column for Vendor comments (letter e.g. 'D' or 1-based number). Defaults to server setting.",
    ),
    process_vendor_response: bool = Form(
        settings.process_vendor_response,
        description="If true, verify vendor response and comments in determining results.",
    ),
    only_cited_references: bool = Form(
        settings.only_cited_references,
        description="If true (default), only include references actually cited as [ID:N] in the answer.",
    ),
):
    """
    **Single-call flow** – upload a questions Excel file and *all* evidence
    documents in one request.

    For large or incremental uploads, use the **two-phase** workflow instead:
    1. ``POST /assessments/sessions``
    2. ``POST /assessments/sessions/{task_id}/documents``  (repeat as needed)
    3. ``POST /assessments/sessions/{task_id}/start``

    Returns immediately with a **task_id** to poll for status/results.
    """
    q_bytes = await questions_file.read()
    try:
        questions = parse_questions_excel(
            q_bytes,
            question_id_column,
            question_column,
            vendor_response_column,
            vendor_comment_column,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to parse questions file: {exc}",
        )
    if not questions:
        raise HTTPException(status_code=400, detail="No questions found in the uploaded Excel file.")

    ev_files: list[tuple[str, bytes]] = []
    for ef in evidence_files:
        ev_bytes = await ef.read()
        ev_files.append((ef.filename or "document", ev_bytes))
    if not ev_files:
        raise HTTPException(status_code=400, detail="At least one evidence document is required.")

    record = await create_task(questions)

    dataset_opts = {}
    if dataset_options:
        try:
            dataset_opts = json.loads(dataset_options)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON in dataset_options")
    
    chat_opts = {}
    if chat_options:
        try:
            chat_opts = json.loads(chat_options)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON in chat_options")

    background_tasks.add_task(
        run_assessment,
        record.task_id,
        questions,
        ev_files,
        dataset_name,
        chat_name,
        dataset_opts,
        chat_opts,
        process_vendor_response,
        only_cited_references,
    )

    return record.status


# ===========================================================================
# From existing dataset  (skip upload & parsing)
# ===========================================================================

@router.post("/assessments/from-dataset", response_model=TaskStatus, status_code=202)
async def start_assessment_from_dataset(
    background_tasks: BackgroundTasks,
    questions_file: UploadFile = File(
        ..., description="Excel file with columns A=Question_Serial_No, B=Question"
    ),
    dataset_ids: str = Form(
        ...,
        description=(
            "One or more existing RAGFlow dataset IDs (documents already uploaded "
            "& parsed).  Pass a single ID or multiple comma-separated IDs, e.g. "
            "'id1,id2,id3'."
        ),
    ),
    chat_name: Optional[str] = Form(None, description="Custom chat assistant name in RAGFlow"),
    dataset_options: Optional[str] = Form(None, description="JSON string of additional options for dataset creation"),
    chat_options: Optional[str] = Form(None, description="JSON string of additional options for chat creation"),
    question_id_column: Optional[str] = Form(
        None,
        description="Column for Question Serial No (letter e.g. 'A' or 1-based number). Defaults to server setting.",
    ),
    question_column: Optional[str] = Form(
        None,
        description="Column for Question text (letter e.g. 'B' or 1-based number). Defaults to server setting.",
    ),
    vendor_response_column: Optional[str] = Form(
        None,
        description="Column for Vendor response (letter e.g. 'C' or 1-based number). Defaults to server setting.",
    ),
    vendor_comment_column: Optional[str] = Form(
        None,
        description="Column for Vendor comments (letter e.g. 'D' or 1-based number). Defaults to server setting.",
    ),
    process_vendor_response: bool = Form(
        settings.process_vendor_response,
        description="If true, verify vendor response and comments in determining results.",
    ),
    only_cited_references: bool = Form(
        settings.only_cited_references,
        description="If true (default), only include references actually cited as [ID:N] in the answer.",
    ),
):
    """
    Run an assessment against one or more **existing** RAGFlow datasets.

    Skips dataset creation, document upload, and parsing entirely.  The
    datasets must already contain uploaded and parsed evidence documents.

    Pass multiple dataset IDs as a comma-separated string in the
    ``dataset_ids`` form field (e.g. ``id1,id2,id3``).

    Returns immediately with a **task_id** to poll for status/results.
    """
    # Parse comma-separated dataset IDs
    parsed_ids = [did.strip() for did in dataset_ids.split(",") if did.strip()]
    if not parsed_ids:
        raise HTTPException(status_code=400, detail="At least one dataset_id is required.")

    q_bytes = await questions_file.read()
    try:
        questions = parse_questions_excel(
            q_bytes,
            question_id_column,
            question_column,
            vendor_response_column,
            vendor_comment_column,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to parse questions file: {exc}",
        )
    if not questions:
        raise HTTPException(status_code=400, detail="No questions found in the uploaded Excel file.")

    record = await create_task(questions)

    dataset_opts = {}
    if dataset_options:
        try:
            dataset_opts = json.loads(dataset_options)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON in dataset_options")
    
    chat_opts = {}
    if chat_options:
        try:
            chat_opts = json.loads(chat_options)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON in chat_options")

    background_tasks.add_task(
        run_assessment_from_dataset,
        record.task_id,
        parsed_ids,
        chat_name,
        chat_opts,
        process_vendor_response,
        only_cited_references,
    )

    return record.status


# ===========================================================================
# Two-phase workflow  (create → upload docs incrementally → start)
# ===========================================================================

@router.post("/assessments/sessions", response_model=SessionCreateResponse, status_code=201)
async def create_assessment_session(
    questions_file: UploadFile = File(
        ..., description="Excel file with columns A=Question_Serial_No, B=Question"
    ),
    dataset_name: Optional[str] = Form(None, description="Custom dataset name in RAGFlow"),
    dataset_options: Optional[str] = Form(None, description="JSON string of additional options for dataset creation"),
    chat_options: Optional[str] = Form(None, description="JSON string of additional options for chat creation"),
    question_id_column: Optional[str] = Form(
        None,
        description="Column for Question Serial No (letter e.g. 'A' or 1-based number). Defaults to server setting.",
    ),
    question_column: Optional[str] = Form(
        None,
        description="Column for Question text (letter e.g. 'B' or 1-based number). Defaults to server setting.",
    ),
    vendor_response_column: Optional[str] = Form(
        None,
        description="Column for Vendor response (letter e.g. 'C' or 1-based number). Defaults to server setting.",
    ),
    vendor_comment_column: Optional[str] = Form(
        None,
        description="Column for Vendor comments (letter e.g. 'D' or 1-based number). Defaults to server setting.",
    ),
):
    """
    **Phase 1** – Create an assessment session.

    Uploads the questions Excel and creates a RAGFlow dataset.  Returns a
    ``task_id`` and ``dataset_id``.  Evidence documents can then be uploaded
    incrementally via ``POST /assessments/sessions/{task_id}/documents``.
    """
    q_bytes = await questions_file.read()
    try:
        questions = parse_questions_excel(
            q_bytes,
            question_id_column,
            question_column,
            vendor_response_column,
            vendor_comment_column,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to parse questions file: {exc}",
        )
    if not questions:
        raise HTTPException(status_code=400, detail="No questions found in the uploaded Excel file.")

    dataset_opts = {}
    if dataset_options:
        try:
            dataset_opts = json.loads(dataset_options)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON in dataset_options")

    return await create_session(questions, dataset_name, dataset_opts)


@router.post(
    "/assessments/sessions/{task_id}/documents",
    response_model=DocumentUploadResponse,
)
async def upload_session_documents(
    task_id: str,
    files: list[UploadFile] = File(..., description="Evidence documents to add"),
):
    """
    **Phase 2** (repeatable) – Upload one or more evidence documents to an
    existing assessment session.

    Can be called multiple times before starting the assessment.
    """
    record = await get_task(task_id)
    if not record:
        raise HTTPException(status_code=404, detail="Task not found")

    file_pairs: list[tuple[str, bytes]] = []
    for f in files:
        fbytes = await f.read()
        file_pairs.append((f.filename or "document", fbytes))
    if not file_pairs:
        raise HTTPException(status_code=400, detail="At least one file is required.")

    try:
        return await add_documents_to_session(task_id, file_pairs)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.post("/assessments/sessions/{task_id}/start", response_model=TaskStatus, status_code=202)
async def start_session_assessment(
    task_id: str,
    background_tasks: BackgroundTasks,
    chat_name: Optional[str] = Form(None, description="Custom chat assistant name in RAGFlow"),
    dataset_options: Optional[str] = Form(None, description="JSON string of additional options for dataset creation"),
    chat_options: Optional[str] = Form(None, description="JSON string of additional options for chat creation"),
    process_vendor_response: bool = Form(
        settings.process_vendor_response,
        description="If true, verify vendor response and comments in determining results.",
    ),
    only_cited_references: bool = Form(
        settings.only_cited_references,
        description="If true (default), only include references actually cited as [ID:N] in the answer.",
    ),
):
    """
    **Phase 3** – Trigger the assessment pipeline.

    All evidence documents must already be uploaded.  The pipeline will parse
    documents, create a chat assistant, and process every question.

    Returns immediately; poll ``GET /assessments/{task_id}`` for progress.
    """
    try:
        record = await claim_session_start(task_id)
    except ValueError as exc:
        msg = str(exc)
        if "not found" in msg.lower():
            raise HTTPException(status_code=404, detail=msg)
        if "no evidence documents uploaded" in msg.lower():
            raise HTTPException(status_code=400, detail=msg)
        raise HTTPException(status_code=409, detail=msg)

    dataset_opts = {}
    if dataset_options:
        try:
            dataset_opts = json.loads(dataset_options)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON in dataset_options")
            
    chat_opts = {}
    if chat_options:
        try:
            chat_opts = json.loads(chat_options)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON in chat_options")

    background_tasks.add_task(
        run_assessment_for_session,
        task_id,
        chat_name,
        dataset_opts,
        chat_opts,
        process_vendor_response,
        only_cited_references,
    )

    return record.status


# ---------------------------------------------------------------------------
# GET /assessments  –  List all tasks
# ---------------------------------------------------------------------------

@router.get("/assessments", response_model=TaskListResponse)
async def list_all_tasks(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=500, description="Tasks per page"),
):
    """Return status of all assessment tasks (paginated)."""
    import math
    tasks, total = await list_tasks(page, page_size)
    total_pages = math.ceil(total / page_size) if page_size > 0 else 1
    return TaskListResponse(
        tasks=tasks,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )


@router.get("/assessments/by-dataset/{dataset_id}", response_model=TaskListResponse)
async def get_tasks_by_dataset(dataset_id: str):
    """Return all tasks that reference the given RAGFlow dataset ID."""
    tasks = await find_tasks_by_dataset_id(dataset_id)
    total = len(tasks)
    return TaskListResponse(
        tasks=tasks,
        total=total,
        page=1,
        page_size=total or 1,
        total_pages=1,
    )


# ---------------------------------------------------------------------------
# GET /assessments/{task_id}  –  Task status
# ---------------------------------------------------------------------------

@router.get("/assessments/{task_id}", response_model=TaskStatus)
async def get_task_status(task_id: str):
    """Return current status / progress of a specific task."""
    record = await get_task(task_id)
    if not record:
        raise HTTPException(status_code=404, detail="Task not found")
    return record.status


# ---------------------------------------------------------------------------
# GET /assessments/{task_id}/events  –  Task events
# ---------------------------------------------------------------------------

@router.get("/assessments/{task_id}/events", response_model=TaskEventListResponse)
async def get_task_event_history(
    task_id: str,
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(100, ge=1, le=1000, description="Events per page"),
):
    """Return task event history for audit/debugging (paginated, newest-first)."""
    record = await get_task(task_id)
    if not record:
        raise HTTPException(status_code=404, detail="Task not found")
    events, total = await list_task_events(task_id, page, page_size)
    import math

    total_pages = max(1, math.ceil(total / page_size))
    return TaskEventListResponse(
        task_id=task_id,
        events=events,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )


# ---------------------------------------------------------------------------
# GET /assessments/{task_id}/results  –  Paginated JSON results
# ---------------------------------------------------------------------------

@router.get("/assessments/{task_id}/results", response_model=TaskResultResponse)
async def get_results(
    task_id: str,
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=500, description="Results per page"),
):
    """
    Retrieve assessment results in JSON with pagination.

    Available even while the task is still processing (partial results).
    """
    record = await get_task(task_id)
    if not record:
        raise HTTPException(status_code=404, detail="Task not found")
    return get_paginated_results(record, page, page_size)


# ---------------------------------------------------------------------------
# GET /assessments/{task_id}/results/excel  –  Download Excel
# ---------------------------------------------------------------------------

@router.get("/assessments/{task_id}/results/excel")
async def download_results_excel(task_id: str):
    """Download the assessment results as an Excel (.xlsx) file."""
    record = await get_task(task_id)
    if not record:
        raise HTTPException(status_code=404, detail="Task not found")
    if record.status.state not in (TaskState.COMPLETED, TaskState.PROCESSING):
        raise HTTPException(
            status_code=409,
            detail=f"Results not ready yet. Current state: {record.status.state.value}",
        )
    excel_bytes = build_results_excel(record.results)
    return Response(
        content=excel_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="assessment_{task_id[:8]}.xlsx"'
        },
    )


@router.get("/assessments/documents/by-hash/{file_hash}", response_model=DocumentLookupResponse)
async def get_document_by_hash(file_hash: str):
    """Locate existing uploaded document(s) by their content hash.

    Useful to avoid re-uploading the same file across tasks/datasets.
    """
    rows = await find_document_by_hash(file_hash)
    matches = [DocumentLookupResponse.Match(**r) for r in rows]
    return DocumentLookupResponse(matches=matches, total=len(matches))


# ===========================================================================
# Proxy endpoints  –  RAGFlow image / document passthrough
# ===========================================================================

@router.get("/proxy/image/{image_id}")
async def proxy_image(image_id: str):
    """
    Proxy a RAGFlow chunk image so that clients never receive raw RAGFlow URLs.

    Streams the response directly from the RAGFlow server.
    """
    from .ragflow_client import RagflowClient

    ragflow_url = f"{settings.ragflow_base_url}/v1/document/image/{image_id}"
    headers = {"Authorization": f"Bearer {settings.ragflow_api_key}"}
    try:
        async with httpx.AsyncClient(timeout=30, verify=RagflowClient._ssl_verify()) as client:
            resp = await client.get(ragflow_url, headers=headers)
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail="Cannot connect to RAGFlow server")
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="RAGFlow server timed out")
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Error communicating with RAGFlow: {exc}")
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail="Failed to fetch image from RAGFlow")
    return Response(
        content=resp.content,
        media_type=resp.headers.get("content-type", "application/octet-stream"),
        headers={"Cache-Control": "public, max-age=3600"},
    )


@router.get("/proxy/document/{document_id}")
async def proxy_document(document_id: str):
    """
    Proxy a RAGFlow document download so that clients never receive raw
    RAGFlow URLs.

    Streams the response directly from the RAGFlow server.
    """
    from .ragflow_client import RagflowClient

    ragflow_url = f"{settings.ragflow_base_url}/v1/document/get/{document_id}"
    headers = {"Authorization": f"Bearer {settings.ragflow_api_key}"}
    try:
        async with httpx.AsyncClient(timeout=60, verify=RagflowClient._ssl_verify()) as client:
            resp = await client.get(ragflow_url, headers=headers)
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail="Cannot connect to RAGFlow server")
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="RAGFlow server timed out")
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Error communicating with RAGFlow: {exc}")
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail="Failed to fetch document from RAGFlow")
    # Forward content-type and content-disposition if present
    response_headers = {}
    if "content-disposition" in resp.headers:
        response_headers["Content-Disposition"] = resp.headers["content-disposition"]
    return Response(
        content=resp.content,
        media_type=resp.headers.get("content-type", "application/octet-stream"),
        headers=response_headers,
    )


# ---------------------------------------------------------------------------
# POST /assessments/documents/upload  –  Standalone document upload
# ---------------------------------------------------------------------------

@router.post("/assessments/documents/upload")
async def upload_documents(
    dataset_id: str = Form(..., description="Existing RAGFlow dataset ID"),
    files: list[UploadFile] = File(..., description="Documents to upload"),
    parse: bool = Form(True, description="Trigger parsing after upload"),
):
    """
    Upload one or more documents to an existing RAGFlow dataset.

    Optionally triggers parsing immediately.
    """
    import asyncio
    from .config import settings as _settings

    try:
        upload_sem = asyncio.Semaphore(_settings.max_concurrent_questions)

        async def _upload_one(f: UploadFile) -> str:
            fbytes = await f.read()
            async with upload_sem:
                resp = await _request_ragflow_official(
                    "POST",
                    f"datasets/{dataset_id}/documents",
                    files={"file": (f.filename or "file", fbytes)},
                )
                payload = _parse_ragflow_json_or_raise(resp)
                docs = payload.get("data") or []
                if not docs:
                    raise HTTPException(status_code=502, detail="RAGFlow upload succeeded but no document returned")
                doc_id = str(docs[0].get("id", "")).strip()
                if not doc_id:
                    raise HTTPException(status_code=502, detail="RAGFlow upload returned an empty document id")
                return doc_id

        doc_ids = list(await asyncio.gather(*(_upload_one(f) for f in files)))
        if parse and doc_ids:
            parse_resp = await _request_ragflow_official(
                "POST",
                f"datasets/{dataset_id}/chunks",
                json_body={"document_ids": doc_ids},
            )
            _parse_ragflow_json_or_raise(parse_resp)
        return {
            "dataset_id": dataset_id,
            "document_ids": doc_ids,
            "parsing_triggered": parse,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


# ===========================================================================
# Dataset & Document Management
# ===========================================================================

class DeleteDatasetsRequest(BaseModel):
    ids: list[str]


class DeleteDocumentsRequest(BaseModel):
    ids: list[str]


@router.get("/datasets", tags=["ragflow-passthrough"])
async def list_datasets(
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1),
    name: Optional[str] = Query(None),
):
    params: dict[str, Any] = {"page": page, "page_size": page_size}
    if name:
        params["name"] = name

    resp = await _request_ragflow_official("GET", "datasets", params=params)

    # Preserve existing behavior from RagflowClient: when searching by name,
    # some RAGFlow versions return a permission error for a non-existing name.
    if name and resp.status_code >= 400:
        detail = _extract_error_detail(resp).lower()
        if "lacks permission" in detail:
            return {
                "items": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
            }

    payload = _parse_ragflow_json_or_raise(resp)

    data = payload.get("data", [])
    if isinstance(data, list):
        items = data
        total = None
    elif isinstance(data, dict):
        inner = data.get("data")
        items = inner if isinstance(inner, list) else []
        total = data.get("total")
    else:
        items = []
        total = 0

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.delete("/datasets", tags=["ragflow-passthrough"])
async def delete_datasets(req: DeleteDatasetsRequest):
    resp = await _request_ragflow_official("DELETE", "datasets", json_body={"ids": req.ids})
    _parse_ragflow_json_or_raise(resp)
    return {"message": "Datasets deleted"}


@router.get("/datasets/{dataset_id}/documents", tags=["ragflow-passthrough"])
async def list_documents(
    dataset_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1),
):
    resp = await _request_ragflow_official(
        "GET",
        f"datasets/{dataset_id}/documents",
        params={"page": page, "page_size": page_size},
    )
    payload = _parse_ragflow_json_or_raise(resp)

    data = payload.get("data", {})
    if isinstance(data, dict):
        items = data.get("docs", [])
        total = data.get("total")
    elif isinstance(data, list):
        items = data
        total = None
    else:
        items = []
        total = 0

    for doc in items:
        doc.pop("words", None)
        run = str(doc.get("run", ""))
        progress = float(doc.get("progress", 0))
        status = "pending"
        if run in ("FAIL", "2"):
            status = "failed"
        elif progress >= 0.999:
            status = "success"
        elif progress > 0:
            status = "running"
        doc["status"] = status

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.delete("/datasets/{dataset_id}/documents", tags=["ragflow-passthrough"])
async def delete_documents(dataset_id: str, req: DeleteDocumentsRequest):
    resp = await _request_ragflow_official(
        "DELETE",
        f"datasets/{dataset_id}/documents",
        json_body={"ids": req.ids},
    )
    _parse_ragflow_json_or_raise(resp)
    return {"message": "Documents deleted"}
