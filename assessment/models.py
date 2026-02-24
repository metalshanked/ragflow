"""
Pydantic models / schemas for the Assessment API.
"""

from __future__ import annotations

import enum
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class TaskState(str, enum.Enum):
    PENDING = "pending"
    AWAITING_DOCUMENTS = "awaiting_documents"  # session created, waiting for doc uploads
    UPLOADING = "uploading"
    PARSING = "parsing"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class PipelineStage(str, enum.Enum):
    IDLE = "idle"
    DOCUMENT_UPLOAD = "document_upload"
    DOCUMENT_PARSING = "document_parsing"
    CHAT_PROCESSING = "chat_processing"
    FINALIZING = "finalizing"


# ---------------------------------------------------------------------------
# Reference / Result models
# ---------------------------------------------------------------------------

class DocumentStatus(BaseModel):
    """Per-document parsing status reported back in the task response."""
    document_id: str
    document_name: str = ""
    status: str = "pending"  # pending | running | success | failed | timeout | not_found
    progress: float = 0.0  # 0.0 – 1.0
    message: str = ""  # human-readable status / error detail


class Reference(BaseModel):
    document_name: str = ""
    document_type: str = ""  # e.g. "pdf", "excel", "docx", "ppt", "md", "txt", …
    page_number: Optional[int] = None  # PDF page number or PPT/PPTX slide number
    chunk_index: Optional[int] = None  # Excel / DOCX / other non-PDF/PPT (0-based chunk/row index)
    coordinates: Optional[list[float]] = None  # PDF bounding-box [x1, x2, y1, y2]
    snippet: str = ""
    image_url: Optional[str] = None
    document_url: Optional[str] = None


class QuestionResult(BaseModel):
    question_serial_no: int | str
    question: str
    vendor_response: str = ""
    vendor_comment: str = ""
    ai_response: str = ""  # Yes / No / N/A
    details: str = ""
    references: list[Reference] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Task models
# ---------------------------------------------------------------------------

class TaskStatus(BaseModel):
    task_id: str
    state: TaskState = TaskState.PENDING
    pipeline_stage: PipelineStage = PipelineStage.IDLE
    progress_message: str = ""
    total_questions: int = 0
    questions_processed: int = 0
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    error: Optional[str] = None

    # RAGFlow resource IDs (populated as the pipeline progresses)
    dataset_id: Optional[str] = None
    dataset_ids: list[str] = Field(default_factory=list)
    chat_id: Optional[str] = None
    session_id: Optional[str] = None
    document_ids: list[str] = Field(default_factory=list)

    # Per-document parsing status
    document_statuses: list[DocumentStatus] = Field(default_factory=list)


class TaskListResponse(BaseModel):
    tasks: list[TaskStatus]
    total: int
    page: int
    page_size: int
    total_pages: int


class TaskResultResponse(BaseModel):
    task_id: str
    state: TaskState
    total_questions: int = 0
    questions_processed: int = 0
    results: list[QuestionResult] = Field(default_factory=list)
    page: int = 1
    page_size: int = 50
    total_pages: int = 1

    # RAGFlow resource IDs
    dataset_id: Optional[str] = None
    dataset_ids: list[str] = Field(default_factory=list)
    chat_id: Optional[str] = None
    session_id: Optional[str] = None
    document_ids: list[str] = Field(default_factory=list)

    # Per-document parsing status
    document_statuses: list[DocumentStatus] = Field(default_factory=list)


class TaskEvent(BaseModel):
    id: int
    task_id: str
    event_type: str
    state: Optional[TaskState] = None
    pipeline_stage: Optional[PipelineStage] = None
    message: str = ""
    error: Optional[str] = None
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class TaskEventListResponse(BaseModel):
    task_id: str
    events: list[TaskEvent] = Field(default_factory=list)
    total: int
    page: int
    page_size: int
    total_pages: int


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class AssessmentRequest(BaseModel):
    """Body for the /assess endpoint (non-file fields)."""
    dataset_name: Optional[str] = None
    chat_name: Optional[str] = None
    question_id_column: Optional[str] = None
    question_column: Optional[str] = None
    vendor_response_column: Optional[str] = None
    vendor_comment_column: Optional[str] = None
    process_vendor_response: bool = False
    dataset_options: dict = Field(default_factory=dict)
    chat_options: dict = Field(default_factory=dict)


class SessionCreateResponse(BaseModel):
    """Response returned when a new assessment session is created (two-phase flow)."""
    task_id: str
    dataset_id: str
    state: TaskState = TaskState.AWAITING_DOCUMENTS
    message: str = "Session created. Upload evidence documents then start the assessment."


class DocumentUploadResponse(BaseModel):
    """Response after uploading documents to an assessment session."""
    task_id: str
    dataset_id: str
    uploaded_document_ids: list[str] = Field(default_factory=list)
    total_documents: int = 0
    message: str = ""


# ---------------------------------------------------------------------------
# RAGFlow-related internal models
# ---------------------------------------------------------------------------

class RagflowContext(BaseModel):
    """Tracks RAGFlow resource IDs created for one assessment task."""
    dataset_id: str = ""
    dataset_ids: list[str] = Field(default_factory=list)
    document_ids: list[str] = Field(default_factory=list)
    file_hashes: dict[str, str] = Field(default_factory=dict)  # Maps file hash -> document_id
    chat_id: str = ""
    session_id: str = ""


class TaskRecord(BaseModel):
    """Full internal record kept in the in-memory store."""
    task_id: str
    status: TaskStatus
    ragflow: RagflowContext = Field(default_factory=RagflowContext)
    questions: list[dict[str, Any]] = Field(default_factory=list)
    results: list[QuestionResult] = Field(default_factory=list)
    document_statuses: list[DocumentStatus] = Field(default_factory=list)
