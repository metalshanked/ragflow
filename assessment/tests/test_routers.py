from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import io
from unittest.mock import AsyncMock, MagicMock, patch
import zipfile

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient
import openpyxl

from assessment import routers
from assessment.models import RagflowContext, SessionCreateResponse, TaskRecord, TaskState, TaskStatus


@contextmanager
def override_settings(**kwargs):
    original: dict[str, object] = {}
    for key, value in kwargs.items():
        original[key] = getattr(routers.settings, key)
        setattr(routers.settings, key, value)
    try:
        yield
    finally:
        for key, value in original.items():
            setattr(routers.settings, key, value)


def _json_response(status_code: int, payload: dict, headers: dict[str, str] | None = None) -> httpx.Response:
    return httpx.Response(
        status_code=status_code,
        json=payload,
        headers=headers,
        request=httpx.Request("GET", "http://testserver/mock"),
    )


def make_app() -> FastAPI:
    app = FastAPI()
    app.include_router(routers.router)
    return app


def _make_minimal_docx_bytes() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "word/document.xml",
            """<?xml version="1.0" encoding="UTF-8"?>
            <w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
              <w:body>
                <w:p><w:r><w:t>Hello DOCX</w:t></w:r></w:p>
                <w:tbl>
                  <w:tr><w:tc><w:p><w:r><w:t>Cell A</w:t></w:r></w:p></w:tc></w:tr>
                </w:tbl>
              </w:body>
            </w:document>""",
        )
    return buffer.getvalue()


def _make_minimal_xlsx_bytes() -> bytes:
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "SheetA"
    sheet.append(["Name", "Value"])
    sheet.append(["alpha", 1])
    sheet.append(["beta", 2])
    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def _make_minimal_pptx_bytes() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "ppt/slides/slide1.xml",
            """<?xml version="1.0" encoding="UTF-8"?>
            <p:sld xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main"
                   xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main">
              <p:cSld>
                <p:spTree>
                  <p:sp>
                    <p:txBody>
                      <a:p><a:r><a:t>Slide text</a:t></a:r></a:p>
                    </p:txBody>
                  </p:sp>
                </p:spTree>
              </p:cSld>
            </p:sld>""",
        )
    return buffer.getvalue()


def test_ragflow_passthrough_forwards_request_and_response():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(jwt_secret_key=""):
            upstream = httpx.Response(
                status_code=201,
                content=b'{"ok":true}',
                headers={"content-type": "application/json", "x-upstream": "1"},
                request=httpx.Request("POST", "http://ragflow/api/v1/chats"),
            )
            mocked = AsyncMock(return_value=upstream)
            with patch("assessment.routers._request_ragflow_official", mocked):
                resp = client.post(
                    "/api/v1/native/chats?page=2&page_size=5",
                    data=b'{"name":"ds"}',
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                    },
                )

            assert resp.status_code == 201
            assert resp.json() == {"ok": True}
            assert resp.headers.get("x-upstream") == "1"

            mocked.assert_awaited_once()
            args = mocked.await_args
            assert args.args[0] == "POST"
            assert args.args[1] == "chats"
            assert args.kwargs["params"] == [("page", "2"), ("page_size", "5")]
            assert args.kwargs["content"] == b'{"name":"ds"}'
            assert args.kwargs["timeout"] == 300.0
            assert args.kwargs["extra_headers"] == {
                "content-type": "application/json",
                "accept": "application/json",
            }


def test_list_datasets_uses_passthrough_helper():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(jwt_secret_key=""):
            mocked = AsyncMock(
                return_value=_json_response(
                    200,
                    {"code": 0, "data": [{"id": "ds-1", "name": "A"}]},
                )
            )
            with patch("assessment.routers._request_ragflow_official", mocked):
                resp = client.get("/api/v1/native/datasets?page=3&page_size=10&name=abc")

            assert resp.status_code == 200
            assert resp.json() == {
                "items": [{"id": "ds-1", "name": "A"}],
                "total": None,
                "page": 3,
                "page_size": 10,
            }
            mocked.assert_awaited_once_with(
                "GET",
                "datasets",
                params={"page": 3, "page_size": 10, "name": "abc"},
            )


def test_list_datasets_name_not_found_permission_error_returns_empty():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(jwt_secret_key=""):
            mocked = AsyncMock(
                return_value=_json_response(
                    403,
                    {"code": 403, "message": "lacks permission"},
                )
            )
            with patch("assessment.routers._request_ragflow_official", mocked):
                resp = client.get("/api/v1/native/datasets?name=missing")

            assert resp.status_code == 200
            assert resp.json() == {
                "items": [],
                "total": 0,
                "page": 1,
                "page_size": 100,
            }


def test_list_documents_uses_passthrough_and_normalizes_status():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(jwt_secret_key=""):
            mocked = AsyncMock(
                return_value=_json_response(
                    200,
                    {
                        "code": 0,
                        "data": {
                            "docs": [
                                {"id": "d1", "run": "FAIL", "progress": 0.1, "words": 10},
                                {"id": "d2", "run": "RUNNING", "progress": 0.5, "words": 20},
                                {"id": "d3", "run": "DONE", "progress": 1.0, "words": 30},
                            ],
                            "total": 3,
                        },
                    },
                )
            )
            with patch("assessment.routers._request_ragflow_official", mocked):
                resp = client.get("/api/v1/native/datasets/ds-1/documents?page=2&page_size=2")

            assert resp.status_code == 200
            payload = resp.json()
            assert payload["page"] == 2
            assert payload["page_size"] == 2
            assert payload["total"] == 3
            assert payload["items"][0]["status"] == "failed"
            assert payload["items"][1]["status"] == "running"
            assert payload["items"][2]["status"] == "success"
            assert "words" not in payload["items"][0]


def test_delete_dataset_and_documents_use_passthrough_helper():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(jwt_secret_key=""):
            mocked = AsyncMock(return_value=_json_response(200, {"code": 0, "data": True}))
            with patch("assessment.routers._request_ragflow_official", mocked):
                ds_resp = client.request(
                    "DELETE",
                    "/api/v1/native/datasets",
                    json={"ids": ["ds-1"]},
                )
                doc_resp = client.request(
                    "DELETE",
                    "/api/v1/native/datasets/ds-1/documents",
                    json={"ids": ["doc-1"]},
                )

            assert ds_resp.status_code == 200
            assert ds_resp.json() == {"message": "Datasets deleted"}
            assert doc_resp.status_code == 200
            assert doc_resp.json() == {"message": "Documents deleted"}
            assert mocked.await_count == 2


def test_render_document_renders_docx_to_html():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(jwt_secret_key=""):
            mocked = AsyncMock(
                return_value=httpx.Response(
                    status_code=200,
                    content=_make_minimal_docx_bytes(),
                    headers={"content-type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
                    request=httpx.Request("GET", "http://testserver/mock"),
                )
            )
            with patch("assessment.routers._fetch_ragflow_resource", mocked):
                resp = client.get("/api/v1/proxy/document/doc-1/render?filename=test.docx")

            assert resp.status_code == 200
            assert resp.headers["content-type"].startswith("text/html")
            assert "Hello DOCX" in resp.text
            assert "<table>" in resp.text


def test_render_document_renders_xlsx_to_html():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(jwt_secret_key=""):
            mocked = AsyncMock(
                return_value=httpx.Response(
                    status_code=200,
                    content=_make_minimal_xlsx_bytes(),
                    headers={"content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
                    request=httpx.Request("GET", "http://testserver/mock"),
                )
            )
            with patch("assessment.routers._fetch_ragflow_resource", mocked):
                resp = client.get("/api/v1/proxy/document/doc-2/render?filename=test.xlsx")

            assert resp.status_code == 200
            assert resp.headers["content-type"].startswith("text/html")
            assert "SheetA" in resp.text
            assert "alpha" in resp.text


def test_render_document_rejects_unsupported_extension():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(jwt_secret_key=""):
            mocked = AsyncMock(
                return_value=httpx.Response(
                    status_code=200,
                    content=b"binary",
                    headers={"content-type": "application/octet-stream"},
                    request=httpx.Request("GET", "http://testserver/mock"),
                )
            )
            with patch("assessment.routers._fetch_ragflow_resource", mocked):
                resp = client.get("/api/v1/proxy/document/doc-3/render?filename=test.pdf")

            assert resp.status_code == 415
            assert resp.json()["detail"] == "Document type is not renderable in the built-in UI"


def test_task_events_endpoint_returns_paginated_history():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(jwt_secret_key=""):
            mock_get_task = AsyncMock(return_value=object())
            mock_list_events = AsyncMock(
                return_value=(
                    [
                        {
                            "id": 1,
                            "task_id": "task-1",
                            "event_type": "task_created",
                            "state": "pending",
                            "pipeline_stage": "idle",
                            "message": "Task created",
                            "error": None,
                            "payload": {"total_questions": 1},
                            "created_at": datetime(2026, 1, 1, 0, 0, 0),
                        }
                    ],
                    1,
                )
            )
            with patch("assessment.routers.get_task", mock_get_task), patch(
                "assessment.routers.list_task_events", mock_list_events
            ):
                resp = client.get("/api/v1/assessments/task-1/events?page=1&page_size=50")

            assert resp.status_code == 200
            body = resp.json()
            assert body["task_id"] == "task-1"
            assert body["total"] == 1
            assert body["page"] == 1
            assert body["page_size"] == 50
            assert body["total_pages"] == 1
            assert body["events"][0]["event_type"] == "task_created"
            mock_get_task.assert_awaited_once_with("task-1")
            mock_list_events.assert_awaited_once_with("task-1", 1, 50)


def test_delete_task_endpoint_returns_deleted_payload():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(jwt_secret_key=""):
            mock_delete = AsyncMock(
                return_value={
                    "task_id": "task-1",
                    "deleted": True,
                    "deleted_chat_id": "chat-1",
                    "deleted_dataset_ids": ["ds-1"],
                }
            )
            with patch("assessment.routers.delete_task_and_resources", mock_delete):
                resp = client.request("DELETE", "/api/v1/assessments/task-1")

            assert resp.status_code == 200
            assert resp.json() == {
                "message": "Task deleted",
                "task_id": "task-1",
                "deleted": True,
                "deleted_chat_id": "chat-1",
                "deleted_dataset_ids": ["ds-1"],
            }
            mock_delete.assert_awaited_once()


def test_delete_task_endpoint_maps_conflict_state():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(jwt_secret_key=""):
            mock_delete = AsyncMock(side_effect=ValueError("Cannot delete task in state 'processing'. Wait until the task finishes or fails."))
            with patch("assessment.routers.delete_task_and_resources", mock_delete):
                resp = client.request("DELETE", "/api/v1/assessments/task-1")

            assert resp.status_code == 409
            assert "Cannot delete task in state" in resp.json()["detail"]


def test_retry_task_endpoint_queues_background_retry():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(jwt_secret_key=""):
            mock_claim = AsyncMock(
                return_value=type(
                    "_RetryRecord",
                    (),
                    {
                        "status": TaskStatus(task_id="task-1", state=TaskState.PARSING),
                        "execution": type("_Exec", (), {"workflow": "session"})(),
                        "ragflow": type("_Rag", (), {"dataset_id": "ds-1", "document_ids": ["doc-1"]})(),
                    },
                )()
            )
            mock_retry = AsyncMock()
            with patch("assessment.routers.claim_task_retry", mock_claim), patch(
                "assessment.routers.retry_task", mock_retry
            ):
                resp = client.post("/api/v1/assessments/task-1/retry")

            assert resp.status_code == 202
            assert resp.json()["task_id"] == "task-1"
            mock_claim.assert_awaited_once()
            mock_retry.assert_awaited_once()


def test_retry_failed_questions_endpoint_queues_background_retry():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(jwt_secret_key=""):
            mock_claim = AsyncMock(
                return_value=type(
                    "_RetryRecord",
                    (),
                    {
                        "status": TaskStatus(task_id="task-1", state=TaskState.PROCESSING),
                        "ragflow": type("_Rag", (), {"dataset_id": "ds-1", "document_ids": ["doc-1"]})(),
                    },
                )()
            )
            mock_retry = AsyncMock()
            with patch("assessment.routers.claim_task_retry", mock_claim), patch(
                "assessment.routers.retry_failed_questions_for_task", mock_retry
            ):
                resp = client.post("/api/v1/assessments/task-1/retry-failed-questions")

            assert resp.status_code == 202
            assert resp.json()["task_id"] == "task-1"
            mock_claim.assert_awaited_once()
            mock_retry.assert_awaited_once()


def test_start_assessment_defaults_reuse_existing_dataset_true():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(jwt_secret_key=""):
            mock_parse = MagicMock(return_value=[{"serial_no": 1, "question": "Q1"}])
            mock_create_task = AsyncMock(
                return_value=type(
                    "_R",
                    (),
                    {
                        "task_id": "task-123",
                        "status": TaskStatus(task_id="task-123", state=TaskState.PENDING, total_questions=1),
                    },
                )()
            )
            mock_run = AsyncMock()

            with patch("assessment.routers.parse_questions_excel", mock_parse), patch(
                "assessment.routers.create_task", mock_create_task
            ), patch("assessment.routers.run_assessment", mock_run):
                resp = client.post(
                    "/api/v1/assessments",
                    files={
                        "questions_file": ("q.xlsx", b"xlsx-bytes", "application/octet-stream"),
                        "evidence_files": ("evidence.pdf", b"pdf-bytes", "application/pdf"),
                    },
                    data={"dataset_name": "shared-ds"},
                )

            assert resp.status_code == 202
            args = mock_run.await_args.args
            assert args[3] == "shared-ds"
            assert args[5] is True


def test_start_assessment_passes_fail_on_document_parse_issue_true():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(jwt_secret_key=""):
            mock_parse = MagicMock(return_value=[{"serial_no": 1, "question": "Q1"}])
            mock_create_task = AsyncMock(
                return_value=type(
                    "_R",
                    (),
                    {
                        "task_id": "task-123",
                        "status": TaskStatus(task_id="task-123", state=TaskState.PENDING, total_questions=1),
                    },
                )()
            )
            mock_run = AsyncMock()

            with patch("assessment.routers.parse_questions_excel", mock_parse), patch(
                "assessment.routers.create_task", mock_create_task
            ), patch("assessment.routers.run_assessment", mock_run):
                resp = client.post(
                    "/api/v1/assessments",
                    files={
                        "questions_file": ("q.xlsx", b"xlsx-bytes", "application/octet-stream"),
                        "evidence_files": ("evidence.pdf", b"pdf-bytes", "application/pdf"),
                    },
                    data={"fail_on_document_parse_issue": "true"},
                )

            assert resp.status_code == 202
            execution = mock_create_task.await_args.kwargs["execution"]
            assert execution.fail_on_document_parse_issue is True
            args = mock_run.await_args.args
            assert args[10] is True


def test_start_assessment_allows_no_evidence_when_processing_vendor_responses():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(jwt_secret_key=""):
            mock_parse = MagicMock(return_value=[{"serial_no": 1, "question": "Q1"}])
            mock_create_task = AsyncMock(
                return_value=type(
                    "_R",
                    (),
                    {
                        "task_id": "task-123",
                        "status": TaskStatus(task_id="task-123", state=TaskState.PENDING, total_questions=1),
                    },
                )()
            )
            mock_run = AsyncMock()

            with patch("assessment.routers.parse_questions_excel", mock_parse), patch(
                "assessment.routers.create_task", mock_create_task
            ), patch("assessment.routers.run_assessment", mock_run):
                resp = client.post(
                    "/api/v1/assessments",
                    files={"questions_file": ("q.xlsx", b"xlsx-bytes", "application/octet-stream")},
                    data={"process_vendor_response": "true"},
                )

            assert resp.status_code == 202
            args = mock_run.await_args.args
            assert args[2] == []


def test_start_assessment_requires_evidence_when_vendor_processing_disabled():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(jwt_secret_key=""):
            mock_parse = MagicMock(return_value=[{"serial_no": 1, "question": "Q1"}])
            with patch("assessment.routers.parse_questions_excel", mock_parse):
                resp = client.post(
                    "/api/v1/assessments",
                    files={"questions_file": ("q.xlsx", b"xlsx-bytes", "application/octet-stream")},
                    data={"process_vendor_response": "false"},
                )

            assert resp.status_code == 400
            assert "evidence document" in resp.json()["detail"].lower()


def test_create_session_endpoint_accepts_reuse_existing_dataset_false():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(jwt_secret_key=""):
            mock_parse = MagicMock(return_value=[{"serial_no": 1, "question": "Q1"}])
            mock_create_session = AsyncMock(
                return_value=SessionCreateResponse(task_id="task-1", dataset_id="ds-1")
            )

            with patch("assessment.routers.parse_questions_excel", mock_parse), patch(
                "assessment.routers.create_session", mock_create_session
            ):
                resp = client.post(
                    "/api/v1/assessments/sessions",
                    files={"questions_file": ("q.xlsx", b"xlsx-bytes", "application/octet-stream")},
                    data={
                        "dataset_name": "shared-ds",
                        "reuse_exisiting_dataset": "false",
                    },
                )

            assert resp.status_code == 201
            mock_create_session.assert_awaited_once()
            args = mock_create_session.await_args.args
            assert args[1] == "shared-ds"
            assert args[2] is False


def test_start_session_assessment_passes_fail_on_document_parse_issue_true():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(jwt_secret_key=""):
            record = TaskRecord(
                task_id="task-1",
                status=TaskStatus(task_id="task-1", state=TaskState.PARSING, total_questions=1),
                ragflow=RagflowContext(dataset_id="ds-1", document_ids=["doc-1"]),
            )
            mock_claim = AsyncMock(return_value=record)
            mock_run = AsyncMock()
            mock_task_event = AsyncMock()
            mock_audit = AsyncMock()

            with patch("assessment.routers.claim_session_start", mock_claim), patch(
                "assessment.routers.run_assessment_for_session", mock_run
            ), patch("assessment.routers._task_event", mock_task_event), patch(
                "assessment.routers._audit", mock_audit
            ):
                resp = client.post(
                    "/api/v1/assessments/sessions/task-1/start",
                    data={"fail_on_document_parse_issue": "true"},
                )

            assert resp.status_code == 202
            assert mock_claim.await_args.kwargs["allow_no_documents"] is False
            args = mock_run.await_args.args
            assert args[6] is True


def test_start_session_assessment_allows_no_documents_when_processing_vendor_responses():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(jwt_secret_key=""):
            record = TaskRecord(
                task_id="task-1",
                status=TaskStatus(task_id="task-1", state=TaskState.PARSING, total_questions=1),
                ragflow=RagflowContext(dataset_id="ds-1", document_ids=[]),
            )
            mock_claim = AsyncMock(return_value=record)
            mock_run = AsyncMock()
            mock_task_event = AsyncMock()
            mock_audit = AsyncMock()

            with patch("assessment.routers.claim_session_start", mock_claim), patch(
                "assessment.routers.run_assessment_for_session", mock_run
            ), patch("assessment.routers._task_event", mock_task_event), patch(
                "assessment.routers._audit", mock_audit
            ):
                resp = client.post(
                    "/api/v1/assessments/sessions/task-1/start",
                    data={"process_vendor_response": "true"},
                )

            assert resp.status_code == 202
            assert mock_claim.await_args.kwargs["allow_no_documents"] is True
