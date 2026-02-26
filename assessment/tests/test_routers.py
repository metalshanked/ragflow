from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient

from assessment import routers


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
                    "/api/v1/ragflow/chats?page=2&page_size=5",
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
                resp = client.get("/api/v1/ragflow/datasets?page=3&page_size=10&name=abc")

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
                resp = client.get("/api/v1/ragflow/datasets?name=missing")

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
                resp = client.get("/api/v1/ragflow/datasets/ds-1/documents?page=2&page_size=2")

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
                    "/api/v1/ragflow/datasets",
                    json={"ids": ["ds-1"]},
                )
                doc_resp = client.request(
                    "DELETE",
                    "/api/v1/ragflow/datasets/ds-1/documents",
                    json={"ids": ["doc-1"]},
                )

            assert ds_resp.status_code == 200
            assert ds_resp.json() == {"message": "Datasets deleted"}
            assert doc_resp.status_code == 200
            assert doc_resp.json() == {"message": "Documents deleted"}
            assert mocked.await_count == 2


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
