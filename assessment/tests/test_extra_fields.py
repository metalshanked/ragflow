"""Tests for passthrough of user-supplied dataset/chat option fields."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

from assessment.models import RagflowContext, TaskRecord, TaskState, TaskStatus
from assessment.services import run_assessment, run_assessment_from_dataset


def _run(coro):
    return asyncio.run(coro)


def _make_record(task_id: str = "task-extra-001") -> TaskRecord:
    return TaskRecord(
        task_id=task_id,
        status=TaskStatus(
            task_id=task_id,
            state=TaskState.PENDING,
            total_questions=1,
        ),
        ragflow=RagflowContext(),
        questions=[{"serial_no": 1, "question": "Is there evidence?"}],
    )


@patch("assessment.services._process_questions", new_callable=AsyncMock)
@patch("assessment.services._update_status", new_callable=AsyncMock)
@patch("assessment.services.get_task", new_callable=AsyncMock)
@patch("assessment.services.RagflowClient")
def test_run_assessment_passes_dataset_and_chat_extra_options(
    MockClient,
    mock_get_task,
    mock_update_status,
    mock_process_questions,
):
    del mock_update_status  # status persistence not part of this assertion
    mock_process_questions.return_value = 0

    record = _make_record("task-extra-001")
    mock_get_task.return_value = record

    mock_client = AsyncMock()
    mock_client.ensure_dataset = AsyncMock(return_value="ds-1")
    mock_client.upload_document = AsyncMock(return_value="doc-1")
    mock_client.start_parsing = AsyncMock()
    mock_client.wait_for_parsing = AsyncMock(return_value=[
        {
            "document_id": "doc-1",
            "document_name": "evidence.pdf",
            "status": "success",
            "progress": 1.0,
            "message": "ok",
        }
    ])
    mock_client.ensure_chat = AsyncMock(return_value="chat-1")
    mock_client.create_session = AsyncMock(return_value="sess-1")
    mock_client.close = AsyncMock()
    MockClient.return_value = mock_client

    dataset_opts = {"permission": "team", "custom_dataset_flag": True}
    chat_opts = {
        "llm": {"temperature": 0.2},
        "custom_chat_flag": "yes",
    }

    _run(
        run_assessment(
            task_id="task-extra-001",
            questions=record.questions,
            evidence_files=[("evidence.pdf", b"file-bytes")],
            dataset_opts=dataset_opts,
            chat_opts=chat_opts,
        )
    )

    _, dataset_kwargs = mock_client.ensure_dataset.await_args
    assert dataset_kwargs["permission"] == "team"
    assert dataset_kwargs["custom_dataset_flag"] is True

    _, chat_kwargs = mock_client.ensure_chat.await_args
    assert chat_kwargs["llm"] == {"temperature": 0.2}
    assert chat_kwargs["custom_chat_flag"] == "yes"


@patch("assessment.services._process_questions", new_callable=AsyncMock)
@patch("assessment.services._update_status", new_callable=AsyncMock)
@patch("assessment.services.get_task", new_callable=AsyncMock)
@patch("assessment.services.RagflowClient")
def test_run_assessment_from_dataset_passes_chat_extra_options(
    MockClient,
    mock_get_task,
    mock_update_status,
    mock_process_questions,
):
    del mock_update_status
    mock_process_questions.return_value = 0

    record = _make_record("task-extra-002")
    mock_get_task.return_value = record

    mock_client = AsyncMock()
    mock_client.ensure_chat = AsyncMock(return_value="chat-ds")
    mock_client.create_session = AsyncMock(return_value="sess-ds")
    mock_client.close = AsyncMock()
    MockClient.return_value = mock_client

    chat_opts = {"prompt": {"top_n": 12}, "custom_chat": 123}
    dataset_ids = ["ds-a", "ds-b"]

    _run(
        run_assessment_from_dataset(
            task_id="task-extra-002",
            dataset_ids=dataset_ids,
            chat_opts=chat_opts,
        )
    )

    args, kwargs = mock_client.ensure_chat.await_args
    assert list(args[1]) == dataset_ids
    assert kwargs["prompt"] == {"top_n": 12}
    assert kwargs["custom_chat"] == 123

