"""Tests for passthrough of user-supplied dataset/chat option fields."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import assessment.services as services
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


@patch("assessment.services._process_questions", new_callable=AsyncMock)
@patch("assessment.services._update_status", new_callable=AsyncMock)
@patch("assessment.services.get_task", new_callable=AsyncMock)
@patch("assessment.services.RagflowClient")
def test_run_assessment_merges_default_and_runtime_dataset_options(
    MockClient,
    mock_get_task,
    mock_update_status,
    mock_process_questions,
):
    del mock_update_status
    mock_process_questions.return_value = 0

    record = _make_record("task-extra-003")
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

    default_opts = {
        "permission": "me",
        "parser_config": {
            "enable_metadata": False,
            "auto_keywords": 2,
            "raptor": {"use_raptor": False, "max_token": 128},
        },
    }
    runtime_opts = {
        "permission": "team",
        "parser_config": {
            "enable_metadata": True,
            "raptor": {"max_token": 512},
        },
    }

    with patch.object(services.settings, "default_dataset_options", default_opts):
        _run(
            run_assessment(
                task_id="task-extra-003",
                questions=record.questions,
                evidence_files=[("evidence.pdf", b"file-bytes")],
                dataset_opts=runtime_opts,
            )
        )

    _, dataset_kwargs = mock_client.ensure_dataset.await_args
    assert dataset_kwargs["permission"] == "team"
    assert dataset_kwargs["parser_config"]["enable_metadata"] is True
    assert dataset_kwargs["parser_config"]["auto_keywords"] == 2
    assert dataset_kwargs["parser_config"]["raptor"]["use_raptor"] is False
    assert dataset_kwargs["parser_config"]["raptor"]["max_token"] == 512


@patch("assessment.services._process_questions", new_callable=AsyncMock)
@patch("assessment.services._update_status", new_callable=AsyncMock)
@patch("assessment.services.get_task", new_callable=AsyncMock)
@patch("assessment.services.RagflowClient")
def test_run_assessment_merges_default_and_runtime_chat_options(
    MockClient,
    mock_get_task,
    mock_update_status,
    mock_process_questions,
):
    del mock_update_status
    mock_process_questions.return_value = 0

    record = _make_record("task-extra-003b")
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

    default_opts = {
        "llm": {"temperature": 0.1, "max_tokens": 512},
        "prompt": {"top_n": 8, "quote": True},
    }
    runtime_opts = {
        "llm": {"temperature": 0.2},
        "prompt": {"top_n": 12},
        "custom_chat_flag": "yes",
    }

    with patch.object(services.settings, "default_chat_options", default_opts):
        _run(
            run_assessment(
                task_id="task-extra-003b",
                questions=record.questions,
                evidence_files=[("evidence.pdf", b"file-bytes")],
                chat_opts=runtime_opts,
            )
        )

    _, chat_kwargs = mock_client.ensure_chat.await_args
    assert chat_kwargs["llm"]["temperature"] == 0.2
    assert chat_kwargs["llm"]["max_tokens"] == 512
    assert chat_kwargs["prompt"]["top_n"] == 12
    assert chat_kwargs["prompt"]["quote"] is True
    assert chat_kwargs["custom_chat_flag"] == "yes"


@patch("assessment.services.db_find_document_by_hash", new_callable=AsyncMock)
@patch("assessment.services._process_questions", new_callable=AsyncMock)
@patch("assessment.services._update_status", new_callable=AsyncMock)
@patch("assessment.services.get_task", new_callable=AsyncMock)
@patch("assessment.services.RagflowClient")
def test_run_assessment_reuses_existing_dataset_docs_by_hash(
    MockClient,
    mock_get_task,
    mock_update_status,
    mock_process_questions,
    mock_find_by_hash,
):
    del mock_update_status
    mock_process_questions.return_value = 0

    record = _make_record("task-extra-004")
    mock_get_task.return_value = record

    mock_client = AsyncMock()
    mock_client.ensure_dataset = AsyncMock(return_value="ds-existing")
    mock_client.list_documents = AsyncMock(return_value=[
        {
            "id": "doc-existing-1",
            "name": "evidence.pdf",
            "status": "success",
            "run": "DONE",
            "progress": 1.0,
        }
    ])
    mock_client.upload_document = AsyncMock(return_value="doc-new-1")
    mock_client.start_parsing = AsyncMock()
    mock_client.wait_for_parsing = AsyncMock(return_value=[])
    mock_client.ensure_chat = AsyncMock(return_value="chat-1")
    mock_client.create_session = AsyncMock(return_value="sess-1")
    mock_client.close = AsyncMock()
    MockClient.return_value = mock_client

    mock_find_by_hash.return_value = [
        {
            "task_id": "old-task",
            "document_id": "doc-existing-1",
            "dataset_id": "ds-existing",
        }
    ]

    _run(
        run_assessment(
            task_id="task-extra-004",
            questions=record.questions,
            evidence_files=[("evidence.pdf", b"same-content")],
            dataset_name="shared-ds",
        )
    )

    _, dataset_kwargs = mock_client.ensure_dataset.await_args
    assert dataset_kwargs["reuse_existing_dataset"] is True
    mock_client.upload_document.assert_not_called()
    mock_client.start_parsing.assert_not_called()
    mock_client.wait_for_parsing.assert_not_called()
    assert record.ragflow.file_hashes
