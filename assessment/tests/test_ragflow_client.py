"""
Tests for RagflowClient – focusing on response parsing and the
list_documents / wait_for_parsing logic that caused the original bug.
"""

from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# ---------------------------------------------------------------------------
# We need to mock settings before importing the client so that the module
# doesn't try to read real env vars.
# ---------------------------------------------------------------------------
_fake_settings = MagicMock()
_fake_settings.ragflow_base_url = "http://test:9380"
_fake_settings.ragflow_api_key = "test-key"
_fake_settings.verify_ssl = True
_fake_settings.ssl_ca_cert = ""
_fake_settings.polling_interval_seconds = 0.01
_fake_settings.document_parse_timeout_seconds = 0.1

import sys
import types

# Provide a minimal fake httpx so the import doesn't fail if httpx isn't installed
if "httpx" not in sys.modules:
    _httpx = types.ModuleType("httpx")

    class _FakeTimeout:
        def __init__(self, **kw):
            pass

    class _FakeAsyncClient:
        def __init__(self, **kw):
            self.is_closed = False

        async def request(self, *a, **kw):
            pass

        async def aclose(self):
            self.is_closed = True

    _httpx.Timeout = _FakeTimeout
    _httpx.AsyncClient = _FakeAsyncClient
    sys.modules["httpx"] = _httpx

with patch.dict("sys.modules", {"assessment.config": MagicMock(settings=_fake_settings)}):
    from assessment.ragflow_client import RagflowClient


def _run(coro):
    """Helper to run a coroutine in tests."""
    return asyncio.run(coro)


class TestListDocuments(unittest.TestCase):
    """list_documents must correctly unwrap the RAGFlow response."""

    def _make_client(self):
        with patch.object(RagflowClient, "__init__", lambda self, **kw: None):
            c = object.__new__(RagflowClient)
            c.base_url = "http://test:9380"
            c.headers = {"Authorization": "Bearer test"}
            c._client = None
            return c

    def test_data_is_dict_with_docs(self):
        """Standard RAGFlow response: data = {docs: [...], total: N}."""
        client = self._make_client()
        client._request = AsyncMock(return_value={
            "code": 0,
            "data": {
                "docs": [
                    {"id": "doc1", "name": "a.pdf", "run": "DONE", "progress": 1.0},
                    {"id": "doc2", "name": "b.pdf", "run": "RUNNING", "progress": 0.5},
                ],
                "total": 2,
            },
        })
        docs = _run(client.list_documents("ds1"))
        self.assertEqual(len(docs), 2)
        self.assertEqual(docs[0]["id"], "doc1")
        self.assertEqual(docs[1]["id"], "doc2")

    def test_data_is_list_fallback(self):
        """Defensive: if data is already a list, return as-is."""
        client = self._make_client()
        client._request = AsyncMock(return_value={
            "code": 0,
            "data": [
                {"id": "doc1", "name": "a.pdf"},
            ],
        })
        docs = _run(client.list_documents("ds1"))
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["id"], "doc1")

    def test_data_is_empty_dict(self):
        """Edge case: data = {} → no docs key → return []."""
        client = self._make_client()
        client._request = AsyncMock(return_value={"code": 0, "data": {}})
        docs = _run(client.list_documents("ds1"))
        self.assertEqual(docs, [])

    def test_data_missing(self):
        """Edge case: no data key at all → return []."""
        client = self._make_client()
        client._request = AsyncMock(return_value={"code": 0})
        docs = _run(client.list_documents("ds1"))
        self.assertEqual(docs, [])

    def test_list_documents_cleans_response(self):
        """Verify that words is removed and status is added."""
        client = self._make_client()
        client._request = AsyncMock(return_value={
            "code": 0,
            "data": {
                "docs": [
                    {"id": "doc1", "words": 0, "run": "FAIL", "progress": 0.2},
                    {"id": "doc2", "words": 100, "run": "1", "progress": 0.5},
                    {"id": "doc3", "words": 0, "run": "", "progress": 1.0},
                ],
                "total": 3,
            },
        })
        docs = _run(client.list_documents("ds1"))
        self.assertEqual(len(docs), 3)
        
        # doc1: failed
        self.assertNotIn("words", docs[0])
        self.assertEqual(docs[0]["status"], "failed")
        
        # doc2: running
        self.assertNotIn("words", docs[1])
        self.assertEqual(docs[1]["status"], "running")
        
        # doc3: success
        self.assertNotIn("words", docs[2])
        self.assertEqual(docs[2]["status"], "success")


class TestRagflowBugWorkarounds(unittest.TestCase):
    """Test that RagflowClient handles specific RAGFlow API bugs."""

    def _make_client(self):
        with patch.object(RagflowClient, "__init__", lambda self, **kw: None):
            c = object.__new__(RagflowClient)
            c.base_url = "http://test:9380"
            c.headers = {"Authorization": "Bearer test"}
            c._client = None
            return c

    def test_list_datasets_lacks_permission_error(self):
        """list_datasets should return [] if 'lacks permission' error occurs (RAGFlow bug)."""
        client = self._make_client()
        client._request = AsyncMock(side_effect=RuntimeError("RAGFlow error: User '...' lacks permission for dataset '...'"))
        
        result = _run(client.list_datasets(name="missing_ds"))
        self.assertEqual(result, [])

    def test_list_datasets_other_error_raises(self):
        """list_datasets should still raise for other RuntimeErrors."""
        client = self._make_client()
        client._request = AsyncMock(side_effect=RuntimeError("Some other error"))
        
        with self.assertRaises(RuntimeError) as cm:
            _run(client.list_datasets(name="any"))
        self.assertEqual(str(cm.exception), "Some other error")

    def test_list_chats_doesnt_exist_error(self):
        """list_chats should return [] if 'doesn't exist' error occurs (RAGFlow bug)."""
        client = self._make_client()
        client._request = AsyncMock(side_effect=RuntimeError("RAGFlow error: The chat doesn't exist"))
        
        result = _run(client.list_chats(name="missing_chat"))
        self.assertEqual(result, [])

    def test_data_is_string_returns_empty(self):
        """Bug scenario: if data is a string, return [] instead of crashing."""
        client = self._make_client()
        client._request = AsyncMock(return_value={"code": 0, "data": "unexpected"})
        docs = _run(client.list_documents("ds1"))
        self.assertEqual(docs, [])


class TestWaitForParsing(unittest.TestCase):
    """wait_for_parsing should iterate documents correctly."""

    def _make_client(self):
        with patch.object(RagflowClient, "__init__", lambda self, **kw: None):
            c = object.__new__(RagflowClient)
            c.base_url = "http://test:9380"
            c.headers = {"Authorization": "Bearer test"}
            c._client = None
            return c

    def test_all_done_immediately(self):
        """All documents already parsed → returns statuses immediately."""
        client = self._make_client()
        client.list_documents = AsyncMock(return_value=[
            {"id": "d1", "name": "a.pdf", "run": "DONE", "progress": 1.0},
            {"id": "d2", "name": "b.pdf", "run": "DONE", "progress": 1.0},
        ])
        results = _run(client.wait_for_parsing("ds1", ["d1", "d2"], poll_interval=0.01, timeout=1))
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["status"], "success")
        self.assertEqual(results[1]["status"], "success")

    def test_parsing_failure_returns_failed_status(self):
        """A document in FAIL state should return a 'failed' status dict."""
        client = self._make_client()
        client.list_documents = AsyncMock(return_value=[
            {"id": "d1", "name": "a.pdf", "run": "FAIL", "progress": 0, "progress_msg": "bad format"},
        ])
        results = _run(client.wait_for_parsing("ds1", ["d1"], poll_interval=0.01, timeout=1))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "failed")
        self.assertIn("bad format", results[0]["message"])
        self.assertEqual(results[0]["document_name"], "a.pdf")

    def test_document_not_found_returns_not_found_status(self):
        """Document ID not in listing returns 'not_found' status."""
        client = self._make_client()
        client.list_documents = AsyncMock(return_value=[
            {"id": "d1", "name": "a.pdf", "run": "DONE", "progress": 1.0},
        ])
        results = _run(client.wait_for_parsing("ds1", ["d1", "missing_id"], poll_interval=0.01, timeout=1))
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["status"], "success")
        self.assertEqual(results[1]["status"], "not_found")
        self.assertIn("not found", results[1]["message"])

    def test_timeout_returns_timeout_status(self):
        """Documents never finish → returns 'timeout' status."""
        client = self._make_client()
        client.list_documents = AsyncMock(return_value=[
            {"id": "d1", "name": "a.pdf", "run": "RUNNING", "progress": 0.3},
        ])
        results = _run(client.wait_for_parsing("ds1", ["d1"], poll_interval=0.01, timeout=0.05))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "timeout")
        self.assertIn("timed out", results[0]["message"])

    def test_progress_completes_after_retries(self):
        """Documents finish after a few polls → should return success."""
        client = self._make_client()
        call_count = 0

        async def _list_docs(dataset_id, **kw):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return [{"id": "d1", "name": "a.pdf", "run": "RUNNING", "progress": 0.5}]
            return [{"id": "d1", "name": "a.pdf", "run": "DONE", "progress": 1.0}]

        client.list_documents = _list_docs
        results = _run(client.wait_for_parsing("ds1", ["d1"], poll_interval=0.01, timeout=1))
        self.assertGreaterEqual(call_count, 3)
        self.assertEqual(results[0]["status"], "success")

    def test_dict_data_response_integration(self):
        """End-to-end: list_documents receives dict data → wait_for_parsing works."""
        client = self._make_client()
        client._request = AsyncMock(return_value={
            "code": 0,
            "data": {
                "docs": [
                    {"id": "d1", "name": "a.pdf", "run": "DONE", "progress": 1.0},
                ],
                "total": 1,
            },
        })
        results = _run(client.wait_for_parsing("ds1", ["d1"], poll_interval=0.01, timeout=1))
        self.assertEqual(results[0]["status"], "success")

    def test_partial_failure_mixed_statuses(self):
        """Mix of success and failure → returns per-doc statuses without raising."""
        client = self._make_client()
        client.list_documents = AsyncMock(return_value=[
            {"id": "d1", "name": "a.pdf", "run": "DONE", "progress": 1.0},
            {"id": "d2", "name": "b.xlsx", "run": "FAIL", "progress": 0.0, "progress_msg": "unsupported"},
            {"id": "d3", "name": "c.docx", "run": "DONE", "progress": 1.0},
        ])
        results = _run(client.wait_for_parsing("ds1", ["d1", "d2", "d3"], poll_interval=0.01, timeout=1))
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0]["status"], "success")
        self.assertEqual(results[1]["status"], "failed")
        self.assertEqual(results[1]["document_name"], "b.xlsx")
        self.assertIn("unsupported", results[1]["message"])
        self.assertEqual(results[2]["status"], "success")


class TestParseYesNo(unittest.TestCase):
    """parse_yes_no should extract verdict and details."""

    def test_yes(self):
        verdict, details = RagflowClient.parse_yes_no("Answer: Yes\nDetails: All good")
        self.assertEqual(verdict, "Yes")
        self.assertIn("All good", details)

    def test_no(self):
        verdict, details = RagflowClient.parse_yes_no("Answer: No\nDetails: Missing evidence")
        self.assertEqual(verdict, "No")
        self.assertIn("Missing evidence", details)

    def test_na(self):
        verdict, details = RagflowClient.parse_yes_no("Answer: N/A\nDetails: Not applicable")
        self.assertEqual(verdict, "N/A")

    def test_no_match(self):
        verdict, details = RagflowClient.parse_yes_no("Some random text without format")
        self.assertEqual(verdict, "N/A")


class TestEnsureDataset(unittest.TestCase):
    """ensure_dataset should delete existing datasets with the same name."""

    def _make_client(self):
        with patch.object(RagflowClient, "__init__", lambda self, **kw: None):
            c = object.__new__(RagflowClient)
            c.base_url = "http://test:9380"
            c.headers = {"Authorization": "Bearer test"}
            c._client = None
            return c

    def test_no_existing_dataset(self):
        """No existing dataset → just creates a new one."""
        client = self._make_client()
        client.list_datasets = AsyncMock(return_value=[])
        client.delete_dataset = AsyncMock()
        client.create_dataset = AsyncMock(return_value="new-id")
        result = _run(client.ensure_dataset("my-ds"))
        self.assertEqual(result, "new-id")
        client.delete_dataset.assert_not_called()
        client.create_dataset.assert_awaited_once_with("my-ds")

    def test_existing_dataset_deleted(self):
        """Existing dataset with same name → deleted then re-created."""
        client = self._make_client()
        client.list_datasets = AsyncMock(return_value=[
            {"id": "old-id", "name": "my-ds"},
        ])
        client.delete_dataset = AsyncMock()
        client.create_dataset = AsyncMock(return_value="new-id")
        result = _run(client.ensure_dataset("my-ds"))
        self.assertEqual(result, "new-id")
        client.delete_dataset.assert_awaited_once_with("old-id")

    def test_different_name_not_deleted(self):
        """Datasets with different names should not be deleted."""
        client = self._make_client()
        client.list_datasets = AsyncMock(return_value=[
            {"id": "other-id", "name": "other-ds"},
        ])
        client.delete_dataset = AsyncMock()
        client.create_dataset = AsyncMock(return_value="new-id")
        result = _run(client.ensure_dataset("my-ds"))
        self.assertEqual(result, "new-id")
        client.delete_dataset.assert_not_called()


class TestEnsureChat(unittest.TestCase):
    """ensure_chat should delete existing chats with the same name."""

    def _make_client(self):
        with patch.object(RagflowClient, "__init__", lambda self, **kw: None):
            c = object.__new__(RagflowClient)
            c.base_url = "http://test:9380"
            c.headers = {"Authorization": "Bearer test"}
            c._client = None
            return c

    def test_no_existing_chat(self):
        """No existing chat → just creates a new one."""
        client = self._make_client()
        client.list_chats = AsyncMock(return_value=[])
        client.delete_chat = AsyncMock()
        client.create_chat = AsyncMock(return_value="chat-new")
        result = _run(client.ensure_chat("my-chat", ["ds1"]))
        self.assertEqual(result, "chat-new")
        client.delete_chat.assert_not_called()

    def test_existing_chat_deleted(self):
        """Existing chat with same name → deleted then re-created."""
        client = self._make_client()
        client.list_chats = AsyncMock(return_value=[
            {"id": "chat-old", "name": "my-chat"},
        ])
        client.delete_chat = AsyncMock()
        client.create_chat = AsyncMock(return_value="chat-new")
        result = _run(client.ensure_chat("my-chat", ["ds1"], similarity_threshold=0.2))
        self.assertEqual(result, "chat-new")
        client.delete_chat.assert_awaited_once_with("chat-old")
        client.create_chat.assert_awaited_once_with(
            "my-chat", ["ds1"], similarity_threshold=0.2, top_n=None,
        )


class TestListDatasets(unittest.TestCase):
    """list_datasets should correctly parse the response."""

    def _make_client(self):
        with patch.object(RagflowClient, "__init__", lambda self, **kw: None):
            c = object.__new__(RagflowClient)
            c.base_url = "http://test:9380"
            c.headers = {"Authorization": "Bearer test"}
            c._client = None
            return c

    def test_returns_list(self):
        client = self._make_client()
        client._request = AsyncMock(return_value={
            "code": 0,
            "data": [{"id": "ds1", "name": "test"}],
        })
        result = _run(client.list_datasets())
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], "ds1")

    def test_non_list_returns_empty(self):
        client = self._make_client()
        client._request = AsyncMock(return_value={"code": 0, "data": "unexpected"})
        result = _run(client.list_datasets())
        self.assertEqual(result, [])


class TestExtractReferences(unittest.TestCase):
    """extract_references should pull references from RAGFlow response."""

    def test_empty_response(self):
        refs = RagflowClient.extract_references({})
        self.assertEqual(refs, [])

    def test_with_chunks(self):
        response = {
            "reference": {
                "chunks": [
                    {
                        "document_name": "test.pdf",
                        "content": "some content",
                        "positions": [[1, 10, 20, 100, 200]],
                    }
                ]
            }
        }
        refs = RagflowClient.extract_references(response)
        self.assertGreaterEqual(len(refs), 1)
        self.assertEqual(refs[0]["document_name"], "test.pdf")
        self.assertEqual(refs[0]["document_type"], "pdf")
        self.assertEqual(refs[0]["page_number"], 1)
        self.assertIsNone(refs[0]["chunk_index"])
        self.assertEqual(refs[0]["coordinates"], [10.0, 20.0, 100.0, 200.0])

    def test_excel_gets_chunk_index_not_page(self):
        """Excel positions like [[26, 25, 25, 25, 25]] must NOT be treated as PDF pages."""
        response = {
            "reference": {
                "chunks": [
                    {
                        "document_name": "data.xlsx",
                        "content": "excel content",
                        "positions": [[26, 25, 25, 25, 25]],
                    }
                ]
            }
        }
        refs = RagflowClient.extract_references(response)
        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0]["document_type"], "excel")
        self.assertIsNone(refs[0]["page_number"])
        self.assertEqual(refs[0]["chunk_index"], 26)
        self.assertIsNone(refs[0]["coordinates"])

    def test_excel_identical_positions(self):
        """Excel with identical position values should also get chunk_index."""
        response = {
            "reference": {
                "chunks": [
                    {
                        "document_name": "report.xlsx",
                        "content": "some data",
                        "positions": [[5, 5, 5, 5, 5]],
                    }
                ]
            }
        }
        refs = RagflowClient.extract_references(response)
        self.assertEqual(refs[0]["chunk_index"], 5)
        self.assertIsNone(refs[0]["page_number"])

    def test_docx_gets_chunk_index(self):
        """DOCX should get chunk_index regardless of position values."""
        response = {
            "reference": {
                "chunks": [
                    {
                        "document_name": "manual.docx",
                        "content": "doc content",
                        "positions": [[10, 9, 9, 9, 9]],
                    }
                ]
            }
        }
        refs = RagflowClient.extract_references(response)
        self.assertEqual(refs[0]["document_type"], "docx")
        self.assertIsNone(refs[0]["page_number"])
        self.assertEqual(refs[0]["chunk_index"], 10)

    def test_pptx_gets_page_number_no_coordinates(self):
        """PPT/PPTX has real slide numbers but no bounding-box coordinates."""
        response = {
            "reference": {
                "chunks": [
                    {
                        "document_name": "slides.pptx",
                        "content": "slide content",
                        "positions": [[3, 0, 0, 0, 0]],
                    }
                ]
            }
        }
        refs = RagflowClient.extract_references(response)
        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0]["document_type"], "ppt")
        self.assertEqual(refs[0]["page_number"], 3)
        self.assertIsNone(refs[0]["chunk_index"])
        self.assertIsNone(refs[0]["coordinates"])

    def test_ppt_gets_page_number(self):
        """Old .ppt extension should also get real slide numbers."""
        response = {
            "reference": {
                "chunks": [
                    {
                        "document_name": "old_deck.ppt",
                        "content": "old slide",
                        "positions": [[7, 0, 0, 0, 0]],
                    }
                ]
            }
        }
        refs = RagflowClient.extract_references(response)
        self.assertEqual(refs[0]["document_type"], "ppt")
        self.assertEqual(refs[0]["page_number"], 7)
        self.assertIsNone(refs[0]["chunk_index"])
        self.assertIsNone(refs[0]["coordinates"])

    def test_no_reference_key(self):
        refs = RagflowClient.extract_references({"answer": "hello"})
        self.assertEqual(refs, [])


class TestPagination(unittest.TestCase):
    def _make_client(self):
        with patch.object(RagflowClient, "__init__", lambda self, **kw: None):
            c = object.__new__(RagflowClient)
            c.base_url = "http://test:9380"
            c.headers = {"Authorization": "Bearer test"}
            c._client = None
            return c

    def test_list_datasets_page_returns_total(self):
        client = self._make_client()
        client._request = AsyncMock(return_value={
            "code": 0,
            "data": {"data": [{"id": "1"}], "total": 10}
        })
        
        async def run():
            return await client.list_datasets_page(page=1, page_size=10)
        
        res = _run(run())
        self.assertEqual(res["items"][0]["id"], "1")
        self.assertEqual(res["total"], 10)

    def test_list_datasets_page_returns_none_total_for_list(self):
        client = self._make_client()
        client._request = AsyncMock(return_value={
            "code": 0,
            "data": [{"id": "1"}]
        })
        
        async def run():
            return await client.list_datasets_page()
            
        res = _run(run())
        self.assertEqual(res["items"][0]["id"], "1")
        self.assertIsNone(res["total"])

    def test_list_documents_page_returns_total(self):
        client = self._make_client()
        client._request = AsyncMock(return_value={
            "code": 0,
            "data": {"docs": [{"id": "d1"}], "total": 5}
        })
        
        async def run():
            return await client.list_documents_page("ds1")
            
        res = _run(run())
        self.assertEqual(res["items"][0]["id"], "d1")
        self.assertEqual(res["total"], 5)


class TestDelete(unittest.TestCase):
    def _make_client(self):
        with patch.object(RagflowClient, "__init__", lambda self, **kw: None):
            c = object.__new__(RagflowClient)
            c.base_url = "http://test:9380"
            c.headers = {"Authorization": "Bearer test"}
            c._client = None
            return c

    def test_delete_datasets(self):
        client = self._make_client()
        client._request = AsyncMock(return_value={"code": 0})
        
        async def run():
            await client.delete_datasets(["1", "2"])
            
        _run(run())
        client._request.assert_called_with(
            "DELETE", "/api/v1/datasets", json={"ids": ["1", "2"]}
        )

    def test_delete_documents(self):
        client = self._make_client()
        client._request = AsyncMock(return_value={"code": 0})
        
        async def run():
            await client.delete_documents("ds1", ["d1"])
            
        _run(run())
        client._request.assert_called_with(
            "DELETE", "/api/v1/datasets/ds1/documents", json={"ids": ["d1"]}
        )


if __name__ == "__main__":
    unittest.main()
