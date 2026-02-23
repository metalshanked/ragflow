
import hashlib
import unittest
from unittest.mock import AsyncMock, patch

from assessment.models import TaskState, TaskStatus, TaskRecord, RagflowContext
from assessment.services import add_documents_to_session
import assessment.services as _services_mod

# Mock database functions since services.py uses them
_mock_db_get = AsyncMock()
_mock_db_save = AsyncMock()

@patch("assessment.services.get_task", new=_mock_db_get)
@patch("assessment.services.db_save_task", new=_mock_db_save)
class TestFileDeduplication(unittest.TestCase):
    
    def setUp(self):
        _mock_db_get.reset_mock()
        _mock_db_save.reset_mock()

    def _make_record(self, file_hashes=None, doc_ids=None):
        if file_hashes is None:
            file_hashes = {}
        if doc_ids is None:
            doc_ids = []
            
        return TaskRecord(
            task_id="test-task-1",
            status=TaskStatus(
                task_id="test-task-1",
                state=TaskState.AWAITING_DOCUMENTS,
                total_questions=0
            ),
            ragflow=RagflowContext(
                dataset_id="ds-1",
                document_ids=doc_ids,
                file_hashes=file_hashes
            ),
            questions=[]
        )

    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    @patch.object(_services_mod, "RagflowClient")
    def test_upload_unique_file(self, MockClient):
        """Test uploading a file with a unique hash."""
        record = self._make_record()
        _mock_db_get.return_value = record
        
        mock_client = AsyncMock()
        mock_client.upload_document = AsyncMock(return_value="doc-new-1")
        MockClient.return_value = mock_client
        
        content = b"unique content"
        fhash = hashlib.sha256(content).hexdigest()
        
        resp = self._run(add_documents_to_session("test-task-1", [("file1.pdf", content)]))
        
        self.assertEqual(resp.uploaded_document_ids, ["doc-new-1"])
        self.assertEqual(resp.total_documents, 1)
        self.assertIn(fhash, record.ragflow.file_hashes)
        self.assertEqual(record.ragflow.file_hashes[fhash], "doc-new-1")
        mock_client.upload_document.assert_called_once()

    @patch.object(_services_mod, "RagflowClient")
    def test_upload_duplicate_of_existing(self, MockClient):
        """Test uploading a file that is a duplicate of an already uploaded file."""
        content = b"existing content"
        fhash = hashlib.sha256(content).hexdigest()
        
        # Pre-populate record with this hash
        record = self._make_record(file_hashes={fhash: "doc-existing-1"}, doc_ids=["doc-existing-1"])
        _mock_db_get.return_value = record
        
        mock_client = AsyncMock()
        MockClient.return_value = mock_client
        
        resp = self._run(add_documents_to_session("test-task-1", [("file2.pdf", content)]))
        
        self.assertEqual(resp.uploaded_document_ids, [])
        self.assertEqual(resp.total_documents, 1)
        self.assertIn("duplicate", resp.message.lower())
        mock_client.upload_document.assert_not_called()

    @patch.object(_services_mod, "RagflowClient")
    def test_upload_batch_duplicates(self, MockClient):
        """Test uploading a batch where files are duplicates of each other."""
        record = self._make_record()
        _mock_db_get.return_value = record
        
        mock_client = AsyncMock()
        mock_client.upload_document = AsyncMock(side_effect=["doc-1"])
        MockClient.return_value = mock_client
        
        content = b"same content"
        # Upload two files with same content
        resp = self._run(add_documents_to_session("test-task-1", [
            ("file1.pdf", content),
            ("file2.pdf", content)
        ]))
        
        # Should only upload one
        self.assertEqual(len(resp.uploaded_document_ids), 1)
        self.assertEqual(resp.total_documents, 1)
        self.assertEqual(mock_client.upload_document.call_count, 1)
        
        fhash = hashlib.sha256(content).hexdigest()
        self.assertIn(fhash, record.ragflow.file_hashes)

    @patch.object(_services_mod, "RagflowClient")
    def test_upload_mixed_batch(self, MockClient):
        """Test a mix of new unique files, existing duplicates, and batch duplicates."""
        content_existing = b"existing"
        hash_existing = hashlib.sha256(content_existing).hexdigest()
        
        content_new1 = b"new1"
        hash_new1 = hashlib.sha256(content_new1).hexdigest()
        
        content_new2 = b"new2" # Unique
        
        # record has 'existing'
        record = self._make_record(file_hashes={hash_existing: "doc-old"}, doc_ids=["doc-old"])
        _mock_db_get.return_value = record
        
        mock_client = AsyncMock()
        # Expect 2 uploads: new1 (once) and new2 (once)
        mock_client.upload_document = AsyncMock(side_effect=["doc-new-1", "doc-new-2"])
        MockClient.return_value = mock_client
        
        files = [
            ("f1.pdf", content_existing), # Duplicate of existing -> skip
            ("f2.pdf", content_new1),     # New -> upload
            ("f3.pdf", content_new1),     # Duplicate of f2 -> skip
            ("f4.pdf", content_new2),     # New -> upload
        ]
        
        resp = self._run(add_documents_to_session("test-task-1", files))
        
        self.assertEqual(len(resp.uploaded_document_ids), 2)
        self.assertEqual(resp.total_documents, 3) # 1 old + 2 new
        self.assertEqual(mock_client.upload_document.call_count, 2)
        self.assertIn("skipped 2 duplicate(s)", resp.message.lower())

