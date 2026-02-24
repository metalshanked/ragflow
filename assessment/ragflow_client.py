"""
Async HTTP client wrapper for RAGFlow API v1.

All network calls go through this module so the rest of the application
stays decoupled from raw HTTP details.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any, Optional

import httpx

from .config import settings
from .observability import openinference_attributes, set_span_attributes, start_span

logger = logging.getLogger(__name__)

# Timeout for individual HTTP calls (seconds).
_TIMEOUT = httpx.Timeout(timeout=120.0, connect=15.0)


class RagflowClient:
    """Thin async wrapper around the RAGFlow REST API."""

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
    ):
        self.base_url = (base_url or settings.ragflow_base_url).rstrip("/")
        self.api_key = api_key or settings.ragflow_api_key
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
        }
        self._client: httpx.AsyncClient | None = None

    @staticmethod
    def _ssl_verify() -> bool | str:
        """Return the *verify* parameter for ``httpx.AsyncClient``.

        * Custom CA cert path  → path string
        * verify_ssl=False     → ``False`` (skip verification)
        * Default               → ``True`` (system CA bundle)
        """
        if settings.ssl_ca_cert:
            return settings.ssl_ca_cert
        return settings.verify_ssl

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=_TIMEOUT,
                verify=self._ssl_verify(),
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict | None = None,
        data: dict | None = None,
        files: Any = None,
        params: dict | None = None,
    ) -> dict:
        url = f"{self.base_url}{path}"
        with start_span(
            "ragflow.http.request",
            span_kind="TOOL",
            attributes={
                "http.method": method.upper(),
                "url.full": url,
                "ragflow.path": path,
            },
        ) as span:
            with openinference_attributes(
                metadata={
                    "tool.name": "ragflow-http-api",
                    "tool.path": path,
                }
            ):
                try:
                    client = await self._get_client()
                    headers = dict(self.headers)
                    if json is not None:
                        headers["Content-Type"] = "application/json"

                    resp = await client.request(
                        method,
                        url,
                        headers=headers,
                        json=json,
                        data=data,
                        files=files,
                        params=params,
                    )
                    set_span_attributes(
                        span,
                        {
                            "http.status_code": resp.status_code,
                        },
                    )
                    resp.raise_for_status()
                except httpx.ConnectError as exc:
                    set_span_attributes(span, {"error.type": "connect_error", "error.message": str(exc)})
                    raise RuntimeError(
                        f"Cannot connect to RAGFlow at {self.base_url}: {exc}"
                    ) from exc
                except httpx.TimeoutException as exc:
                    set_span_attributes(span, {"error.type": "timeout", "error.message": str(exc)})
                    raise RuntimeError(
                        f"Request to RAGFlow timed out ({method} {url}): {exc}"
                    ) from exc
                except httpx.HTTPStatusError as exc:
                    detail = ""
                    try:
                        detail = exc.response.text[:500]
                    except Exception:
                        pass
                    set_span_attributes(
                        span,
                        {
                            "http.status_code": exc.response.status_code,
                            "error.type": "http_status_error",
                            "error.message": detail,
                        },
                    )
                    raise RuntimeError(
                        f"RAGFlow returned HTTP {exc.response.status_code} "
                        f"for {method} {path}: {detail}"
                    ) from exc

                try:
                    body = resp.json()
                except Exception as exc:
                    set_span_attributes(span, {"error.type": "invalid_json", "error.message": resp.text[:300]})
                    raise RuntimeError(
                        f"RAGFlow returned non-JSON response for {method} {path}: "
                        f"{resp.text[:300]}"
                    ) from exc

                if body.get("code") not in (0, None):
                    set_span_attributes(
                        span,
                        {
                            "error.type": "ragflow_error",
                            "error.message": str(body.get("message", body)),
                            "ragflow.code": body.get("code"),
                        },
                    )
                    raise RuntimeError(f"RAGFlow error: {body.get('message', body)}")
                return body

    # ------------------------------------------------------------------
    # Dataset
    # ------------------------------------------------------------------

    async def list_datasets(self, name: str | None = None, page: int = 1, page_size: int = 100) -> list[dict]:
        """Return a list of dataset dicts, optionally filtered by name."""
        res = await self.list_datasets_page(name, page, page_size)
        return res["items"]

    async def list_datasets_page(
        self, name: str | None = None, page: int = 1, page_size: int = 100
    ) -> dict[str, Any]:
        """Return {"items": [...], "total": N}."""
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if name:
            params["name"] = name
        try:
            body = await self._request("GET", "/api/v1/datasets", params=params)
        except RuntimeError as exc:
            # Handle RAGFlow API bug: it returns 403-ish error if dataset not found by name
            if name and "lacks permission" in str(exc):
                return {"items": [], "total": 0}
            raise
        data = body.get("data", [])
        if isinstance(data, list):
            # API returns just a list, so we don't know total.
            return {"items": data, "total": None}
        if isinstance(data, dict):
            # If API returns wrapper with total
            return {
                "items": data.get("data", []) if isinstance(data.get("data"), list) else [],
                "total": data.get("total")
            }
        return {"items": [], "total": 0}

    async def create_dataset(self, name: str, **kwargs) -> str:
        """Create a dataset and return its ID."""
        payload = {"name": name}
        payload.update(kwargs)
        body = await self._request("POST", "/api/v1/datasets", json=payload)
        return body["data"]["id"]

    async def ensure_dataset(self, name: str, **kwargs) -> str:
        """Return an existing dataset ID for *name*, or create a new one.

        If a dataset with the given name already exists it is deleted first
        so that the caller always gets a clean, empty dataset.
        """
        existing = await self.list_datasets(name=name)
        to_delete = [ds for ds in existing if ds.get("name") == name]
        if to_delete:
            for ds in to_delete:
                logger.info("Deleting existing dataset '%s' (id=%s)", name, ds["id"])
            await asyncio.gather(
                *(self.delete_dataset(ds["id"]) for ds in to_delete)
            )
        return await self.create_dataset(name, **kwargs)

    async def delete_dataset(self, dataset_id: str) -> None:
        await self.delete_datasets([dataset_id])

    async def delete_datasets(self, dataset_ids: list[str]) -> None:
        await self._request("DELETE", "/api/v1/datasets", json={"ids": dataset_ids})

    # ------------------------------------------------------------------
    # Documents
    # ------------------------------------------------------------------

    async def delete_documents(self, dataset_id: str, document_ids: list[str]) -> None:
        await self._request(
            "DELETE",
            f"/api/v1/datasets/{dataset_id}/documents",
            json={"ids": document_ids},
        )

    async def upload_document(
        self, dataset_id: str, filename: str, file_bytes: bytes
    ) -> str:
        """Upload a single document and return its document ID."""
        body = await self._request(
            "POST",
            f"/api/v1/datasets/{dataset_id}/documents",
            files={"file": (filename, file_bytes)},
        )
        docs = body.get("data", [])
        if not docs:
            raise RuntimeError("Upload succeeded but no document returned")
        return docs[0]["id"]

    async def start_parsing(self, dataset_id: str, document_ids: list[str]) -> None:
        """Trigger chunk parsing for the given documents."""
        await self._request(
            "POST",
            f"/api/v1/datasets/{dataset_id}/chunks",
            json={"document_ids": document_ids},
        )

    async def list_documents(
        self, dataset_id: str, page: int = 1, page_size: int = 100
    ) -> list[dict]:
        """Return list of document dicts with their current status."""
        res = await self.list_documents_page(dataset_id, page, page_size)
        return res["items"]

    async def list_documents_page(
        self, dataset_id: str, page: int = 1, page_size: int = 100
    ) -> dict[str, Any]:
        """Return {"items": [...], "total": N}."""
        body = await self._request(
            "GET",
            f"/api/v1/datasets/{dataset_id}/documents",
            params={"page": page, "page_size": page_size},
        )
        data = body.get("data", {})
        # data may be a dict with a "docs" key, or (defensively) a list
        if isinstance(data, dict):
            items = data.get("docs", [])
            total = data.get("total")
        elif isinstance(data, list):
            items = data
            total = None
        else:
            items = []
            total = 0

        # Fix for issue where "words" is always 0 and "status" is undefined.
        # We'll map the raw "run" status to a human-readable "status" field,
        # and remove the useless "words" field.
        for doc in items:
            doc.pop("words", None)
            
            run = str(doc.get("run", ""))
            progress = float(doc.get("progress", 0))
            
            status = "pending"
            if run in ("FAIL", "2"):
                status = "failed"
            elif progress >= 0.999: # treat ~1.0 as success
                status = "success"
            elif progress > 0:
                status = "running"
            
            doc["status"] = status

        return {"items": items, "total": total}

    async def wait_for_parsing(
        self,
        dataset_id: str,
        document_ids: list[str],
        poll_interval: float | None = None,
        timeout: float | None = None,
    ) -> list[dict]:
        """Block until every document reaches a terminal state (done / failed).

        Returns a list of per-document status dicts::

            [
                {
                    "document_id": "...",
                    "document_name": "...",
                    "status": "success" | "failed" | "timeout" | "not_found",
                    "progress": 0.0-1.0,
                    "message": "..."
                },
                ...
            ]

        The caller decides whether partial failures are acceptable.  No
        exception is raised for individual document failures — only if
        *every* document fails does the caller need to abort the pipeline.
        """
        poll_interval = poll_interval or settings.polling_interval_seconds
        timeout = timeout or settings.document_parse_timeout_seconds
        elapsed = 0.0

        # Track terminal state per document
        terminal: dict[str, dict] = {}

        while elapsed < timeout:
            docs = await self.list_documents(dataset_id)
            id_to_doc = {d["id"]: d for d in docs}
            pending = False
            for did in document_ids:
                if did in terminal:
                    continue  # already resolved
                doc = id_to_doc.get(did)
                if not doc:
                    terminal[did] = {
                        "document_id": did,
                        "document_name": "",
                        "status": "not_found",
                        "progress": 0.0,
                        "message": f"Document {did} not found in dataset",
                    }
                    continue
                run = doc.get("run", "")
                progress = float(doc.get("progress", 0))
                name = doc.get("name", did)
                if run in ("FAIL", "2"):
                    terminal[did] = {
                        "document_id": did,
                        "document_name": name,
                        "status": "failed",
                        "progress": progress,
                        "message": doc.get("progress_msg", "") or "Parsing failed",
                    }
                elif progress >= 1.0:
                    terminal[did] = {
                        "document_id": did,
                        "document_name": name,
                        "status": "success",
                        "progress": 1.0,
                        "message": "Parsed successfully",
                    }
                else:
                    pending = True
            if not pending:
                break
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        # Any documents still not resolved after timeout
        for did in document_ids:
            if did not in terminal:
                doc = id_to_doc.get(did, {}) if 'id_to_doc' in dir() else {}
                terminal[did] = {
                    "document_id": did,
                    "document_name": doc.get("name", did) if isinstance(doc, dict) else did,
                    "status": "timeout",
                    "progress": float(doc.get("progress", 0)) if isinstance(doc, dict) else 0.0,
                    "message": "Document parsing timed out",
                }

        return [terminal[did] for did in document_ids]

    # ------------------------------------------------------------------
    # Chat assistant
    # ------------------------------------------------------------------

    async def list_chats(self, name: str | None = None, page: int = 1, page_size: int = 100) -> list[dict]:
        """Return a list of chat assistant dicts, optionally filtered by name."""
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if name:
            params["name"] = name
        try:
            body = await self._request("GET", "/api/v1/chats", params=params)
        except RuntimeError as exc:
            # Handle RAGFlow API bug: it returns error if chat not found by name
            if name and "doesn't exist" in str(exc):
                return []
            raise
        data = body.get("data", [])
        if isinstance(data, list):
            return data
        return []

    async def create_chat(
        self,
        name: str,
        dataset_ids: list[str],
        *,
        similarity_threshold: float | None = None,
        top_n: int | None = None,
        **kwargs
    ) -> str:
        """Create a chat assistant linked to datasets. Returns chat_id."""
        payload: dict[str, Any] = {
            "name": name,
            "dataset_ids": dataset_ids,
        }
        prompt: dict[str, Any] = {}
        if similarity_threshold is not None:
            prompt["similarity_threshold"] = similarity_threshold
        if top_n is not None:
            prompt["top_n"] = top_n
        
        # Merge extra chat options
        payload.update(kwargs)

        # Merge or setup prompt if user provided one in kwargs, otherwise set default prompt
        user_prompt = kwargs.get("prompt", {})
        if isinstance(user_prompt, dict):
            prompt.update(user_prompt)

        if "system" not in prompt:
            prompt["system"] = (
                "You are a compliance/assessment assistant. "
                "For each question you receive, determine if the evidence in the "
                "knowledge base supports a YES or NO answer. "
                "Respond with EXACTLY this format:\n"
                "Answer: Yes/No\n"
                "Details: <brief explanation>\n"
                "If the knowledge base does not contain relevant information, "
                'answer "N/A" and explain why.\n'
                "Here is the knowledge base:\n{knowledge}\n"
                "The above is the knowledge base."
            )
        if "quote" not in prompt:
            prompt["quote"] = True
            
        payload["prompt"] = prompt

        body = await self._request("POST", "/api/v1/chats", json=payload)
        return body["data"]["id"]

    async def delete_chat(self, chat_id: str) -> None:
        await self._request("DELETE", "/api/v1/chats", json={"ids": [chat_id]})

    async def ensure_chat(
        self,
        name: str,
        dataset_ids: list[str],
        *,
        similarity_threshold: float | None = None,
        top_n: int | None = None,
        **kwargs
    ) -> str:
        """Return a chat assistant ID, deleting any existing chat with *name* first."""
        existing = await self.list_chats(name=name)
        to_delete = [ch for ch in existing if ch.get("name") == name]
        if to_delete:
            for ch in to_delete:
                logger.info("Deleting existing chat '%s' (id=%s)", name, ch["id"])
            await asyncio.gather(
                *(self.delete_chat(ch["id"]) for ch in to_delete)
            )
        return await self.create_chat(
            name, dataset_ids,
            similarity_threshold=similarity_threshold,
            top_n=top_n,
            **kwargs
        )

    # ------------------------------------------------------------------
    # Sessions & completions
    # ------------------------------------------------------------------

    async def create_session(self, chat_id: str) -> str:
        body = await self._request(
            "POST", f"/api/v1/chats/{chat_id}/sessions"
        )
        return body["data"]["id"]

    async def ask(
        self,
        chat_id: str,
        session_id: str,
        question: str,
        stream: bool = False,
    ) -> dict:
        """
        Send a question and return the full response dict (non-streaming).

        Returns dict with keys: ``answer``, ``reference`` (with ``chunks``,
        ``total``).
        """
        body = await self._request(
            "POST",
            f"/api/v1/chats/{chat_id}/completions",
            json={
                "question": question,
                "session_id": session_id,
                "stream": stream,
            },
        )
        return body.get("data", {})

    # ------------------------------------------------------------------
    # Reference extraction helpers  (mirrors example logic)
    # ------------------------------------------------------------------

    # Well-known extension → friendly category.  Anything not listed
    # falls back to the raw extension (e.g. "md", "txt", "html") so
    # that every supported file type gets a meaningful label.
    _EXT_TO_TYPE: dict[str, str] = {
        ".pdf": "pdf",
        ".xls": "excel", ".xlsx": "excel", ".xlsm": "excel",
        ".xlsb": "excel", ".csv": "excel",
        ".doc": "docx", ".docx": "docx",
        ".ppt": "ppt", ".pptx": "ppt",
    }

    @staticmethod
    def _detect_doc_type(document_name: str) -> str:
        """Infer a document type label from the file extension.

        Known families (pdf, excel, docx, ppt) get a canonical name.
        Everything else returns the bare extension (e.g. "md", "txt",
        "html") so callers always receive a meaningful label.
        """
        import os
        ext = os.path.splitext(document_name or "")[1].lower()
        if ext in RagflowClient._EXT_TO_TYPE:
            return RagflowClient._EXT_TO_TYPE[ext]
        # Return the extension without the dot; fall back for names
        # with no extension at all.
        return ext.lstrip(".") if ext else "unknown"

    # Document types whose positions encode real page/slide numbers.
    # PDF:  [page, x1, x2, y1, y2] — real page number + bounding-box coordinates.
    # PPT:  [slide, 0, 0, 0, 0]   — real slide number, coordinates always zero.
    _PAGE_NUMBER_TYPES = frozenset({"pdf", "ppt"})
    # Subset that also carries meaningful bounding-box coordinates.
    _COORDINATE_TYPES = frozenset({"pdf"})

    @staticmethod
    def _has_page_number(doc_type: str) -> bool:
        """Return *True* when *doc_type* has real page/slide numbers."""
        return doc_type in RagflowClient._PAGE_NUMBER_TYPES

    @staticmethod
    def _has_coordinates(doc_type: str) -> bool:
        """Return *True* when *doc_type* has real bounding-box coordinates."""
        return doc_type in RagflowClient._COORDINATE_TYPES

    @staticmethod
    def extract_references(response_data: dict) -> list[dict]:
        """
        Given the ``reference`` block from a completion response, return a
        list of cleaned-up reference dicts usable by the assessment models.

        Each dict has:
        - document_name, document_type, page_number, chunk_index,
          coordinates, snippet, image_url, document_url

        Position interpretation varies by document type:
        - **PDF**: ``positions`` encodes ``[page, x1, x2, y1, y2]`` with real
          page numbers and bounding-box coordinates → ``page_number`` and
          ``coordinates`` are populated.
        - **PPT/PPTX**: ``positions`` encodes ``[slide, 0, 0, 0, 0]`` with
          real slide numbers but zero coordinates → ``page_number`` is
          populated, ``coordinates`` is ``None``.
        - **Excel / DOCX / other**: RAGFlow stores
          ``[[index, index, index, index, index]]`` where *index* is a
          chunk or row counter.  There is no meaningful page number, so
          ``page_number`` is ``None`` and ``chunk_index`` is set instead.
        """
        ref_block = response_data.get("reference", {})
        if not ref_block:
            return []
        chunks = ref_block.get("chunks", [])
        results = []
        for chunk in chunks:
            doc_name = chunk.get("document_name", "")
            doc_type = RagflowClient._detect_doc_type(doc_name)
            positions = chunk.get("positions", [])

            page_num = None
            chunk_index = None
            coords = None

            if positions and isinstance(positions, list) and len(positions) > 0:
                pos = positions[0]
                if isinstance(pos, list) and len(pos) >= 1:
                    if RagflowClient._has_page_number(doc_type):
                        # PDF / PPT: first value is a real page or slide number
                        page_num = int(pos[0])
                        if RagflowClient._has_coordinates(doc_type) and len(pos) >= 5:
                            coords = [float(pos[1]), float(pos[2]),
                                       float(pos[3]), float(pos[4])]
                    else:
                        # Excel / DOCX / other: chunk/row index only
                        chunk_index = int(pos[0])

            # Build URLs through the assessment proxy so that raw RAGFlow
            # links are never exposed to clients.
            image_id = chunk.get("image_id")
            image_url = None
            if image_id:
                image_url = f"/api/v1/proxy/image/{image_id}"

            doc_id = chunk.get("document_id")
            doc_url = None
            if doc_id:
                doc_url = f"/api/v1/proxy/document/{doc_id}"
                if page_num is not None:
                    doc_url += f"#page={page_num}"

            content = chunk.get("content", "").strip()
            snippet = (content[:300] + "...") if len(content) > 300 else content

            results.append(
                {
                    "document_name": doc_name,
                    "document_type": doc_type,
                    "page_number": page_num,
                    "chunk_index": chunk_index,
                    "coordinates": coords,
                    "snippet": snippet,
                    "image_url": image_url,
                    "document_url": doc_url,
                }
            )
        return results

    @staticmethod
    def parse_yes_no(answer_text: str) -> tuple[str, str]:
        """
        Parse the LLM answer to extract a Yes/No/N/A verdict and the detail
        explanation.

        Returns (verdict, details).
        """
        answer_text = answer_text or ""
        verdict = "N/A"
        details = answer_text

        # Try to find "Answer: Yes" / "Answer: No" pattern
        m = re.search(r"(?i)\banswer\s*:\s*(yes|no|n/?a)\b", answer_text)
        if m:
            raw = m.group(1).strip().upper()
            if raw in ("YES",):
                verdict = "Yes"
            elif raw in ("NO",):
                verdict = "No"
            else:
                verdict = "N/A"

        # Try to extract details after "Details:" line
        d = re.search(r"(?i)\bdetails?\s*:\s*(.*)", answer_text, re.DOTALL)
        if d:
            details = d.group(1).strip()

        return verdict, details

    @staticmethod
    def get_cited_indices(answer_text: str) -> set[int]:
        """Return set of [ID:N] indices found in the answer."""
        return {int(i) for i in re.findall(r"\[ID:(\d+)\]", answer_text)}
