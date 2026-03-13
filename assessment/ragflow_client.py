"""
Async HTTP client wrapper for RAGFlow API v1.

All network calls go through this module so the rest of the application
stays decoupled from raw HTTP details.
"""

from __future__ import annotations

import asyncio
from copy import deepcopy
import logging
import re
from typing import Any, Optional
from urllib.parse import quote

import httpx

from .config import settings
from .observability import openinference_attributes, set_span_attributes, start_span

logger = logging.getLogger(__name__)


class TransientRagflowError(RuntimeError):
    """Raised when a transient upstream/network failure is likely retryable."""


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

    @staticmethod
    def _http_timeout() -> httpx.Timeout:
        """Return the per-request timeout used for outbound RAGFlow HTTP calls."""
        return httpx.Timeout(
            timeout=settings.ragflow_http_timeout_seconds,
            connect=min(15.0, settings.ragflow_http_timeout_seconds),
        )

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=self._http_timeout(),
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
        retry_attempts: int | None = None,
    ) -> dict:
        url = f"{self.base_url}{path}"
        method_upper = method.upper()
        retries = retry_attempts
        if retries is None:
            retries = settings.ragflow_http_retry_attempts if method_upper in {"GET", "PUT", "DELETE"} else 0
        max_attempts = max(1, int(retries or 0) + 1)
        with start_span(
            "ragflow.http.request",
            span_kind="TOOL",
            attributes={
                "http.method": method_upper,
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
                headers = dict(self.headers)
                if json is not None:
                    headers["Content-Type"] = "application/json"
                resp: httpx.Response | None = None
                for attempt in range(1, max_attempts + 1):
                    try:
                        client = await self._get_client()
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
                        break
                    except httpx.ConnectError as exc:
                        is_last = attempt >= max_attempts
                        set_span_attributes(span, {"error.type": "connect_error", "error.message": str(exc)})
                        if is_last:
                            raise TransientRagflowError(
                                f"Cannot connect to RAGFlow at {self.base_url}: {exc}"
                            ) from exc
                        logger.warning(
                            "Retrying RAGFlow request after connect error attempt=%s/%s method=%s path=%s",
                            attempt,
                            max_attempts,
                            method_upper,
                            path,
                        )
                        await asyncio.sleep(settings.ragflow_retry_backoff_seconds * attempt)
                        continue
                    except httpx.TimeoutException as exc:
                        is_last = attempt >= max_attempts
                        set_span_attributes(span, {"error.type": "timeout", "error.message": str(exc)})
                        if is_last:
                            raise TransientRagflowError(
                                f"Request to RAGFlow timed out ({method_upper} {url}): {exc}"
                            ) from exc
                        logger.warning(
                            "Retrying RAGFlow request after timeout attempt=%s/%s method=%s path=%s",
                            attempt,
                            max_attempts,
                            method_upper,
                            path,
                        )
                        await asyncio.sleep(settings.ragflow_retry_backoff_seconds * attempt)
                        continue
                    except httpx.HTTPStatusError as exc:
                        detail = ""
                        try:
                            detail = exc.response.text[:500]
                        except Exception:
                            pass
                        status_code = exc.response.status_code
                        set_span_attributes(
                            span,
                            {
                                "http.status_code": status_code,
                                "error.type": "http_status_error",
                                "error.message": detail,
                            },
                        )
                        is_retryable = status_code == 429 or status_code >= 500
                        is_last = attempt >= max_attempts
                        if is_retryable and not is_last:
                            logger.warning(
                                "Retrying RAGFlow request after HTTP %s attempt=%s/%s method=%s path=%s",
                                status_code,
                                attempt,
                                max_attempts,
                                method_upper,
                                path,
                            )
                            await asyncio.sleep(settings.ragflow_retry_backoff_seconds * attempt)
                            continue
                        error_cls = TransientRagflowError if is_retryable else RuntimeError
                        raise error_cls(
                            f"RAGFlow returned HTTP {status_code} "
                            f"for {method_upper} {path}: {detail}"
                        ) from exc

                try:
                    if resp is None:
                        raise RuntimeError(f"No response received from RAGFlow for {method_upper} {path}")
                    body = resp.json()
                except Exception as exc:
                    set_span_attributes(span, {"error.type": "invalid_json", "error.message": resp.text[:300]})
                    raise RuntimeError(
                        f"RAGFlow returned non-JSON response for {method_upper} {path}: "
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

    @staticmethod
    def _deep_merge_dicts(base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
        """Recursively merge dictionaries with *overrides* taking precedence."""
        merged = deepcopy(base)
        for key, value in overrides.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = RagflowClient._deep_merge_dicts(merged[key], value)
            else:
                merged[key] = value
        return merged

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

    @staticmethod
    def _validate_dataset_payload(payload: dict[str, Any]) -> dict[str, Any]:
        """Reject non-REST dataset fields so callers use the public dataset contract."""
        normalized = dict(payload)
        legacy_keys = {"parser_id", "embd_id", "kb_id"}
        present_legacy = sorted(key for key in legacy_keys if key in normalized)
        if present_legacy:
            raise RuntimeError(
                "Unsupported legacy dataset option(s): "
                f"{', '.join(present_legacy)}. "
                "Use the REST dataset fields instead: chunk_method, embedding_model, and id-free payloads."
            )
        normalized.pop("id", None)
        return normalized

    async def create_dataset(self, name: str, **kwargs) -> str:
        """Create a dataset through the public REST dataset API and return its ID."""
        payload = self._validate_dataset_payload({"name": name, **kwargs})
        body = await self._request("POST", "/api/v1/datasets", json=payload)
        data = body.get("data", {}) if isinstance(body, dict) else {}
        dataset_id = data.get("id") if isinstance(data, dict) else None
        if not dataset_id:
            raise RuntimeError(f"Create dataset succeeded but no dataset id returned: {body}")
        return str(dataset_id)

    async def get_dataset_detail(self, dataset_id: str) -> dict[str, Any]:
        """Fetch dataset details from the public REST dataset API."""
        body = await self._request(
            "GET",
            "/api/v1/datasets",
            params={"id": dataset_id, "page": 1, "page_size": 1},
        )
        data = body.get("data", []) if isinstance(body, dict) else []
        items: list[dict[str, Any]]
        if isinstance(data, list):
            items = [item for item in data if isinstance(item, dict)]
        elif isinstance(data, dict):
            raw_items = data.get("data", [])
            items = [item for item in raw_items if isinstance(item, dict)] if isinstance(raw_items, list) else []
        else:
            items = []

        for item in items:
            if str(item.get("id", "")).strip() == dataset_id:
                return item
        raise RuntimeError(f"Dataset detail not found for id={dataset_id}")

    async def update_dataset(self, dataset_id: str, **kwargs) -> dict[str, Any]:
        """Update a dataset through the public REST dataset API."""
        detail = await self.get_dataset_detail(dataset_id)
        payload: dict[str, Any] = {}

        updates = dict(kwargs)
        updates = self._validate_dataset_payload(updates)

        parser_updates = updates.get("parser_config")
        if isinstance(parser_updates, dict) and isinstance(detail.get("parser_config"), dict):
            updates["parser_config"] = self._deep_merge_dicts(detail["parser_config"], parser_updates)

        payload.update(updates)

        # Preserve name when callers only provide partial update fields.
        if "name" not in payload:
            payload["name"] = str(detail.get("name") or "").strip()
        if not payload["name"]:
            raise RuntimeError(f"Cannot update dataset {dataset_id}: dataset name is empty")
        if "description" in payload and payload["description"] is None:
            payload["description"] = ""

        body = await self._request("PUT", f"/api/v1/datasets/{dataset_id}", json=payload)
        data = body.get("data", {}) if isinstance(body, dict) else {}
        return data if isinstance(data, dict) else {}

    async def ensure_dataset(
        self,
        name: str,
        *,
        reuse_existing_dataset: bool = False,
        **kwargs,
    ) -> str:
        """Return an existing dataset ID for *name*, or create a new one.

        If ``reuse_existing_dataset`` is true and a dataset with the exact
        same name exists, reuse it and apply dataset updates in-place.

        Otherwise, if a dataset with the same name exists it is deleted first
        so the caller gets a clean, empty dataset.
        """
        existing = await self.list_datasets(name=name)
        to_delete = [ds for ds in existing if ds.get("name") == name]
        if reuse_existing_dataset and to_delete:
            picked = to_delete[0]
            dataset_id = str(picked.get("id") or "").strip()
            if not dataset_id:
                raise RuntimeError(
                    f"Cannot reuse dataset '{name}': existing dataset id is empty"
                )
            if kwargs:
                await self.update_dataset(dataset_id, **kwargs)
            return dataset_id

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

    @staticmethod
    def _has_page_number(doc_type: str) -> bool:
        """Return *True* when *doc_type* has real page/slide numbers."""
        return doc_type in RagflowClient._PAGE_NUMBER_TYPES

    _IMAGE_TYPES = frozenset({"png", "jpg", "jpeg", "gif", "bmp", "webp", "svg", "tiff"})
    _TEXT_TYPES = frozenset({"txt", "md", "markdown", "html", "htm", "json", "xml", "yaml", "yml"})
    _SPREADSHEET_TYPES = frozenset({"excel"})
    _WORD_TYPES = frozenset({"docx", "rtf", "odt"})

    @staticmethod
    def _detect_media_family(doc_type: str) -> str:
        if doc_type == "pdf":
            return "pdf"
        if doc_type == "ppt":
            return "presentation"
        if doc_type in RagflowClient._SPREADSHEET_TYPES:
            return "spreadsheet"
        if doc_type in RagflowClient._WORD_TYPES:
            return "document"
        if doc_type in RagflowClient._IMAGE_TYPES:
            return "image"
        if doc_type in RagflowClient._TEXT_TYPES:
            return "text"
        return "other"

    @staticmethod
    def _build_location(doc_type: str, positions: Any) -> dict[str, Any]:
        location: dict[str, Any] = {
            "kind": "",
            "value": None,
            "label": "",
            "page_number": None,
            "highlight_box": None,
        }
        if not isinstance(positions, list) or not positions:
            return location
        pos = positions[0]
        if not isinstance(pos, list) or not pos:
            return location
        try:
            first_value = int(pos[0])
        except (TypeError, ValueError):
            return location

        if RagflowClient._has_page_number(doc_type):
            location["page_number"] = first_value
            if doc_type == "pdf":
                location["kind"] = "page"
                location["value"] = first_value
                location["label"] = f"Page {first_value}"
                if len(pos) >= 5:
                    location["highlight_box"] = {
                        "left": float(pos[1]),
                        "right": float(pos[2]),
                        "top": float(pos[3]),
                        "bottom": float(pos[4]),
                    }
            else:
                location["kind"] = "slide"
                location["value"] = first_value
                location["label"] = f"Slide {first_value}"
            return location

        location["kind"] = "chunk"
        location["value"] = first_value
        location["label"] = f"Chunk {first_value}"
        return location

    @staticmethod
    def _strip_html(html: str) -> str:
        text = re.sub(r"<[^>]+>", " ", html or "")
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _build_preview(reference_type: str, content: Any, image_url: str | None) -> dict[str, Any]:
        raw_content = str(content or "").strip()
        html_content = ""
        table_html = ""
        content_format = "none"
        excerpt_source = raw_content

        if raw_content:
            lower_content = raw_content.lower()
            if reference_type == "table" or "<table" in lower_content:
                content_format = "table_html"
                table_html = raw_content
                html_content = raw_content
                excerpt_source = RagflowClient._strip_html(raw_content)
            elif bool(re.search(r"<(div|p|span|ul|ol|li|img|table|tr|td|th)\b", raw_content, re.IGNORECASE)):
                content_format = "html"
                html_content = raw_content
                excerpt_source = RagflowClient._strip_html(raw_content)
            else:
                content_format = "text"

        text_excerpt = excerpt_source[:300] + "..." if len(excerpt_source) > 300 else excerpt_source
        return {
            "text_excerpt": text_excerpt,
            "full_content": raw_content,
            "content_format": content_format,
            "html_content": html_content,
            "table_html": table_html,
            "has_inline_preview": bool(raw_content or image_url),
        }

    @staticmethod
    def _first_present(chunk: dict[str, Any], *keys: str) -> Any:
        for key in keys:
            value = chunk.get(key)
            if value not in (None, ""):
                return value
        return None

    @staticmethod
    def extract_references(response_data: dict) -> list[dict]:
        """
        Given the ``reference`` block from a completion response, return a
        list of cleaned-up reference dicts usable by the assessment models.

        Each dict has nested document, location, preview, links, retrieval,
        and source-metadata sections.

        Position interpretation varies by document type:
        - **PDF**: ``positions`` encodes ``[page, x1, x2, y1, y2]`` with real
          page numbers and bounding-box coordinates.
        - **PPT/PPTX**: ``positions`` encodes ``[slide, 0, 0, 0, 0]`` with
          real slide numbers but zero coordinates.
        - **Excel / DOCX / other**: RAGFlow commonly stores
          ``[[index, index, index, index, index]]`` via generic chunk
          tokenization. The wrapper therefore treats the value as a generic
          chunk index instead of inventing a row number or sheet location.
        """
        ref_block = response_data.get("reference", {})
        if not ref_block:
            return []
        chunks = ref_block.get("chunks", [])
        known_keys = {
            "content",
            "content_with_weight",
            "document_id",
            "doc_id",
            "document_name",
            "docnm_kwd",
            "dataset_id",
            "kb_id",
            "image_id",
            "img_id",
            "positions",
            "position_int",
            "url",
            "similarity",
            "vector_similarity",
            "term_similarity",
            "doc_type",
            "doc_type_kwd",
        }
        results = []
        for chunk in chunks:
            doc_name = str(chunk.get("document_name", "") or "")
            doc_type = RagflowClient._detect_doc_type(doc_name)
            reference_type = str(chunk.get("doc_type") or chunk.get("doc_type_kwd") or "").strip() or "text"
            image_id = RagflowClient._first_present(chunk, "image_id", "img_id")
            image_url = f"/api/v1/proxy/image/{image_id}" if image_id else None
            doc_id = RagflowClient._first_present(chunk, "document_id", "doc_id")
            doc_url = f"/api/v1/proxy/document/{doc_id}" if doc_id else None
            rendered_doc_url = (
                f"/api/v1/proxy/document/{doc_id}/render?filename={quote(doc_name or ('document.' + (doc_type or 'bin')))}"
                if doc_id
                else None
            )
            dataset_id = RagflowClient._first_present(chunk, "dataset_id", "kb_id")
            preview = RagflowClient._build_preview(reference_type, chunk.get("content", ""), image_url)
            extra_fields = {k: v for k, v in chunk.items() if k not in known_keys}

            results.append(
                {
                    "reference_type": reference_type,
                    "document": {
                        "document_id": doc_id,
                        "dataset_id": dataset_id,
                        "image_id": image_id,
                        "document_name": doc_name,
                        "document_type": doc_type,
                        "media_family": RagflowClient._detect_media_family(doc_type),
                    },
                    "location": RagflowClient._build_location(doc_type, chunk.get("positions", [])),
                    "preview": preview,
                    "links": {
                        "document_url": doc_url,
                        "rendered_document_url": rendered_doc_url,
                        "image_url": image_url,
                        "source_url": chunk.get("url"),
                    },
                    "retrieval": {
                        "score": chunk.get("similarity"),
                        "vector_score": chunk.get("vector_similarity"),
                        "term_score": chunk.get("term_similarity"),
                    },
                    "source_metadata": {
                        "provider": "ragflow",
                        "provider_reference_type": str(chunk.get("doc_type") or chunk.get("doc_type_kwd") or "").strip(),
                        "extra_fields": extra_fields,
                    },
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
