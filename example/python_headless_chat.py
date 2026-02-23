"""
RAGFlow Headless Chat Client Example

This script demonstrates how to interact with the RAGFlow HTTP API to build a 
custom chat interface that replicates the core features of the built-in RAGFlow UI.

Key Concepts:
1. Session Management: Every conversation requires a session_id created under a chat_id.
2. Citation Format: RAGFlow returns citations in the text as `[ID:N]`. 
   This script preserves this format and handles the 0-based indexing (starting at [ID:0]).
3. Reference Chunks: The API provides a list of source 'chunks' used to generate the answer.
4. Page Numbers & Locations: Derived from the `positions` field.
   - For PDFs: [[page, x1, x2, y1, y2], ...] with real page numbers and bounding-box coordinates.
   - For PPT/PPTX: [[slide, 0, 0, 0, 0], ...] with real slide numbers but zero coordinates.
   - For Excel/DOCX/other: [[index, index, index, index, index], ...] where all values are identical
     and represent a chunk/row counter (NOT a page number).
5. Image Links: Derived from `image_id` using the `/v1/document/image/<id>` endpoint.

Usage Example:
    client = RAGFlowChatClient(API_KEY, BASE_URL)
    session_id = client.create_session(CHAT_ID)
    client.ask(CHAT_ID, session_id, "What is RAGFlow?")
"""
import os
import requests
import json
import re
import time

# ==========================================
# CONFIGURATION
# ==========================================
# Replace with your actual RAGFlow API key and address
API_KEY = "your_api_key_here"
BASE_URL = "http://localhost:9380"  # Default RAGFlow address
CHAT_ID = "your_chat_id_here"       # The ID of the Chat Assistant you want to use

# Set this to False if you want to use non-streaming mode
STREAM = True

# Set this to True to only see the references that the LLM actually cited in its answer
ONLY_SHOW_CITED = True

# ==========================================
# CLIENT LOGIC
# ==========================================

class RAGFlowChatClient:
    """
    A client to interact with RAGFlow's Chat API.
    
    Handles authentication, session creation, and parsing of both streaming 
    and non-streaming responses including citations and references.
    """
    def __init__(self, api_key, base_url):
        """
        Initialize the client.
        
        :param api_key: Your RAGFlow API Key.
        :param base_url: The base URL of your RAGFlow instance (e.g., http://localhost:9380).
        """
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def create_session(self, chat_id):
        """Creates a new chat session."""
        url = f"{self.base_url}/api/v1/chats/{chat_id}/sessions"
        resp = requests.post(url, headers=self.headers)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise Exception(f"Failed to create session: {data.get('message')}")
        return data["data"]["id"]

    def ask(self, chat_id, session_id, question, stream=True):
        """
        Sends a question to the assistant and prints the response.
        
        This method handles:
        - Preserving internal citations ([ID:N]).
        - Detecting and displaying "Thinking" process blocks.
        - Collecting and formatting source references.
        
        :param chat_id: The ID of the Chat Assistant.
        :param session_id: The ID of the current chat session.
        :param question: The user's query.
        :param stream: Whether to use SSE streaming or wait for the full response.
        """
        url = f"{self.base_url}/api/v1/chats/{chat_id}/completions"
        payload = {
            "question": question,
            "session_id": session_id,
            "stream": stream
        }

        references = []
        total_found = 0
        full_answer_for_citations = ""

        print(f"\n[User]: {question}")
        print("[Assistant]: ", end="", flush=True)

        if stream:
            with requests.post(url, headers=self.headers, json=payload, stream=True) as r:
                r.raise_for_status()
                for line in r.iter_lines():
                    if not line:
                        continue
                    
                    line_text = line.decode('utf-8')
                    if line_text.startswith("data:"):
                        data_str = line_text[5:].strip()
                        if data_str == "[DONE]":
                            break
                        
                        try:
                            event_data = json.loads(data_str)
                        except json.JSONDecodeError:
                            continue

                        # 1. Handle Thinking Process (UI uses <think> tags)
                        if event_data.get("start_to_think"):
                            print("\n--- Thinking Process ---")
                            continue
                        if event_data.get("end_to_think"):
                            print("\n--- End of Thinking ---")
                            continue

                        chunk = event_data.get("data")
                        if not chunk or chunk is True: # chunk is True means end of data in some versions
                            continue

                        # 2. Handle Answer Content
                        answer_part = chunk.get("answer", "")
                        if answer_part:
                            full_answer_for_citations += answer_part
                            print(answer_part, end="", flush=True)

                        # 3. Collect References
                        if "reference" in chunk:
                            ref_data = chunk["reference"]
                            references = ref_data.get("chunks", [])
                            total_found = ref_data.get("total", 0)
        else:
            resp = requests.post(url, headers=self.headers, json=payload)
            resp.raise_for_status()
            response_data = resp.json()
            
            if response_data.get("code") != 0:
                raise Exception(f"Error from API: {response_data.get('message')}")
            
            data = response_data.get("data", {})
            full_answer = data.get("answer", "")
            full_answer_for_citations = full_answer
            
            print(full_answer)
            
            if "reference" in data:
                ref_data = data["reference"]
                references = ref_data.get("chunks", [])
                total_found = ref_data.get("total", 0)

        # Prepare references for printing. 
        # We store them as tuples of (original_id, chunk) to ensure the 
        # labels in the reference list match the citations in the text.
        references_to_print = []
        
        # 1. Extract citation indices from the answer text
        cited_indices = set()
        # Match [ID:N]
        id_matches = re.findall(r"\[ID:(\d+)\]", full_answer_for_citations)
        cited_indices.update(int(i) for i in id_matches)

        # 2. Map original indices to chunks
        if ONLY_SHOW_CITED and cited_indices:
            for i, chunk in enumerate(references):
                if i in cited_indices:
                    references_to_print.append((i, chunk))
            
            # If we found citations but they weren't in the references list, 
            # it might be an LLM hallucination or truncation. 
            if not references_to_print:
                # Fallback to all if filtering resulted in nothing but citations existed
                references_to_print = [(i, c) for i, c in enumerate(references)]
        else:
            # Show all retrieved chunks
            references_to_print = [(i, c) for i, c in enumerate(references)]

        print("\n" + "="*60)
        self._print_references(references_to_print, cited_indices, total_found)

    # ------------------------------------------------------------------
    # Document type helpers
    # ------------------------------------------------------------------
    # Well-known extension → friendly category.  Anything not listed
    # falls back to the raw extension (e.g. "md", "txt", "html") so
    # that every supported file type gets a meaningful label.
    _EXT_TO_TYPE = {
        ".pdf": "pdf",
        ".xls": "excel", ".xlsx": "excel", ".xlsm": "excel",
        ".xlsb": "excel", ".csv": "excel",
        ".doc": "docx", ".docx": "docx",
        ".ppt": "ppt", ".pptx": "ppt",
    }

    @staticmethod
    def _detect_doc_type(document_name):
        """Infer a document type label from the file extension.

        Known families (pdf, excel, docx, ppt) get a canonical name.
        Everything else returns the bare extension (e.g. "md", "txt",
        "html") so callers always receive a meaningful label.
        """
        ext = os.path.splitext(document_name or "")[1].lower()
        if ext in RAGFlowChatClient._EXT_TO_TYPE:
            return RAGFlowChatClient._EXT_TO_TYPE[ext]
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
    def _has_page_number(doc_type):
        """Return True when *doc_type* has real page/slide numbers."""
        return doc_type in RAGFlowChatClient._PAGE_NUMBER_TYPES

    @staticmethod
    def _has_coordinates(doc_type):
        """Return True when *doc_type* has real bounding-box coordinates."""
        return doc_type in RAGFlowChatClient._COORDINATE_TYPES

    def _print_references(self, references, cited_indices=None, total_found=0):
        """
        Prints formatted references including images and locations.
        
        Position interpretation varies by document type:
        - PDF: positions encode [page, x1, x2, y1, y2] with real coordinates.
        - PPT/PPTX: positions encode [slide, 0, 0, 0, 0] with real slide numbers.
        - Excel/DOCX/other: positions encode [index, index, index, index, index]
          where *index* is a chunk/row counter — NOT a page number.
        
        :param references: A list of (id, chunk) tuples where 'id' is the 
                          original 0-based index from the RAG search results.
        :param cited_indices: The set of indices found in the LLM's answer.
        :param total_found: The total number of chunks found by the RAG engine.
        """
        if not references:
            if total_found > 0:
                print(f"No cited references to display (Total chunks found: {total_found}).")
            else:
                print("No references found.")
            return

        print(f"REFERENCES (Showing {len(references)} cited of {total_found} chunks found):")
        for ref_id, chunk in references:
            doc_name = chunk.get("document_name", "Unknown Document")
            doc_id = chunk.get("document_id")
            doc_type = self._detect_doc_type(doc_name)
            
            # 1. Position / Location info
            # PDF:       [[page, x1, x2, y1, y2], ...] — real page + bounding box
            # PPT/PPTX:  [[slide, 0, 0, 0, 0], ...]     — real slide number, zero coords
            # Excel/DOCX/other: [[idx, idx, idx, idx, idx], ...] — chunk/row index
            positions = chunk.get("positions", [])
            location_str = ""
            page_num = None
            chunk_index = None
            if positions and isinstance(positions, list) and len(positions) > 0:
                pos = positions[0]
                if isinstance(pos, list) and len(pos) >= 1:
                    if self._has_page_number(doc_type):
                        # PDF / PPT: first value is a real page or slide number
                        page_num = pos[0]
                        if self._has_coordinates(doc_type) and len(pos) >= 5:
                            location_str = f" | Page: {page_num} (Coordinates: x={pos[1]}..{pos[2]}, y={pos[3]}..{pos[4]})"
                        else:
                            location_str = f" | Slide/Page: {page_num}"
                    else:
                        # Excel / DOCX / other: chunk/row index only
                        chunk_index = pos[0]
                        location_str = f" | Chunk/Row index: {chunk_index}"

            type_label = f" [{doc_type.upper()}]" if doc_type else ""
            # We use [ID:N] to clearly link back to the [N] citations in the text
            print(f"[ID:{ref_id}] {doc_name}{type_label}{location_str}")

            # 2. Document/Source Links
            # Direct link to the document in RAGFlow
            if doc_id:
                # We use /v1/document/get/ which is the standard endpoint for fetching/viewing
                doc_url = f"{self.base_url}/v1/document/get/{doc_id}"
                if page_num is not None:
                    # Appending #page=N works for many browser-based PDF viewers
                    doc_url += f"#page={page_num}"
                print(f"    🔗 Doc Link: {doc_url}")
            
            # If it's a web page, it might have an external URL
            web_url = chunk.get("url")
            if web_url:
                print(f"    🌐 Source URL: {web_url}")

            # 3. Image Preview URL
            image_id = chunk.get("image_id")
            if image_id:
                # The image endpoint is typically /v1/document/image/<image_id>
                image_url = f"{self.base_url}/v1/document/image/{image_id}"
                print(f"    📷 Image Link: {image_url}")

            # 4. Snippet/Content
            content = chunk.get("content", "").strip()
            if content:
                snippet = (content[:150] + "...") if len(content) > 150 else content
                print(f"    📄 Snippet: {snippet}")
            
            print("-" * 40)
        
        # Optional: Check for hallucinated citations
        if cited_indices:
            available_ids = {r[0] for r in references}
            missing = [cid for cid in cited_indices if cid not in available_ids]
            if missing:
                print(f"NOTE: The assistant cited indices {missing} which were not found in the source documents.")

# ==========================================
# MAIN EXECUTION
# ==========================================

if __name__ == "__main__":
    client = RAGFlowChatClient(API_KEY, BASE_URL)
    
    try:
        # Create a new session or you can reuse an existing one
        session_id = client.create_session(CHAT_ID)
        
        # Simple loop for interaction
        while True:
            user_input = input("\nEnter your question (or 'quit' to exit): ")
            if user_input.lower() in ['quit', 'exit', 'q']:
                break
            
            client.ask(CHAT_ID, session_id, user_input, stream=STREAM)
            
    except Exception as e:
        print(f"\n[Error]: {e}")
        print("\nPlease ensure:")
        print("1. RAGFlow is running.")
        print("2. Your API_KEY is correct.")
        print("3. Your CHAT_ID is correct (found in the Chat Assistant's URL or API settings).")
