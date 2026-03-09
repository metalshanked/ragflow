`performance/app.py` is a standalone RAGFlow load-testing UI.

Run it from the repo root:

```bash
python performance/app.py
```

Open `http://127.0.0.1:8787`.

TLS:

- leave `Verify SSL certificates` enabled to use the HTTP library default trust store
- disable it to turn certificate verification off for self-signed or otherwise untrusted endpoints

What it does:

- uploads your files to a new RAGFlow dataset
- can queue multiple runs from one upload via `Run Count`
- lets you enable or disable parsing, retrieval, and chat as separate stages
- starts parsing through the official dataset/document APIs when parsing is enabled
- polls document status until parsing finishes
- collects parsing telemetry from the same KB pipeline log endpoints the RAGFlow UI uses
- auto-generates prompts from parsed chunk samples when retrieval or chat is enabled
- benchmarks official retrieval calls when retrieval is enabled
- benchmarks official OpenAI-compatible chat completions when chat is enabled
- stores each run as JSON under `performance/data/runs/`

Config model:

- `Dataset Options JSON` is merged into the UI KB create payload (`/v1/kb/create`)
  - default example includes `parser_config.layout_recognize: "DeepDOC"`
  - layout values used by the main UI include `DeepDOC`, `Plain Text`, `Docling`, and `TCADP Parser`
  - compatibility remapping is applied so `chunk_method` becomes `parser_id` and `embedding_model` becomes `embd_id`
- `Parsing Config JSON` controls parse request options plus local stage controls like `poll_interval_sec`, `chunk_sample_size`, and `collect_pipeline_logs`
- `Retrieval Config JSON` controls retrieval request options plus local stage controls like `concurrency`
- `Chat Config JSON` controls chat creation under `create`, chat completion under `completion`, and local stage controls like `concurrency`
  - default example includes `create.llm.model_name: null`
  - set it to a RAGFlow model ID in `model@provider` format if you want a specific chat model
- `Prompt Generation Settings` controls prompt volume with `prompts_per_document` and `shared_prompts`

Notes on stage behavior:

- retrieval and chat currently require parsing to be enabled in this upload-based workflow because prompts are generated from parsed chunks
- disabled stages are shown as skipped in the run view instead of reporting `0` metrics
- when `Run Count` is greater than `1`, the app creates independent runs that reuse the same uploaded source files but provision separate remote datasets/chats for each run

Auth:

- provide a RAGFlow API key
- the app uses that key for both `/api/v1/...` official APIs and `/v1/...` UI endpoints

Notes:

- `cleanup_remote` removes the created dataset and chat assistant after the run
- local uploaded temp files are removed automatically after each run
