`performance/app.py` is a standalone RAGFlow load-testing UI.

Run it from the repo root:

```bash
python performance/app.py
```

Open `http://127.0.0.1:8787`.

Docker:

- build from the repo root:

```bash
docker build -f performance/Dockerfile performance -t ragflow-performance
```

- the Dockerfile is self-contained:
  - it inlines the current app code
  - it inlines the Python requirements
  - it does not copy any local files during the image build

- run on the default root path:

```bash
docker run --rm -p 8787:8787 ragflow-performance
```

- run on a custom subpath:

```bash
docker run --rm -p 8787:8787 \
  -e PERFORMANCE_APP_BASE_PATH=/tools/performance \
  ragflow-performance
```

- the Dockerfile installs Python packages from the internal company PyPI proxy by default:
  - `https://artifactor.cera.com/api/pypi/pypi/simple`
- override that at build time if needed:

```bash
docker build \
  --build-arg PIP_INDEX_URL=https://artifactor.cera.com/api/pypi/pypi/simple \
  --build-arg PIP_TRUSTED_HOST=artifactor.cera.com \
  -f performance/Dockerfile performance -t ragflow-performance
```

- the container exposes:
  - `PERFORMANCE_APP_PORT`
  - `PERFORMANCE_APP_BASE_PATH`

Browser persistence:

- the form stores non-file fields in browser `localStorage`
- uploaded files are not persisted by the browser and must be selected again after a page reload

TLS:

- leave `Verify SSL certificates` enabled to use the HTTP library default trust store
- disable it to turn certificate verification off for self-signed or otherwise untrusted endpoints
- insecure SSL warnings are suppressed when verification is disabled

What it does:

- uploads your files to a new RAGFlow dataset
- can start multiple parallel runs from one upload via `Parallel Runs`
- lets you enable or disable parsing, retrieval, and chat as separate stages
- starts parsing through the official dataset/document APIs when parsing is enabled
- polls document status until parsing finishes
- auto-generates prompts from parsed chunk samples when retrieval or chat is enabled
- benchmarks official retrieval calls when retrieval is enabled
- benchmarks official OpenAI-compatible chat completions when chat is enabled
- records app-controlled execution stage timings for provisioning, upload, parsing, prompt generation, retrieval, assistant setup, chat, summary, and cleanup
- stores each run as JSON under `performance/data/runs/`
- can export each run as a Word report with summaries, tables, and visualizations

Config model:

- `Dataset Options JSON` is applied through the UI KB flow: create (`/v1/kb/create`) and then detail/update (`/v1/kb/detail`, `/v1/kb/update`)
  - default example includes `parser_config.layout_recognize: "DeepDOC"`
  - layout values used by the main UI include `DeepDOC`, `Plain Text`, `Docling`, and `TCADP Parser`
  - compatibility remapping is applied so `chunk_method` becomes `parser_id` and `embedding_model` becomes `embd_id`
  - if `embd_id` is omitted, the app fetches the tenant default from `/v1/user/tenant_info`
- `Parsing Config JSON` controls parse request options plus local stage controls like `poll_interval_sec`, `chunk_sample_size`, and `collect_pipeline_logs`
- `Retrieval Config JSON` controls retrieval request options plus local stage controls like `concurrency`
- `Chat Config JSON` controls chat creation under `create`, chat completion under `completion`, and local stage controls like `concurrency`
  - default example includes `create.llm.model_name: null`
  - set it to a RAGFlow model ID in `model@provider` format if you want a specific chat model
- `Prompt Generation Settings` controls prompt volume with `prompts_per_document` and `shared_prompts`

Notes on stage behavior:

- retrieval and chat currently require parsing to be enabled in this upload-based workflow because prompts are generated from parsed chunks
- disabled stages are shown as skipped in the run view instead of reporting `0` metrics
- when `Parallel Runs` is greater than `1`, the app creates independent runs that reuse the same uploaded source files but provision separate remote datasets/chats for each run
- the results UI includes an execution-stage chart and timeline based on this app's own workflow rather than RagFlow pipeline canvases

Auth:

- provide a RAGFlow API key
- the app uses that key for both `/api/v1/...` official APIs and `/v1/...` UI endpoints

Notes:

- `cleanup_remote` removes the created dataset and chat assistant after the run
- local uploaded temp files are removed automatically after each run
