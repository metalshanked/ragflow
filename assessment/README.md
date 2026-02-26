# RAGFlow Assessment API

A FastAPI wrapper application that uses RAGFlow's HTTP APIs to verify assessment questions against evidence documents. Upload an Excel file of questions and supporting evidence (PDFs, PPTX, XLSX, etc.), and the API will use RAGFlow's RAG-powered chat to answer each question with **Yes/No/N/A**, detailed explanations, and source references.

## Architecture

```
┌──────────────┐       ┌───────────────────┐       ┌─────────────┐
│  Client      │──────▶│  Assessment API    │──────▶│  RAGFlow    │
│  (curl/UI)   │◀──────│  (FastAPI)         │◀──────│  Server     │
└──────────────┘       └───────────────────┘       └─────────────┘
                              │
                        ┌─────┴─────┐
                        │ Background │
                        │ Pipeline   │
                        └───────────┘
```

### Upload Strategies

The API supports **three workflows** for running assessments:

| Strategy | Best for | Endpoints |
|---|---|---|
| **Single-call** | Small assessments where all docs are ready | `POST /assessments` |
| **From existing dataset(s)** | Dataset(s) already uploaded & parsed in RAGFlow | `POST /assessments/from-dataset` |
| **Two-phase** | Large/incremental uploads, unreliable networks | `POST /sessions` → `POST /sessions/{id}/documents` (repeat) → `POST /sessions/{id}/start` |

### Pipeline Flow

1. **Upload** – Questions Excel + evidence docs are received.
2. **Dataset Creation** – A RAGFlow dataset is created automatically.
3. **Document Upload** – Evidence files are uploaded to the dataset. The system performs **SHA256-based deduplication**: files with identical content (even with different names) are skipped if they were already uploaded in the current session.
4. **Parsing** – Document parsing is triggered and polled until complete.
5. **Chat Assistant** – A chat assistant is created and linked to the dataset with a Yes/No assessment prompt.
6. **Question Processing** – Each question is sent to the chat API concurrently (controlled by semaphore). Responses are parsed for verdict, details, and references.
7. **Results** – Available via paginated JSON or downloadable Excel.

> **Retry support:** If the pipeline fails at any stage (e.g. documents fail to parse), you can
> upload replacement or additional documents and re-start the assessment without losing the
> existing dataset or previously uploaded files. See [Error Handling & Retry](#error-handling--retry).

### File Deduplication & Redundancy Checks

To ensure efficiency and data integrity, the Assessment API implements **SHA256-based file deduplication**:

*   **Hash Calculation:** Every uploaded evidence file is hashed using SHA256 before processing.
*   **Duplicate Detection:** If a file with the same content has already been uploaded to the current assessment session (even with a different filename), it is identified as a duplicate.
*   **Handling:** Duplicate files are skipped automatically. The API response (and the UI) reports the count of successfully uploaded files vs. skipped duplicates.

This applies to both initial uploads and incremental uploads (Two-phase strategy).

## Quick Start

### 1. Install Dependencies

```bash
cd assessment
pip install -r requirements.txt
```

### Docker

Build and run with Docker:

```bash
# Build the image (from the project root)
docker build -t ragflow-assessment ./assessment

# Run the container
docker run -p 8000:8000 \
  -e ASSESSMENT_RAGFLOW_BASE_URL=http://ragflow-host:9380 \
  -e ASSESSMENT_RAGFLOW_API_KEY=ragflow-your-api-key-here \
  ragflow-assessment

# With all optional settings
docker run -p 8000:8000 \
  -e ASSESSMENT_RAGFLOW_BASE_URL=http://ragflow-host:9380 \
  -e ASSESSMENT_RAGFLOW_API_KEY=ragflow-your-api-key-here \
  -e ASSESSMENT_JWT_SECRET_KEY=my-super-secret-key \
  -e ASSESSMENT_API_BASE_PATH=/assessment \
  -e ASSESSMENT_VERIFY_SSL=false \
  -e ASSESSMENT_MAX_CONCURRENT_QUESTIONS=10 \
  ragflow-assessment

# Scale with multiple workers
docker run -p 8000:8000 \
  -e ASSESSMENT_RAGFLOW_BASE_URL=http://ragflow-host:9380 \
  -e ASSESSMENT_RAGFLOW_API_KEY=ragflow-your-api-key-here \
  ragflow-assessment \
  uvicorn assessment.main:app --host 0.0.0.0 --port 8000 --workers 4

# Mount a custom CA certificate
docker run -p 8000:8000 \
  -v /path/to/ca-bundle.pem:/certs/ca-bundle.pem:ro \
  -e ASSESSMENT_SSL_CA_CERT=/certs/ca-bundle.pem \
  -e ASSESSMENT_RAGFLOW_BASE_URL=https://ragflow-host:9380 \
  -e ASSESSMENT_RAGFLOW_API_KEY=ragflow-your-api-key-here \
  ragflow-assessment
```

### 2. Configure

Set environment variables (or create a `.env` file in the project root):

```bash
export ASSESSMENT_RAGFLOW_BASE_URL=http://localhost:9380
export ASSESSMENT_RAGFLOW_API_KEY=ragflow-your-api-key-here

# Optional: serve under a subpath (e.g. behind a reverse proxy)
export ASSESSMENT_API_BASE_PATH=/assessment

# Optional: enable JWT auth signing key (required for LDAP login/token mode)
export ASSESSMENT_JWT_SECRET_KEY=my-super-secret-key

# Optional: LDAP / Active Directory login mode
export ASSESSMENT_LDAP_SERVER_URI=ldap://dc1.example.local:389
export ASSESSMENT_LDAP_USER_BASE_DN="OU=Users,DC=example,DC=local"
export ASSESSMENT_LDAP_GROUP_ROLE_MAPPING_JSON='{"viewer":["RGF-Readers"],"operator":["RGF-Operators"],"admin":["RGF-Admins"]}'

# Optional: SSL / TLS settings for RAGFlow connection
export ASSESSMENT_VERIFY_SSL=false                   # disable SSL verification (e.g. self-signed certs)
export ASSESSMENT_SSL_CA_CERT=/path/to/ca-bundle.pem  # or point to a custom CA / self-signed cert
```

All settings (see `config.py`):

| Variable | Default | Description |
|---|---|---|
| `ASSESSMENT_RAGFLOW_BASE_URL` | `http://localhost:9380` | RAGFlow server URL |
| `ASSESSMENT_RAGFLOW_API_KEY` | (empty) | RAGFlow API key |
| `ASSESSMENT_MAX_CONCURRENT_QUESTIONS` | `5` | Parallel question processing limit |
| `ASSESSMENT_POLLING_INTERVAL_SECONDS` | `3.0` | Seconds between parsing status polls |
| `ASSESSMENT_DOCUMENT_PARSE_TIMEOUT_SECONDS` | `600.0` | Max wait for document parsing |
| `ASSESSMENT_DEFAULT_SIMILARITY_THRESHOLD` | `0.1` | RAG similarity threshold |
| `ASSESSMENT_DEFAULT_TOP_N` | `8` | Number of chunks to retrieve |
| `ASSESSMENT_QUESTION_ID_COLUMN` | `A` | Default Excel column for Question Serial No (letter like `A` or 1-based number like `1`) |
| `ASSESSMENT_QUESTION_COLUMN` | `B` | Default Excel column for Question text (letter like `B` or 1-based number like `2`) |
| `ASSESSMENT_VENDOR_RESPONSE_COLUMN` | `C` | Default Excel column for Vendor response (letter like `C` or 1-based number like `3`) |
| `ASSESSMENT_VENDOR_COMMENT_COLUMN` | `D` | Default Excel column for Vendor comments (letter like `D` or 1-based number like `4`) |
| `ASSESSMENT_PROCESS_VENDOR_RESPONSE` | `false` | If `true`, verify vendor response and comments in determining results. |
| `ASSESSMENT_ONLY_CITED_REFERENCES` | `true` | If `true` (default), only references actually cited as `[ID:N]` in the LLM answer are included in results. Set to `false` to return all retrieved chunks. |
| `ASSESSMENT_HOST` | `0.0.0.0` | Server bind host |
| `ASSESSMENT_PORT` | `8000` | Server bind port |
| `ASSESSMENT_API_BASE_PATH` | (empty) | Subpath prefix, e.g. `/assessment` |
| `ASSESSMENT_JWT_SECRET_KEY` | (empty) | JWT signing key; empty = auth disabled |
| `ASSESSMENT_JWT_ALGORITHM` | `HS256` | JWT algorithm for access/refresh tokens |
| `ASSESSMENT_JWT_ACCESS_TOKEN_TTL_MINUTES` | `30` | Access token TTL in minutes |
| `ASSESSMENT_JWT_REFRESH_TOKEN_TTL_MINUTES` | `10080` | Refresh token TTL in minutes |
| `ASSESSMENT_LDAP_SERVER_URI` | (empty) | LDAP/AD server URI (set to enable LDAP login endpoints) |
| `ASSESSMENT_LDAP_USE_SSL` | `false` | Use LDAPS (`ldaps://`) |
| `ASSESSMENT_LDAP_START_TLS` | `false` | Upgrade LDAP connection with StartTLS |
| `ASSESSMENT_LDAP_VERIFY_SSL` | `true` | Verify LDAP TLS certificates |
| `ASSESSMENT_LDAP_CA_CERT` | (empty) | Custom CA cert path for LDAP TLS |
| `ASSESSMENT_LDAP_USER_DN_TEMPLATE` | (empty) | Direct-bind template, e.g. `user@domain.local` |
| `ASSESSMENT_LDAP_BIND_DN` | (empty) | Service-account bind DN for LDAP search mode |
| `ASSESSMENT_LDAP_BIND_PASSWORD` | (empty) | Service-account bind password |
| `ASSESSMENT_LDAP_USER_BASE_DN` | (empty) | User search base DN |
| `ASSESSMENT_LDAP_USER_FILTER` | `(|(sAMAccountName={username})(uid={username})(cn={username}))` | User search LDAP filter |
| `ASSESSMENT_LDAP_GROUP_MEMBER_ATTRIBUTE` | `memberOf` | Group membership attribute on user object |
| `ASSESSMENT_LDAP_GROUP_SEARCH_BASE_DN` | (empty) | Optional group search base DN |
| `ASSESSMENT_LDAP_GROUP_SEARCH_FILTER` | `(|(member={user_dn})(memberUid={username}))` | Optional group search filter |
| `ASSESSMENT_LDAP_GROUP_NAME_ATTRIBUTE` | `cn` | Group name attribute used for mapping |
| `ASSESSMENT_LDAP_GROUP_ROLE_MAPPING_JSON` | `{"viewer":[],"operator":[],"admin":[]}` | LDAP group-to-role mapping JSON |
| `ASSESSMENT_LDAP_REQUIRE_MAPPED_ROLES` | `true` | Require at least one mapped role after LDAP auth |
| `ASSESSMENT_DATABASE_URL` | `sqlite+aiosqlite:///./assessment.db` | Database URL (SQLite default; see [Database Persistence](#database-persistence) for PostgreSQL) |
| `ASSESSMENT_DATABASE_BOOTSTRAP_MODE` | `create` | `create` = create missing tables only; `recreate` = drop and recreate all assessment tables at startup |
| `ASSESSMENT_DATABASE_ALLOW_DESTRUCTIVE_RECREATE` | `false` | Required to allow destructive `recreate` bootstrap on PostgreSQL |
| `ASSESSMENT_VERIFY_SSL` | `true` | Set `false` to skip SSL certificate verification |
| `ASSESSMENT_SSL_CA_CERT` | (empty) | Path to a custom CA bundle or self-signed certificate (PEM) |
| `ASSESSMENT_TASK_RETENTION_DAYS` | `0` | Auto-delete task rows older than this many days; `0` = disabled (kept forever) |
| `ASSESSMENT_TASK_CLEANUP_INTERVAL_HOURS` | `24.0` | How often the cleanup job runs (in hours) |
| `ASSESSMENT_LOG_LEVEL` | `INFO` | Log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `ASSESSMENT_LOG_JSON` | `true` | Emit JSON-formatted logs |
| `ASSESSMENT_LOG_TO_CONSOLE` | `true` | Write logs to stdout/stderr |
| `ASSESSMENT_LOG_FILE_ENABLED` | `true` | Enable local file logging |
| `ASSESSMENT_LOG_DIR` | `./logs` | Directory for local log files |
| `ASSESSMENT_LOG_FILE_NAME` | `assessment.log` | Active log file name |
| `ASSESSMENT_LOG_MAX_BYTES` | `20971520` | Rotate log file after this size in bytes |
| `ASSESSMENT_LOG_BACKUP_COUNT` | `30` | Number of rotated files to retain |
| `ASSESSMENT_OTEL_ENABLED` | `false` | Enable OpenTelemetry initialization/instrumentation |
| `ASSESSMENT_OTEL_EXPORTER_OTLP_ENDPOINT` | (empty) | Base OTLP HTTP endpoint, e.g. `http://otel-collector:4318` |
| `ASSESSMENT_OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | (empty) | Optional full traces endpoint override |
| `ASSESSMENT_OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` | (empty) | Optional full logs endpoint override |
| `ASSESSMENT_OTEL_EXPORTER_OTLP_HEADERS` | (empty) | OTLP headers (JSON object or comma `k=v` list) |
| `ASSESSMENT_OTEL_SERVICE_NAME` | `assessment-api` | OTel service name |
| `ASSESSMENT_OTEL_SERVICE_VERSION` | `1.0.0` | OTel service version |
| `ASSESSMENT_OTEL_RESOURCE_ATTRIBUTES_JSON` | `{}` | Extra resource attributes JSON |
| `ASSESSMENT_OTEL_SAMPLE_RATIO` | `1.0` | Trace sampling ratio (0.0-1.0) |
| `ASSESSMENT_OTEL_EXPORT_TRACES` | `true` | Export traces to OTLP |
| `ASSESSMENT_OTEL_EXPORT_LOGS` | `true` | Export logs to OTLP |
| `ASSESSMENT_OTEL_ENABLE_CONSOLE_EXPORTER` | `false` | Also export traces/logs to console exporter |
| `ASSESSMENT_OTEL_INSTRUMENT_FASTAPI` | `true` | Enable FastAPI auto instrumentation |
| `ASSESSMENT_OTEL_INSTRUMENT_HTTPX` | `true` | Enable HTTPX auto instrumentation |
| `ASSESSMENT_OTEL_INSTRUMENT_SQLALCHEMY` | `true` | Enable SQLAlchemy auto instrumentation |
| `ASSESSMENT_OPENINFERENCE_ENABLED` | `true` | Add OpenInference semantic attributes/context |

### Observability (OpenTelemetry + OpenInference)

The Assessment API supports end-to-end tracing and log export via OpenTelemetry:

- Incoming API spans (FastAPI)
- Outbound RAGFlow HTTP spans (HTTPX)
- Database spans (SQLAlchemy)
- Application-level pipeline spans (`run_assessment`, per-question processing, etc.)
- OpenInference semantic span attributes for Phoenix-compatible LLM workflow analysis

Enable and export to an OTLP collector:

```bash
export ASSESSMENT_OTEL_ENABLED=true
export ASSESSMENT_OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
export ASSESSMENT_OTEL_SERVICE_NAME=assessment-api
export ASSESSMENT_OTEL_SERVICE_VERSION=1.0.0
```

For Arize Phoenix (OTLP ingest), point the endpoint to your Phoenix OTLP collector URL.

### Log Rotation

File logging uses built-in rotating handlers:

- Active file: `${ASSESSMENT_LOG_DIR}/${ASSESSMENT_LOG_FILE_NAME}`
- Rotation trigger: `ASSESSMENT_LOG_MAX_BYTES`
- Retention count: `ASSESSMENT_LOG_BACKUP_COUNT`

This keeps logs self-contained for long-running deployments while still allowing OTLP export to external backends.

### Database Persistence

By default the assessment API stores task state (IDs, status, metadata, results) in a **SQLite** database file.
This is suitable for single-instance deployments and can be mounted on a Docker volume or Kubernetes PersistentVolume.

**SQLite** requires no external setup — the database file and tables are created automatically on first startup.

To avoid schema migration tooling in controlled environments, you can opt into destructive bootstrap:

```bash
export ASSESSMENT_DATABASE_BOOTSTRAP_MODE=recreate
```

`recreate` drops and recreates all assessment tables at startup. Use only when data loss is acceptable.

For PostgreSQL, destructive recreate is blocked by default. To allow it intentionally:

```bash
export ASSESSMENT_DATABASE_ALLOW_DESTRUCTIVE_RECREATE=true
```

#### Using PostgreSQL

No manual database creation is needed. On first startup the application connects to the
PostgreSQL server's default `postgres` database and automatically creates the target
database (e.g. `assessment`) if it does not already exist. Tables are then created inside
it — so the entire setup is **fully automatic**.

> **Prerequisite:** The PostgreSQL **user** specified in the URL must have the
> `CREATEDB` privilege (or be a superuser). If your DBA restricts this, create
> the database manually once and the app will simply skip the creation step.

The `asyncpg` driver is included by default and is the recommended choice for this async FastAPI application.

```bash
export ASSESSMENT_DATABASE_URL=postgresql+asyncpg://user:password@db-host:5432/assessment
```

#### Docker with persistent SQLite

Data survives container restarts when you mount a volume:

```bash
docker run -p 8000:8000 \
  -v assessment-data:/app/data \
  -e ASSESSMENT_RAGFLOW_BASE_URL=http://ragflow-host:9380 \
  -e ASSESSMENT_RAGFLOW_API_KEY=ragflow-your-api-key-here \
  ragflow-assessment
```

#### Kubernetes PersistentVolume example

```yaml
volumeMounts:
  - name: assessment-db
    mountPath: /app/data
volumes:
  - name: assessment-db
    persistentVolumeClaim:
      claimName: assessment-pvc
```

#### Automatic Cleanup of Old Records

Set `ASSESSMENT_TASK_RETENTION_DAYS` to automatically purge task rows (and their results) and old task-event audit rows older than the specified number of days. A background job runs on a configurable interval (`ASSESSMENT_TASK_CLEANUP_INTERVAL_HOURS`, default 24 h).

```bash
# Delete tasks older than 30 days, check every 12 hours
export ASSESSMENT_TASK_RETENTION_DAYS=30
export ASSESSMENT_TASK_CLEANUP_INTERVAL_HOURS=12
```

When set to `0` (the default), no automatic cleanup is performed and rows are kept indefinitely.

When running **multiple instances** (e.g. Kubernetes pods), the cleanup job uses a
PostgreSQL **advisory lock** (`pg_try_advisory_xact_lock`) so that only one instance
performs the purge at any given time — the others skip that cycle gracefully.

#### Horizontal Scaling

The assessment API is designed to run as **multiple replicas** behind a load balancer.
All task state is stored in the database — there is no in-memory cache that could drift
between pods.

| Concern | How it's handled |
|---|---|
| **Task state** | Every read/write goes through the database; no in-memory cache. Any pod can serve any request. |
| **Background pipelines** | A pipeline runs on the pod that received the request. Status updates are persisted to the DB and visible from all pods. |
| **Auto-cleanup** | Uses a PostgreSQL **advisory lock** so only one pod purges old rows per cycle; others skip gracefully. |
| **Database choice** | **PostgreSQL is required** for multi-instance deployments. SQLite does not support concurrent writes and must only be used with a single instance. |

> **Important:** SQLite is a single-writer database and is **not suitable** for
> horizontal scaling. Switch to PostgreSQL when running more than one replica.

### 3. Run

```bash
# From the ragflow project root
uvicorn assessment.main:app --host 0.0.0.0 --port 8000 --reload
```

Interactive docs at: **http://localhost:8000/docs**

If `ASSESSMENT_API_BASE_PATH` is set (e.g. `/assessment`), docs are at **http://localhost:8000/assessment/docs** and all endpoints are served under that prefix.

### Web UI

A built-in browser dashboard is available at **`/ui`** (or `/<base_path>/ui` when `ASSESSMENT_API_BASE_PATH` is set):

- **http://localhost:8000/ui** (default)
- **http://localhost:8000/assessment/ui** (when `ASSESSMENT_API_BASE_PATH=/assessment`)

The UI is a single-page app served directly from FastAPI — no extra dependencies or build steps required. It provides:

| Feature | Description |
|---|---|
| **Tasks list** | View all assessment tasks with state, stage, progress, and creation time; auto-refresh with configurable interval |
| **Task detail** | Drill into any task to see full status, RAGFlow resource IDs, per-document parsing status table, results with pagination, and download Excel |
| **Single-call assessment** | Upload questions Excel + evidence documents in one step |
| **From existing dataset** | Run assessment against already-uploaded RAGFlow datasets |
| **Two-phase workflow** | Create session → upload docs incrementally → start assessment |
| **Document upload** | Upload documents to an existing RAGFlow dataset |
| **Data Management** | List, paginate, and delete datasets and documents directly from the dashboard |
| **Health check** | View API and RAGFlow connection status |
| **LDAP login / token refresh** | Sign in with LDAP username/password, refresh tokens, verify token status, and logout from the dashboard |
| **Retry panel** | Upload replacement documents and re-start failed assessments directly from the task detail view |

> **Note:** The UI endpoint (`/ui`) is **not** behind JWT authentication — it is a static HTML page.
> All API calls made *from* the UI include the bearer token when authenticated via the login bar.

## Authentication

When `ASSESSMENT_JWT_SECRET_KEY` is set, **all** API endpoints require a valid JWT token in the `Authorization` header:

```
Authorization: Bearer <token>
```

### LDAP / AD Token Endpoints

If `ASSESSMENT_LDAP_SERVER_URI` is configured, use these endpoints:

- `POST /api/v1/auth/token` with `{ "username": "...", "password": "..." }` to authenticate against LDAP and receive access/refresh JWTs.
- `POST /api/v1/auth/refresh` with `{ "refresh_token": "..." }` to rotate/refresh session tokens.
- `GET /api/v1/auth/verify` with bearer access token to validate token and return identity/roles/groups.

LDAP group mapping supports users in multiple groups. Matched groups are merged into role grants (`viewer`, `operator`, `admin`).

Role permissions:

- `viewer`: read-only API usage (`GET`/`HEAD`/`OPTIONS`)
- `operator`: everything in `viewer` plus write/execute flows (`POST`/`PUT`/`PATCH`) for assessment operations
- `admin`: full access, including destructive endpoints (`DELETE`) and direct RAGFlow passthrough (`/api/v1/ragflow/*`)

### Legacy Manual JWT

If LDAP mode is not enabled, you can still generate JWTs manually:

```python
import jwt
token = jwt.encode({"sub": "assessment-client"}, "my-super-secret-key", algorithm="HS256")
print(token)
```

When the secret is **empty** (default), authentication is disabled and all requests are allowed.

The `/health` endpoint is **not** behind auth.

## API Endpoints

### Option A: Single-call Flow (all docs at once)

```
POST /api/v1/assessments
Content-Type: multipart/form-data
```

**Form fields:**
- `questions_file` (file, required) – Excel with columns A = `Question_Serial_No`, B = `Question`
- `evidence_files` (file[], required) – One or more evidence documents
- `dataset_name` (string, optional) – Custom RAGFlow dataset name
- `chat_name` (string, optional) – Custom chat assistant name
- `question_id_column` (string, optional) – Column for Question Serial No (letter e.g. `C` or 1-based number e.g. `3`). Defaults to server setting.
- `question_column` (string, optional) – Column for Question text (letter e.g. `D` or 1-based number e.g. `4`). Defaults to server setting.
- `vendor_response_column` (string, optional) – Column for Vendor response (letter e.g. `E` or 1-based number e.g. `5`). Defaults to server setting.
- `vendor_comment_column` (string, optional) – Column for Vendor comments (letter e.g. `F` or 1-based number e.g. `6`). Defaults to server setting.
- `process_vendor_response` (boolean, optional) – If true, verify vendor response and comments in determining results. Defaults to server setting.
- `only_cited_references` (boolean, optional) – If true (default), only include references actually cited as `[ID:N]` in the LLM answer. Set to false to return all retrieved chunks. Defaults to server setting.

**Response** (202 Accepted):
```json
{
  "task_id": "abc123...",
  "state": "pending",
  "pipeline_stage": "idle",
  "total_questions": 25,
  "questions_processed": 0
}
```

---

### Option B: From Existing Dataset(s) (skip upload & parsing)

Use this when your evidence documents are already uploaded and parsed in one or more RAGFlow datasets.

```
POST /api/v1/assessments/from-dataset
Content-Type: multipart/form-data
```

**Form fields:**
- `questions_file` (file, required) – Excel with columns A = `Question_Serial_No`, B = `Question`
- `dataset_ids` (string, required) – One or more existing RAGFlow dataset IDs. Pass a single ID or multiple comma-separated IDs (e.g. `id1,id2,id3`). Documents must already be uploaded & parsed.
- `chat_name` (string, optional) – Custom chat assistant name
- `question_id_column` (string, optional) – Column for Question Serial No (letter e.g. `C` or 1-based number e.g. `3`). Defaults to server setting.
- `question_column` (string, optional) – Column for Question text (letter e.g. `D` or 1-based number e.g. `4`). Defaults to server setting.
- `vendor_response_column` (string, optional) – Column for Vendor response (letter e.g. `E` or 1-based number e.g. `5`). Defaults to server setting.
- `vendor_comment_column` (string, optional) – Column for Vendor comments (letter e.g. `F` or 1-based number e.g. `6`). Defaults to server setting.
- `process_vendor_response` (boolean, optional) – If true, verify vendor response and comments in determining results. Defaults to server setting.
- `only_cited_references` (boolean, optional) – If true (default), only include references actually cited as `[ID:N]` in the LLM answer. Set to false to return all retrieved chunks. Defaults to server setting.

**Response** (202 Accepted):
```json
{
  "task_id": "abc123...",
  "state": "pending",
  "pipeline_stage": "idle",
  "total_questions": 25,
  "questions_processed": 0
}
```

The pipeline skips dataset creation, document upload, and parsing — it creates a chat assistant linked to the existing dataset(s) and starts processing questions immediately. This is significantly faster.

Multiple datasets are useful when evidence is spread across different knowledge bases (e.g. policies in one dataset, audit reports in another).

---

### Option C: Two-phase Flow (incremental uploads)

Use this when you have many/large evidence documents and cannot upload them all at once.

#### Phase 1 – Create Session

```
POST /api/v1/assessments/sessions
Content-Type: multipart/form-data
```

**Form fields:**
- `questions_file` (file, required) – Excel with columns A = `Question_Serial_No`, B = `Question`
- `dataset_name` (string, optional) – Custom RAGFlow dataset name
- `question_id_column` (string, optional) – Column for Question Serial No (letter e.g. `C` or 1-based number e.g. `3`). Defaults to server setting.
- `question_column` (string, optional) – Column for Question text (letter e.g. `D` or 1-based number e.g. `4`). Defaults to server setting.
- `vendor_response_column` (string, optional) – Column for Vendor response (letter e.g. `E` or 1-based number e.g. `5`). Defaults to server setting.
- `vendor_comment_column` (string, optional) – Column for Vendor comments (letter e.g. `F` or 1-based number e.g. `6`). Defaults to server setting.

**Response** (201 Created):
```json
{
  "task_id": "abc123...",
  "dataset_id": "ds_456...",
  "state": "awaiting_documents",
  "message": "Session created. Upload evidence documents then start the assessment."
}
```

#### Phase 2 – Upload Documents (repeatable)

```
POST /api/v1/assessments/sessions/{task_id}/documents
Content-Type: multipart/form-data
```

**Form fields:**
- `files` (file[], required) – One or more evidence documents

Call this endpoint **as many times as needed** — each call adds documents to the session's dataset.

**Response** (200 OK):
```json
{
  "task_id": "abc123...",
  "dataset_id": "ds_456...",
  "uploaded_document_ids": ["doc_1", "doc_2"],
  "total_documents": 5,
  "message": "Uploaded 2 document(s). Total: 5."
}
```

#### Phase 3 – Start Assessment

```
POST /api/v1/assessments/sessions/{task_id}/start
Content-Type: multipart/form-data
```

**Form fields:**
- `chat_name` (string, optional) – Custom chat assistant name
- `process_vendor_response` (boolean, optional) – If true, verify vendor response and comments in determining results. Defaults to server setting.
- `only_cited_references` (boolean, optional) – If true (default), only include references actually cited as `[ID:N]` in the LLM answer. Set to false to return all retrieved chunks. Defaults to server setting.

**Response** (202 Accepted):
```json
{
  "task_id": "abc123...",
  "state": "awaiting_documents",
  "pipeline_stage": "idle",
  "total_questions": 25,
  "questions_processed": 0
}
```

The pipeline starts in the background. Poll `GET /assessments/{task_id}` for progress.

---

### Common Endpoints

#### Check Task Status

```
GET /api/v1/assessments/{task_id}
```

The `pipeline_stage` field tells you exactly what the system is doing:
- `document_upload` – Uploading documents to RAGFlow
- `document_parsing` – Waiting for RAGFlow to parse/chunk documents
- `chat_processing` – Sending questions to the chat assistant
- `finalizing` – Complete

The `state` field tracks the overall lifecycle:
- `pending` – Task created, pipeline not yet started
- `awaiting_documents` – Session created, waiting for document uploads (two-phase only)
- `uploading` – Uploading documents to RAGFlow
- `parsing` – Documents being parsed
- `processing` – Questions being processed
- `completed` – Done
- `failed` – Error occurred

The `document_statuses` array provides **per-document parsing status**, so you can see exactly which documents succeeded, failed, or timed out:

```json
{
  "document_statuses": [
    {"document_id": "d1", "document_name": "policy.pdf", "status": "success", "progress": 1.0, "message": "Parsed successfully"},
    {"document_id": "d2", "document_name": "corrupt.xlsx", "status": "failed", "progress": 0.0, "message": "Unsupported format"},
    {"document_id": "d3", "document_name": "large.docx", "status": "timeout", "progress": 0.6, "message": "Document parsing timed out"}
  ]
}
```

Possible per-document statuses:
- `success` – Parsed successfully
- `failed` – RAGFlow reported a parsing failure (e.g. unsupported format, corrupt file)
- `timeout` – Document did not finish parsing within `ASSESSMENT_DOCUMENT_PARSE_TIMEOUT_SECONDS`
- `not_found` – Document ID was not found in the dataset listing

> **Partial failure handling:** If some documents fail but at least one succeeds, the pipeline **continues** with the successfully parsed documents. The task only fails if *all* documents fail to parse. Failed document details are always visible in the task status and results responses.
>
> **Retry after failure:** When a task is in `failed` state, you can upload additional or replacement
> documents via the two-phase upload endpoint and re-start the assessment. The existing dataset and
> previously uploaded documents are preserved. See [Error Handling & Retry](#error-handling--retry).

#### Get Task Events (Audit Trail)

Retrieve task lifecycle/audit events (newest first).

**Query Parameters:**
- `page` (int, default 1): Page number.
- `page_size` (int, default 100): Events per page.

```
GET /api/v1/assessments/{task_id}/events?page=1&page_size=100
```

Use this endpoint to troubleshoot pipeline transitions (`task_created`, `status_update`, errors, etc.).

#### Get Results (JSON, Paginated)

Retrieve assessment results in JSON format.

**Query Parameters:**
- `page` (int, default 1): Page number.
- `page_size` (int, default 50): Results per page.

```
GET /api/v1/assessments/{task_id}/results?page=1&page_size=50
```

**Response:**
```json
{
  "task_id": "abc123...",
  "state": "completed",
  "total_questions": 25,
  "questions_processed": 25,
  "results": [
    {
      "question_serial_no": 1,
      "question": "Does the organization have a data privacy policy?",
      "ai_response": "Yes",
      "details": "The evidence document contains a comprehensive data privacy policy...",
      "references": [
        {
          "document_name": "privacy_policy.pdf",
          "document_type": "pdf",
          "page_number": 3,
          "chunk_index": null,
          "coordinates": [120.0, 540.0, 80.0, 400.0],
          "snippet": "Section 2.1 defines the organization's approach to data privacy...",
          "document_url": "/api/v1/proxy/document/doc123#page=3",
          "image_url": "/api/v1/proxy/image/img456"
        },
        {
          "document_name": "Evidence_List.xlsx",
          "document_type": "excel",
          "page_number": null,
          "chunk_index": 36,
          "coordinates": null,
          "snippet": "P07: Physical – Access Controls ...",
          "document_url": "/api/v1/proxy/document/doc789",
          "image_url": null
        }
      ]
    }
  ],
  "page": 1,
  "page_size": 50,
  "total_pages": 1
}
```

#### Download Results as Excel

```
GET /api/v1/assessments/{task_id}/results/excel
```

Returns an `.xlsx` file with columns: `Question_Serial_No`, `Question`, `Vendor_Response`, `Vendor_Comment`, `AI_Response`, `Details`, `References`.

#### Proxy: RAGFlow Image

```
GET /api/v1/proxy/image/{image_id}
```

Proxies chunk images from RAGFlow so clients never see raw RAGFlow URLs. Reference results include these proxy URLs automatically.

#### Proxy: RAGFlow Document

```
GET /api/v1/proxy/document/{document_id}
```

Proxies document downloads from RAGFlow. Reference `document_url` fields point here.

#### Upload Documents (Standalone)

```
POST /api/v1/ragflow/documents/upload
Content-Type: multipart/form-data
```

Upload documents to an existing RAGFlow dataset (not tied to an assessment session).

#### List All Tasks

Retrieve a paginated list of all assessment tasks.

**Query Parameters:**
- `page` (int, default 1): Page number.
- `page_size` (int, default 50): Tasks per page.

```
GET /api/v1/assessments?page=1&page_size=50
```

**Response:**
```json
{
  "tasks": [
    {
      "task_id": "abc123...",
      "state": "completed",
      "pipeline_stage": "finalizing",
      "progress_message": "Assessment completed",
      "total_questions": 25,
      "questions_processed": 25,
      "dataset_id": "ds-xyz",
      "chat_id": "ch-123",
      "created_at": "2024-03-20T10:00:00",
      "updated_at": "2024-03-20T10:05:00"
    }
  ],
  "total": 1,
  "page": 1,
  "page_size": 50,
  "total_pages": 1
}
```

#### Health Check

```
GET /health
```

#### Data Management

Manage datasets and documents programmatically.

**List Datasets:**
```
GET /api/v1/ragflow/datasets?page=1&page_size=100&name=optional_filter
```
Returns `{"items": [...], "total": N}`.

**Delete Datasets:**
```
DELETE /api/v1/ragflow/datasets
Content-Type: application/json
{ "ids": ["dataset_id_1", "dataset_id_2"] }
```

**List Documents:**
```
GET /api/v1/ragflow/datasets/{dataset_id}/documents?page=1&page_size=100
```
Returns `{"items": [...], "total": N}`.

**Delete Documents:**
```
DELETE /api/v1/ragflow/datasets/{dataset_id}/documents
Content-Type: application/json
{ "ids": ["doc_id_1", "doc_id_2"] }
```

## Citations & References

### How Citations Work

When the LLM answers a question, it may cite source chunks using `[ID:N]` markers in its response text (e.g. `[ID:0]`, `[ID:2]`). These markers correspond to 0-based indices into the list of retrieved reference chunks.

### Cited-Only Filtering (`only_cited_references`)

By default (`only_cited_references=true`), the API filters the references returned with each answer to include **only** those chunks actually cited in the LLM’s response. This keeps results concise and directly relevant.

- **Default behaviour (true):** Only references with a matching `[ID:N]` citation in the answer text are included. If the LLM does not cite any references, all retrieved chunks are returned as a fallback.
- **When set to false:** All retrieved chunks are returned regardless of whether they were cited.

This can be controlled at three levels (highest priority first):
1. **Per-request** – Pass `only_cited_references=true` or `false` as a form field on any assessment endpoint.
2. **Server-wide default** – Set the `ASSESSMENT_ONLY_CITED_REFERENCES` environment variable.
3. **Built-in default** – `true`.

### Document-Type-Aware References

The reference model adapts its fields based on the source document type. RAGFlow stores positional data differently depending on the file format:

| Document type | `page_number` | `chunk_index` | `coordinates` | `image_url` |
|---|---|---|---|---|
| **PDF** | ✔ Real page number | `null` | ✔ Bounding box `[x1, x2, y1, y2]` | ✔ Chunk image preview |
| **Excel** | `null` | ✔ Row/chunk index (0-based) | `null` | `null` |
| **DOCX** | `null` | ✔ Chunk index (0-based) | `null` | `null` |
| **PPT/PPTX** | ✔ Real slide number | `null` | `null` | `null` |
| **Other** (md, txt, html…) | `null` | ✔ Chunk index (0-based) | `null` | `null` |

**How it works internally:** RAGFlow’s `positions` field is a list of `[a, b, c, d, e]` arrays. For PDFs, this encodes `[page, x1, x2, y1, y2]` with real page numbers and bounding-box coordinates. For PPT/PPTX, RAGFlow stores `[slide, 0, 0, 0, 0]` with real slide numbers but zero coordinates. For Excel, DOCX, and other non-PDF/non-PPT documents, RAGFlow stores `[index, index, index, index, index]` where all five values are identical — representing a chunk or row counter, **not** a page number. The API detects the document type from the file extension and populates `page_number` + `coordinates` (PDF), `page_number` only (PPT), or `chunk_index` (everything else).

### Reference Response Example

```json
{
  "references": [
    {
      "document_name": "privacy_policy.pdf",
      "document_type": "pdf",
      "page_number": 3,
      "chunk_index": null,
      "coordinates": [120.0, 540.0, 80.0, 400.0],
      "snippet": "Section 2.1 defines the organization's approach to data privacy...",
      "document_url": "/api/v1/proxy/document/doc123#page=3",
      "image_url": "/api/v1/proxy/image/img456"
    },
    {
      "document_name": "Evidence_List.xlsx",
      "document_type": "excel",
      "page_number": null,
      "chunk_index": 36,
      "coordinates": null,
      "snippet": "P07: Physical – Access Controls ...",
      "document_url": "/api/v1/proxy/document/doc789",
      "image_url": null
    }
  ]
}
```

> **Note on the UI:** The RAGFlow built-in web UI (`web/src/components/`) has its own reference rendering logic that is separate from this Assessment API. The Assessment API’s citation filtering (`only_cited_references`) applies only to the Assessment API endpoints and its built-in dashboard at `/ui`. The main RAGFlow chat UI renders references using its own component (`reference-document-list.tsx`) and is not affected by these assessment settings.

### Python Example

For a complete working example of how to interact with RAGFlow’s chat API, parse `[ID:N]` citations, detect document types from the `positions` field, and filter to cited-only references, see [`example/python_headless_chat.py`](../example/python_headless_chat.py).

## Error Handling & Retry

### Comprehensive Error Handling

The API implements structured error handling at every layer to ensure graceful failure:

| Layer | What's handled |
|---|---|
| **RAGFlow client** (`ragflow_client.py`) | Connection errors, timeouts, HTTP status errors, and non-JSON responses are caught and converted to descriptive `RuntimeError` messages. Includes workarounds for API bugs where non-existent items are reported as permission or data errors. |
| **API routers** (`routers.py`) | Proxy and upload endpoints catch `ConnectError` (→ 502), `TimeoutException` (→ 504), and generic HTTP errors (→ 502) with structured JSON responses |
| **Service pipeline** (`services.py`) | All pipeline failures are caught, logged, and stored on the task record with state `failed` and a human-readable `error` message |
| **Global handler** (`main.py`) | A catch-all exception handler returns `{"detail": "Internal server error. Please try again later."}` with status 500 for any unhandled exceptions, preventing raw stack traces from leaking to clients |
| **Web UI** (`ui.py`) | All `fetch()` calls are wrapped in try/catch for network errors; HTTP status codes and content-type are checked before parsing; error messages include status codes and response excerpts |

### Per-Document Failure Tracking

After document parsing, the API reports **per-document status** in both the task status and results responses:

```json
{
  "document_statuses": [
    {"document_id": "d1", "document_name": "policy.pdf", "status": "success", "progress": 1.0, "message": "Parsed successfully"},
    {"document_id": "d2", "document_name": "corrupt.xlsx", "status": "failed", "progress": 0.0, "message": "Parsing failed"}
  ]
}
```

The UI task detail view renders this as a colour-coded **Document Parsing Status** table showing each document's name, status badge, progress percentage, and failure reason.

### Retry After Failure

When a task fails (e.g. all documents fail to parse, or the pipeline encounters an error), the two-phase workflow supports **retry without data loss**:

1. **Upload more documents** – Call `POST /assessments/sessions/{task_id}/documents` even when the task is in `failed` state. The task automatically resets to `awaiting_documents` and the error is cleared. Previously uploaded documents are preserved.
2. **Re-start the assessment** – Call `POST /assessments/sessions/{task_id}/start`. Previous results are cleared and the pipeline re-runs from parsing onward.

The UI task detail view includes a **"Retry / Upload More Documents"** panel that appears automatically when a session is in `failed` or `awaiting_documents` state, with file upload and start buttons.

```bash
# Example: retry after failure
# Upload replacement documents to the failed task
curl -X POST http://localhost:8000/api/v1/assessments/sessions/$TASK_ID/documents \
  -H "Authorization: Bearer $TOKEN" \
  -F "files=@corrected_document.pdf"

# Re-start the assessment
curl -X POST http://localhost:8000/api/v1/assessments/sessions/$TASK_ID/start \
  -H "Authorization: Bearer $TOKEN"
```

## Postman Collection

A ready-to-use Postman collection is included at `assessment/postman_collection.json`.

### Import

1. Open Postman → **Import** → select `postman_collection.json`
2. The collection includes all endpoints organised by workflow, with example responses and test scripts that auto-save `task_id` / `dataset_id`.

### Variables

After importing, configure the collection-level variables:

| Variable | Description | Example |
|---|---|---|
| `base_url` | Assessment API host + port | `http://localhost:8000` |
| `base_path` | Subpath prefix (if configured) | `/assessment` or empty |
| `jwt_token` | JWT bearer token (if auth enabled) | `eyJhbGci...` |
| `task_id` | Auto-populated after creating an assessment | |
| `dataset_id` | Auto-populated from session creation | |
| `dataset_ids` | Comma-separated dataset IDs for from-dataset flow | `id1,id2` |
| `image_id` | From reference results (for proxy endpoint) | |
| `document_id` | From reference results (for proxy endpoint) | |

The collection uses **Bearer token** auth at the collection level referencing `{{jwt_token}}`. The Health Check endpoint overrides this with **No Auth**.

## Example Usage with curl

> **Note:** If JWT auth is enabled, add `-H "Authorization: Bearer <token>"` to all requests below.
> If using a subpath (e.g. `ASSESSMENT_API_BASE_PATH=/assessment`), prepend it to all URLs
> (e.g. `http://localhost:8000/assessment/api/v1/assessments`).

### Single-call workflow

```bash
# Start an assessment against an existing dataset (fastest – skips upload & parsing)
curl -X POST http://localhost:8000/api/v1/assessments/from-dataset \
  -H "Authorization: Bearer $TOKEN" \
  -F "questions_file=@questions.xlsx" \
  -F "dataset_ids=your-existing-dataset-id"

# Or use multiple datasets (comma-separated)
curl -X POST http://localhost:8000/api/v1/assessments/from-dataset \
  -H "Authorization: Bearer $TOKEN" \
  -F "questions_file=@questions.xlsx" \
  -F "dataset_ids=dataset-id-1,dataset-id-2,dataset-id-3"

# Start an assessment (all docs at once)
curl -X POST http://localhost:8000/api/v1/assessments \
  -H "Authorization: Bearer $TOKEN" \
  -F "questions_file=@questions.xlsx" \
  -F "evidence_files=@policy_doc.pdf" \
  -F "evidence_files=@procedures.docx"

# Start an assessment with custom dataset and chat options
curl -X POST http://localhost:8000/api/v1/assessments \
  -H "Authorization: Bearer $TOKEN" \
  -F "questions_file=@questions.xlsx" \
  -F "evidence_files=@policy_doc.pdf" \
  -F 'dataset_options={"permission":"team"}' \
  -F 'chat_options={"prompt":{"system":"You are a helpful assistant."}}'

# Start with custom question columns (questions in columns C and D instead of A and B)
curl -X POST http://localhost:8000/api/v1/assessments \
  -H "Authorization: Bearer $TOKEN" \
  -F "questions_file=@questions.xlsx" \
  -F "evidence_files=@policy_doc.pdf" \
  -F "question_id_column=C" \
  -F "question_column=D"

# Or use 1-based column numbers instead of letters
curl -X POST http://localhost:8000/api/v1/assessments/from-dataset \
  -H "Authorization: Bearer $TOKEN" \
  -F "questions_file=@questions.xlsx" \
  -F "dataset_ids=your-existing-dataset-id" \
  -F "question_id_column=3" \
  -F "question_column=4"

# Poll status
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8000/api/v1/assessments/{task_id}

# Get results
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8000/api/v1/assessments/{task_id}/results?page=1

# Download Excel
curl -H "Authorization: Bearer $TOKEN" \
  -o results.xlsx http://localhost:8000/api/v1/assessments/{task_id}/results/excel

# Fetch a proxied image from a reference
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8000/api/v1/proxy/image/{image_id} -o chunk_image.png

# Start an assessment with vendor response verification
curl -X POST http://localhost:8000/api/v1/assessments \
  -H "Authorization: Bearer $TOKEN" \
  -F "questions_file=@questions_with_responses.xlsx" \
  -F "evidence_files=@policy_doc.pdf" \
  -F "process_vendor_response=true" \
  -F "vendor_response_column=C" \
  -F "vendor_comment_column=D"

# Include all retrieved references (not just cited ones)
curl -X POST http://localhost:8000/api/v1/assessments \
  -H "Authorization: Bearer $TOKEN" \
  -F "questions_file=@questions.xlsx" \
  -F "evidence_files=@policy_doc.pdf" \
  -F "only_cited_references=false"
```

### Two-phase workflow (incremental uploads)

```bash
# Phase 1: Create session with questions
RESPONSE=$(curl -s -X POST http://localhost:8000/api/v1/assessments/sessions \
  -H "Authorization: Bearer $TOKEN" \
  -F "questions_file=@questions.xlsx")
TASK_ID=$(echo $RESPONSE | jq -r '.task_id')
echo "Task ID: $TASK_ID"

# Phase 2: Upload evidence documents (call as many times as needed)
curl -X POST http://localhost:8000/api/v1/assessments/sessions/$TASK_ID/documents \
  -H "Authorization: Bearer $TOKEN" \
  -F "files=@policy_doc.pdf" \
  -F "files=@procedures.docx"

# Upload more documents later...
curl -X POST http://localhost:8000/api/v1/assessments/sessions/$TASK_ID/documents \
  -H "Authorization: Bearer $TOKEN" \
  -F "files=@audit_report.pdf"

# Upload a single large file on its own
curl -X POST http://localhost:8000/api/v1/assessments/sessions/$TASK_ID/documents \
  -H "Authorization: Bearer $TOKEN" \
  -F "files=@large_evidence.pptx"

# Phase 3: Start the assessment
curl -X POST http://localhost:8000/api/v1/assessments/sessions/$TASK_ID/start \
  -H "Authorization: Bearer $TOKEN"

# Phase 3: Start the assessment with custom chat options
curl -X POST http://localhost:8000/api/v1/assessments/sessions/$TASK_ID/start \
  -H "Authorization: Bearer $TOKEN" \
  -F 'chat_options={"prompt":{"system":"You are a helpful assistant."}}'

# Poll status (same as single-call)
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8000/api/v1/assessments/$TASK_ID

# Get results
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8000/api/v1/assessments/$TASK_ID/results
```

## Questions Excel Format

By default, the API reads questions from:
- **Column A**: Question Serial No
- **Column B**: Question text
- **Column C**: Vendor Response (optional, used if `process_vendor_response` is enabled)
- **Column D**: Vendor Comment (optional, used if `process_vendor_response` is enabled)

The first row is automatically detected as a header and skipped if it contains non-numeric text.

### Default Layout (columns A, B, C & D)

| A (Serial_No) | B (Question) | C (Vendor_Response) | D (Vendor_Comment) |
|---|---|---|---|
| 1 | Does the organization have a data privacy policy? | Yes | Available on company intranet |
| 2 | Is there a documented incident response plan? | No | In progress |
| 3 | Are access controls implemented for sensitive data? | Yes | See section 4.2 of security policy |

### Custom Column Layout

If your Excel file uses different columns, you can specify which columns to read via:

- **Per-request**: Pass `question_id_column`, `question_column`, `vendor_response_column`, and `vendor_comment_column` as form fields in any endpoint that accepts a questions file.
- **Server-wide default**: Set `ASSESSMENT_QUESTION_ID_COLUMN`, `ASSESSMENT_QUESTION_COLUMN`, `ASSESSMENT_VENDOR_RESPONSE_COLUMN`, and `ASSESSMENT_VENDOR_COMMENT_COLUMN` environment variables.

Columns can be specified as **Excel letters** (`A`, `B`, `C`, …) or **1-based numbers** (`1`, `2`, `3`, …).

For example, if your spreadsheet has extra columns before the questions:

| A (Department) | B (Category) | C (ID) | D (Question) |
|---|---|---|---|
| IT | Security | 1 | Does the organization have a data privacy policy? |
| IT | Security | 2 | Is there a documented incident response plan? |

You would set `question_id_column=C` and `question_column=D` (or equivalently `question_id_column=3` and `question_column=4`).

```bash
# Per-request override (curl)
curl -X POST http://localhost:8000/api/v1/assessments \
  -F "questions_file=@questions.xlsx" \
  -F "evidence_files=@evidence.pdf" \
  -F "question_id_column=C" \
  -F "question_column=D"

# Server-wide default (environment variables)
export ASSESSMENT_QUESTION_ID_COLUMN=C
export ASSESSMENT_QUESTION_COLUMN=D
```

> **Note:** Per-request values override server-wide defaults. If neither is specified, the API uses columns A and B.

## Performance & Parallelization

The assessment pipeline is optimized for throughput at every stage:

| Stage | Parallelization |
|---|---|
| **Document uploads** | Uploaded concurrently via `asyncio.gather` with a semaphore (bounded by `MAX_CONCURRENT_QUESTIONS`) instead of sequentially |
| **Question processing** | Concurrent question processing with semaphore-controlled parallelism |
| **Progress persistence** | Batched DB writes — progress is persisted every 5 questions (and always on the final question), reducing DB round-trips by ~80% while still providing timely updates |
| **Resource cleanup** | When stale datasets or chat assistants need deletion (e.g. `ensure_dataset`, `ensure_chat`), deletions run concurrently via `asyncio.gather` |
| **Standalone uploads** | The `POST /ragflow/documents/upload` endpoint also uploads files concurrently |

## Scaling Considerations

- **Concurrent Questions**: Controlled by `MAX_CONCURRENT_QUESTIONS` (semaphore). Increase for faster throughput if your RAGFlow instance can handle the load.
- **Two-phase Uploads**: Use the session-based workflow for large document sets. Upload files one at a time or in small batches to avoid timeout issues and memory pressure.
- **Background Tasks**: FastAPI's `BackgroundTasks` handles async pipeline execution. For production, consider Celery + Redis for distributed task queues.
- **Database Persistence**: Task state is stored in SQLite (default) or PostgreSQL. See [Database Persistence](#database-persistence) for details.
- **Multiple Workers**: Use `uvicorn --workers N` with PostgreSQL for horizontal scaling. See [Horizontal Scaling](#horizontal-scaling).

## Troubleshooting

### RAGFlow API Key and Permissions

If you encounter a `RuntimeError: RAGFlow error: User '...' lacks permission for dataset '...'` even though your API key is correct, it is likely due to a known quirk in the RAGFlow SDK API where searching for a dataset or chat that hasn't been created yet returns a permission error instead of an empty list.

The assessment app's `RagflowClient` has been updated to handle these cases by:
- Catching "lacks permission" errors during dataset lookups and treating them as "not found", allowing the app to proceed with creating the dataset.
- Catching "doesn't exist" errors during chat lookups and treating them as "not found", allowing the app to proceed with creating the chat assistant.

If you continue to see permission errors, please verify:
1. Your `ASSESSMENT_RAGFLOW_API_KEY` is valid and hasn't expired.
2. The user associated with the API key has the necessary quota and permissions in RAGFlow to create datasets and chat assistants.

## Project Structure

```
assessment/
├── __init__.py          # Package marker
├── auth.py              # LDAP/JWT auth endpoints + role-based authorization
├── config.py            # Settings via environment variables
├── db.py                # Async database layer (SQLite / PostgreSQL)
├── Dockerfile           # Container image definition
├── .dockerignore        # Docker build exclusions
├── main.py              # FastAPI app entry point (includes global error handler)
├── models.py            # Pydantic schemas (requests, responses, internal)
├── ragflow_client.py    # Async HTTP client for RAGFlow API (with error handling)
├── routers.py           # API endpoint definitions (with error handling)
├── services.py          # Task store, Excel I/O, assessment pipeline (with retry support)
├── ui.py                # Built-in web UI dashboard (single-page HTML app)
├── postman_collection.json  # Postman collection for all API endpoints
├── requirements.txt     # Python dependencies
├── README.md            # This file
└── tests/
    ├── test_ragflow_client.py  # Unit tests for RAGFlow client
    └── test_services.py        # Unit tests for services layer
```
