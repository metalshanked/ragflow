"""
Self-contained load test client for the Assessment API single-call workflow.

Update the configuration block below, then run:

    python assessment/load_test_single_call.py

The script:
1. Reuses one questions file and one evidence directory for every run.
2. Starts N single-call assessments in parallel via POST /api/v1/assessments.
3. Polls GET /api/v1/assessments/{task_id} until each task completes or fails.
4. Fetches paginated results from GET /api/v1/assessments/{task_id}/results.
5. Prints per-run outcomes and aggregate benchmark statistics.
"""

from __future__ import annotations

import concurrent.futures
import json
import math
import mimetypes
import statistics
import threading
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "http://localhost:8000"
BASE_PATH = ""
JWT_TOKEN = ""

# Number of single-call assessments to run in parallel.
PARALLEL_RUNS = 5

QUESTIONS_FILE_PATH = r"E:\Projects\ragflow\assessment\sample_questions.xlsx"
EVIDENCE_DIR_PATH = r"E:\Projects\ragflow\assessment\evidence"

# SSL verification behavior:
# - Set SSL_CA_CERT_PATH to a PEM file path to verify with that cert/bundle.
# - Otherwise VERIFY_SSL=False disables verification.
# - Otherwise VERIFY_SSL=True uses system trust store.
VERIFY_SSL = True
SSL_CA_CERT_PATH = ""

REQUEST_TIMEOUT_SECONDS = 300.0
POLL_INTERVAL_SECONDS = 5.0
TASK_TIMEOUT_SECONDS = 3600.0
RESULTS_PAGE_SIZE = 500
FETCH_RESULTS_AT_END = True
# Deletes chat/dataset artifacts after each run reaches completed/failed.
# This uses /api/v1/native/* endpoints, so the caller may need admin rights.
# Timed-out runs are not cleaned up automatically because the server task may still be active.
CLEANUP_ARTIFACTS = False

# Leave blank to let the server generate unique names automatically.
DATASET_NAME_PREFIX = ""
CHAT_NAME_PREFIX = ""

# Optional request fields.
REUSE_EXISTING_DATASET = True
PROCESS_VENDOR_RESPONSE = False
ONLY_CITED_REFERENCES = True
QUESTION_ID_COLUMN = ""
QUESTION_COLUMN = ""
VENDOR_RESPONSE_COLUMN = ""
VENDOR_COMMENT_COLUMN = ""
DATASET_OPTIONS: dict[str, Any] = {}
CHAT_OPTIONS: dict[str, Any] = {}


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

TERMINAL_STATES = {"completed", "failed"}


@dataclass
class PreparedFile:
    name: str
    content: bytes
    content_type: str


@dataclass
class RunMetrics:
    run_number: int
    submit_started_at: float = 0.0
    submit_completed_at: float = 0.0
    terminal_at: float = 0.0
    results_completed_at: float = 0.0
    task_id: str = ""
    accepted: bool = False
    start_http_status: int = 0
    final_state: str = "not_started"
    pipeline_stage: str = ""
    progress_message: str = ""
    questions_processed: int = 0
    total_questions: int = 0
    dataset_id: str = ""
    dataset_ids: list[str] = field(default_factory=list)
    chat_id: str = ""
    session_id: str = ""
    document_ids: list[str] = field(default_factory=list)
    poll_count: int = 0
    result_pages: int = 0
    result_rows: int = 0
    verdict_counts: Counter[str] = field(default_factory=Counter)
    document_status_counts: Counter[str] = field(default_factory=Counter)
    cleanup_attempted: bool = False
    cleanup_success: bool = False
    cleanup_message: str = ""
    error: str = ""

    @property
    def submit_latency(self) -> float | None:
        if self.submit_started_at and self.submit_completed_at:
            return self.submit_completed_at - self.submit_started_at
        return None

    @property
    def terminal_latency(self) -> float | None:
        if self.submit_started_at and self.terminal_at:
            return self.terminal_at - self.submit_started_at
        return None

    @property
    def results_latency(self) -> float | None:
        if self.terminal_at and self.results_completed_at:
            return self.results_completed_at - self.terminal_at
        return None

    @property
    def end_to_end_latency(self) -> float | None:
        end_time = self.results_completed_at or self.terminal_at
        if self.submit_started_at and end_time:
            return end_time - self.submit_started_at
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

print_lock = threading.Lock()


def log(message: str) -> None:
    with print_lock:
        print(message, flush=True)


def build_verify_setting() -> bool | str:
    if SSL_CA_CERT_PATH.strip():
        cert_path = Path(SSL_CA_CERT_PATH).expanduser()
        if not cert_path.is_file():
            raise FileNotFoundError(f"SSL_CA_CERT_PATH does not exist: {cert_path}")
        return str(cert_path)
    return VERIFY_SSL


def normalize_base_path(base_path: str) -> str:
    value = (base_path or "").strip()
    if not value:
        return ""
    if not value.startswith("/"):
        value = f"/{value}"
    return value.rstrip("/")


def build_url(path: str) -> str:
    return f"{BASE_URL.rstrip('/')}{normalize_base_path(BASE_PATH)}{path}"


def maybe_json(response: httpx.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except Exception:
        text = response.text.strip()
        raise RuntimeError(f"HTTP {response.status_code}: {text[:500] or 'non-JSON response'}")
    if not isinstance(payload, dict):
        raise RuntimeError(f"HTTP {response.status_code}: unexpected JSON payload")
    return payload


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    rank = (len(values) - 1) * pct
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return values[lower]
    fraction = rank - lower
    return values[lower] + (values[upper] - values[lower]) * fraction


def summarize_numeric(values: list[float]) -> str:
    if not values:
        return "n/a"
    ordered = sorted(values)
    return (
        f"count={len(ordered)} "
        f"min={ordered[0]:.2f}s "
        f"mean={statistics.mean(ordered):.2f}s "
        f"p50={percentile(ordered, 0.50):.2f}s "
        f"p95={percentile(ordered, 0.95):.2f}s "
        f"max={ordered[-1]:.2f}s"
    )


def summarize_counts(values: list[int]) -> str:
    if not values:
        return "n/a"
    ordered = sorted(values)
    return (
        f"count={len(ordered)} "
        f"min={ordered[0]} "
        f"mean={statistics.mean(ordered):.2f} "
        f"p50={percentile([float(v) for v in ordered], 0.50):.2f} "
        f"p95={percentile([float(v) for v in ordered], 0.95):.2f} "
        f"max={ordered[-1]}"
    )


def prepare_questions_file(path_str: str) -> PreparedFile:
    path = Path(path_str).expanduser()
    if not path.is_file():
        raise FileNotFoundError(f"Questions file not found: {path}")
    content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    return PreparedFile(name=path.name, content=path.read_bytes(), content_type=content_type)


def prepare_evidence_files(path_str: str) -> list[PreparedFile]:
    directory = Path(path_str).expanduser()
    if not directory.is_dir():
        raise FileNotFoundError(f"Evidence directory not found: {directory}")

    files: list[PreparedFile] = []
    for file_path in sorted(p for p in directory.iterdir() if p.is_file()):
        content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
        files.append(
            PreparedFile(
                name=file_path.name,
                content=file_path.read_bytes(),
                content_type=content_type,
            )
        )

    if not files:
        raise RuntimeError(f"No evidence files found in: {directory}")
    return files


def build_start_form(run_number: int) -> dict[str, str]:
    form: dict[str, str] = {
        "reuse_exisiting_dataset": str(REUSE_EXISTING_DATASET).lower(),
        "process_vendor_response": str(PROCESS_VENDOR_RESPONSE).lower(),
        "only_cited_references": str(ONLY_CITED_REFERENCES).lower(),
    }
    if DATASET_NAME_PREFIX.strip():
        form["dataset_name"] = f"{DATASET_NAME_PREFIX}-{run_number:03d}-{uuid.uuid4().hex[:8]}"
    if CHAT_NAME_PREFIX.strip():
        form["chat_name"] = f"{CHAT_NAME_PREFIX}-{run_number:03d}-{uuid.uuid4().hex[:8]}"
    if QUESTION_ID_COLUMN.strip():
        form["question_id_column"] = QUESTION_ID_COLUMN.strip()
    if QUESTION_COLUMN.strip():
        form["question_column"] = QUESTION_COLUMN.strip()
    if VENDOR_RESPONSE_COLUMN.strip():
        form["vendor_response_column"] = VENDOR_RESPONSE_COLUMN.strip()
    if VENDOR_COMMENT_COLUMN.strip():
        form["vendor_comment_column"] = VENDOR_COMMENT_COLUMN.strip()
    if DATASET_OPTIONS:
        form["dataset_options"] = json.dumps(DATASET_OPTIONS)
    if CHAT_OPTIONS:
        form["chat_options"] = json.dumps(CHAT_OPTIONS)
    return form


def build_start_files(
    questions_file: PreparedFile,
    evidence_files: list[PreparedFile],
) -> list[tuple[str, tuple[str, bytes, str]]]:
    files: list[tuple[str, tuple[str, bytes, str]]] = [
        (
            "questions_file",
            (questions_file.name, questions_file.content, questions_file.content_type),
        )
    ]
    files.extend(
        (
            "evidence_files",
            (evidence_file.name, evidence_file.content, evidence_file.content_type),
        )
        for evidence_file in evidence_files
    )
    return files


def create_client(verify_setting: bool | str) -> httpx.Client:
    headers = {}
    if JWT_TOKEN.strip():
        headers["Authorization"] = f"Bearer {JWT_TOKEN.strip()}"
    return httpx.Client(
        headers=headers,
        timeout=httpx.Timeout(REQUEST_TIMEOUT_SECONDS, connect=15.0),
        verify=verify_setting,
    )


def update_artifact_ids(metrics: RunMetrics, payload: dict[str, Any]) -> None:
    metrics.dataset_id = str(payload.get("dataset_id") or "").strip()
    metrics.dataset_ids = [
        str(value).strip()
        for value in (payload.get("dataset_ids") or [])
        if str(value).strip()
    ]
    metrics.chat_id = str(payload.get("chat_id") or "").strip()
    metrics.session_id = str(payload.get("session_id") or "").strip()
    metrics.document_ids = [
        str(value).strip()
        for value in (payload.get("document_ids") or [])
        if str(value).strip()
    ]


def _best_effort_json_detail(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except Exception:
        return response.text.strip()[:300]
    if isinstance(payload, dict):
        detail = payload.get("detail") or payload.get("message")
        if detail:
            return str(detail)[:300]
    return str(payload)[:300]


def cleanup_artifacts(client: httpx.Client, metrics: RunMetrics) -> None:
    dataset_ids = [value for value in metrics.dataset_ids if value]
    if metrics.dataset_id:
        dataset_ids.append(metrics.dataset_id)
    dataset_ids = list(dict.fromkeys(dataset_ids))

    actions: list[str] = []
    errors: list[str] = []
    metrics.cleanup_attempted = bool(metrics.chat_id or dataset_ids)

    if not metrics.cleanup_attempted:
        metrics.cleanup_success = True
        metrics.cleanup_message = "No chat or dataset IDs available for cleanup"
        return

    if metrics.chat_id:
        try:
            response = client.request(
                "DELETE",
                build_url("/api/v1/native/chats"),
                json={"ids": [metrics.chat_id]},
            )
            if response.status_code >= 400:
                errors.append(
                    f"chat {metrics.chat_id}: HTTP {response.status_code} {_best_effort_json_detail(response)}"
                )
            else:
                actions.append(f"chat={metrics.chat_id}")
        except Exception as exc:
            errors.append(f"chat {metrics.chat_id}: {exc}")

    if dataset_ids:
        try:
            response = client.request(
                "DELETE",
                build_url("/api/v1/native/datasets"),
                json={"ids": dataset_ids},
            )
            if response.status_code >= 400:
                errors.append(
                    f"dataset(s) {','.join(dataset_ids)}: HTTP {response.status_code} {_best_effort_json_detail(response)}"
                )
            else:
                actions.append(f"dataset_count={len(dataset_ids)}")
        except Exception as exc:
            errors.append(f"dataset(s) {','.join(dataset_ids)}: {exc}")

    metrics.cleanup_success = not errors
    if errors:
        metrics.cleanup_message = "; ".join(errors)
    else:
        metrics.cleanup_message = "Deleted " + ", ".join(actions)


def check_health(verify_setting: bool | str) -> None:
    url = build_url("/health")
    with create_client(verify_setting) as client:
        response = client.get(url)
    if response.status_code >= 400:
        raise RuntimeError(f"Health check failed: HTTP {response.status_code} {response.text[:500]}")
    payload = maybe_json(response)
    log(
        "Health: "
        f"status={payload.get('status', '')} "
        f"auth_enabled={payload.get('auth_enabled', '')} "
        f"base_path={payload.get('base_path', '')} "
        f"ragflow_url={payload.get('ragflow_url', '')}"
    )


def poll_task_status(client: httpx.Client, task_id: str, metrics: RunMetrics) -> None:
    deadline = time.perf_counter() + TASK_TIMEOUT_SECONDS
    status_url = build_url(f"/api/v1/assessments/{task_id}")

    while True:
        if time.perf_counter() > deadline:
            metrics.final_state = "timeout"
            metrics.error = f"Timed out after {TASK_TIMEOUT_SECONDS:.1f}s while polling task status"
            metrics.terminal_at = time.perf_counter()
            return

        response = client.get(status_url)
        metrics.poll_count += 1
        if response.status_code >= 400:
            raise RuntimeError(f"Status poll failed for task {task_id}: HTTP {response.status_code} {response.text[:300]}")

        payload = maybe_json(response)
        update_artifact_ids(metrics, payload)
        metrics.final_state = str(payload.get("state") or "")
        metrics.pipeline_stage = str(payload.get("pipeline_stage") or "")
        metrics.progress_message = str(payload.get("progress_message") or "")
        metrics.questions_processed = int(payload.get("questions_processed") or 0)
        metrics.total_questions = int(payload.get("total_questions") or 0)
        metrics.error = str(payload.get("error") or "").strip()

        metrics.document_status_counts = Counter(
            str((doc or {}).get("status") or "unknown").strip() or "unknown"
            for doc in (payload.get("document_statuses") or [])
        )

        if metrics.final_state in TERMINAL_STATES:
            metrics.terminal_at = time.perf_counter()
            return

        time.sleep(POLL_INTERVAL_SECONDS)


def fetch_all_results(client: httpx.Client, task_id: str, metrics: RunMetrics) -> None:
    if not FETCH_RESULTS_AT_END:
        return

    page = 1
    total_pages = 1
    results_url = build_url(f"/api/v1/assessments/{task_id}/results")

    while page <= total_pages:
        response = client.get(results_url, params={"page": page, "page_size": RESULTS_PAGE_SIZE})
        if response.status_code >= 400:
            raise RuntimeError(
                f"Results fetch failed for task {task_id} page {page}: "
                f"HTTP {response.status_code} {response.text[:300]}"
            )

        payload = maybe_json(response)
        total_pages = max(1, int(payload.get("total_pages") or 1))
        metrics.result_pages += 1

        results = payload.get("results") or []
        metrics.result_rows += len(results)
        for row in results:
            verdict = str((row or {}).get("ai_response") or "").strip() or "blank"
            metrics.verdict_counts[verdict] += 1

        page += 1

    metrics.results_completed_at = time.perf_counter()


def run_single_assessment(
    run_number: int,
    questions_file: PreparedFile,
    evidence_files: list[PreparedFile],
    verify_setting: bool | str,
) -> RunMetrics:
    metrics = RunMetrics(run_number=run_number, submit_started_at=time.perf_counter())

    try:
        with create_client(verify_setting) as client:
            response = client.post(
                build_url("/api/v1/assessments"),
                data=build_start_form(run_number),
                files=build_start_files(questions_file, evidence_files),
            )
            metrics.submit_completed_at = time.perf_counter()
            metrics.start_http_status = response.status_code

            if response.status_code != 202:
                body_preview = response.text.strip()[:500]
                metrics.error = f"Start request failed: HTTP {response.status_code} {body_preview}"
                metrics.final_state = "start_failed"
                metrics.terminal_at = metrics.submit_completed_at
                return metrics

            payload = maybe_json(response)
            metrics.accepted = True
            metrics.task_id = str(payload.get("task_id") or "").strip()
            update_artifact_ids(metrics, payload)
            metrics.final_state = str(payload.get("state") or "pending")
            metrics.pipeline_stage = str(payload.get("pipeline_stage") or "")
            metrics.questions_processed = int(payload.get("questions_processed") or 0)
            metrics.total_questions = int(payload.get("total_questions") or 0)
            if not metrics.task_id:
                raise RuntimeError("Start request succeeded but no task_id was returned")

            log(
                f"[run {run_number:03d}] accepted task_id={metrics.task_id} "
                f"submit={metrics.submit_latency:.2f}s"
            )

            poll_task_status(client, metrics.task_id, metrics)

            try:
                if metrics.final_state != "timeout":
                    fetch_all_results(client, metrics.task_id, metrics)
            finally:
                if CLEANUP_ARTIFACTS and metrics.final_state in TERMINAL_STATES:
                    cleanup_artifacts(client, metrics)

            if metrics.final_state == "timeout":
                log(f"[run {run_number:03d}] timeout task_id={metrics.task_id} polls={metrics.poll_count}")
                return metrics

            log(
                f"[run {run_number:03d}] state={metrics.final_state} "
                f"task_id={metrics.task_id} polls={metrics.poll_count} "
                f"terminal={metrics.terminal_latency:.2f}s "
                f"results={metrics.result_rows} "
                f"cleanup={(metrics.cleanup_message or 'disabled') if CLEANUP_ARTIFACTS else 'disabled'}"
            )
            return metrics

    except Exception as exc:
        now = time.perf_counter()
        if not metrics.submit_completed_at:
            metrics.submit_completed_at = now
        if not metrics.terminal_at:
            metrics.terminal_at = now
        metrics.final_state = metrics.final_state if metrics.final_state != "not_started" else "client_error"
        metrics.error = str(exc)
        log(f"[run {run_number:03d}] error={metrics.error}")
        return metrics


def print_run_details(results: list[RunMetrics]) -> None:
    log("")
    log("Per-run summary")
    for item in sorted(results, key=lambda r: r.run_number):
        log(
            f"run={item.run_number:03d} "
            f"task_id={item.task_id or '-'} "
            f"state={item.final_state} "
            f"submit={(item.submit_latency or 0):.2f}s "
            f"terminal={(item.terminal_latency or 0):.2f}s "
            f"end_to_end={(item.end_to_end_latency or 0):.2f}s "
            f"polls={item.poll_count} "
            f"questions={item.questions_processed}/{item.total_questions} "
            f"results={item.result_rows} "
            f"cleanup={(item.cleanup_message or '-') if CLEANUP_ARTIFACTS else '-'} "
            f"error={item.error or '-'}"
        )


def print_aggregate_summary(results: list[RunMetrics], wall_seconds: float) -> None:
    state_counts = Counter(item.final_state for item in results)
    verdict_counts = Counter()
    document_status_counts = Counter()
    errors = Counter(item.error for item in results if item.error)
    cleanup_status = Counter()
    cleanup_errors = Counter()

    for item in results:
        verdict_counts.update(item.verdict_counts)
        document_status_counts.update(item.document_status_counts)
        if CLEANUP_ARTIFACTS:
            key = "success" if item.cleanup_success else ("not_attempted" if not item.cleanup_attempted else "failed")
            cleanup_status[key] += 1
            if item.cleanup_attempted and not item.cleanup_success and item.cleanup_message:
                cleanup_errors[item.cleanup_message] += 1

    submit_latencies = [value for item in results if (value := item.submit_latency) is not None]
    terminal_latencies = [value for item in results if (value := item.terminal_latency) is not None]
    results_latencies = [value for item in results if (value := item.results_latency) is not None]
    end_to_end_latencies = [value for item in results if (value := item.end_to_end_latency) is not None]
    poll_counts = [item.poll_count for item in results]

    accepted_count = sum(1 for item in results if item.accepted)
    completed_count = state_counts.get("completed", 0)
    failed_count = state_counts.get("failed", 0)
    timeout_count = state_counts.get("timeout", 0)
    total_questions = sum(item.total_questions for item in results)
    processed_questions = sum(item.questions_processed for item in results)
    total_result_rows = sum(item.result_rows for item in results)

    log("")
    log("=" * 60)
    log("🏁 LOAD TEST PERFORMANCE REPORT (Layman's Summary)")
    log("=" * 60)

    log("\n📊 1. OVERALL EXECUTION")
    log("  (Helper: 'Average Throughput' shows how many assessments completed per second on average)")
    log(f"  • Total Assessments Run  : {len(results)}")
    log(f"  • Successfully Completed : {completed_count} ({(completed_count/len(results)*100 if results else 0):.1f}%)")
    log(f"  • Failed / Errors        : {failed_count}")
    log(f"  • Timed Out              : {timeout_count}")
    log(f"  • Total Load Test Time   : {wall_seconds:.1f} seconds")
    
    throughput = len(results) / wall_seconds if wall_seconds > 0 else 0
    if 0 < throughput < 0.01:
        log(f"  • Average Throughput     : {throughput:.4f} runs per second (approx {(1/throughput):.1f} seconds per run)")
    else:
        log(f"  • Average Throughput     : {throughput:.2f} runs per second")

    log("\n⏱️ 2. TIME TAKEN (PER ASSESSMENT)")
    log("  (Helper: Measures the start-to-finish time for an individual assessment run)")
    log("  (Helper: '95% of runs...' means 95 out of 100 runs were faster than this time)")
    if end_to_end_latencies:
        import statistics
        mean_time = statistics.mean(end_to_end_latencies)
        p95_time = percentile(end_to_end_latencies, 0.95)
        log(f"  • Average Time           : {mean_time:.1f} seconds")
        log(f"  • Fastest Run            : {min(end_to_end_latencies):.1f} seconds")
        log(f"  • Slowest Run            : {max(end_to_end_latencies):.1f} seconds")
        log(f"  • 95% of runs finished in: {p95_time:.1f} seconds or less")
    else:
        log("  • No assessments completed successfully to measure time.")

    log("\n📝 3. QUESTION PROCESSING")
    log("  (Helper: Not processed questions usually mean the supporting documents failed to parse or timed out)")
    log(f"  • Total Questions Given  : {total_questions}")
    log(f"  • Questions Processed    : {processed_questions}")
    if total_questions > 0:
        log(f"  • Processing Success Rate: {(processed_questions / total_questions * 100.0):.1f}%")

    log("\n📄 4. DOCUMENT PROCESSING STATUS")
    log("  (Helper: Shows if documents were parsed successfully. 'timeout' = parsing took too long)")
    if document_status_counts:
        for status, count in document_status_counts.items():
            log(f"  • {str(status).capitalize()}: {count}")
    else:
        log("  • No documents were processed.")

    if verdict_counts:
        log("\n✅ 5. VERDICT BREAKDOWN")
        log("  (Helper: The final Yes/No/N/A answers produced by the AI across all processed questions)")
        for verdict, count in verdict_counts.items():
            log(f"  • {str(verdict).capitalize()}: {count}")

    if errors or cleanup_errors:
        log("\n⚠️ 6. ERRORS & WARNINGS")
        for err, count in errors.items():
            log(f"  • Assessment Error ({count} times): {err}")
        for err, count in cleanup_errors.items():
            log(f"  • Cleanup Error ({count} times): {err}")

    log("\n" + "=" * 60)
    log("⚙️ TECHNICAL METRICS (For debugging)")
    log("-" * 60)
    log(f"state_breakdown={dict(state_counts)}")
    log(f"submit_latency={summarize_numeric(submit_latencies)}")
    log(f"time_to_terminal={summarize_numeric(terminal_latencies)}")
    log(f"results_fetch_latency={summarize_numeric(results_latencies)}")
    log(f"end_to_end_latency={summarize_numeric(end_to_end_latencies)}")
    log(f"poll_count={summarize_counts(poll_counts)}")
    log(f"document_status_breakdown={dict(document_status_counts)}")
    if CLEANUP_ARTIFACTS:
        log(f"cleanup_status={dict(cleanup_status)}")
    log("=" * 60)


def main() -> None:
    verify_setting = build_verify_setting()
    questions_file = prepare_questions_file(QUESTIONS_FILE_PATH)
    evidence_files = prepare_evidence_files(EVIDENCE_DIR_PATH)

    log("Starting assessment single-call load test")
    log(f"base_url={BASE_URL}")
    log(f"base_path={normalize_base_path(BASE_PATH) or '/'}")
    log(f"parallel_runs={PARALLEL_RUNS}")
    log(f"questions_file={Path(QUESTIONS_FILE_PATH).expanduser()}")
    log(f"evidence_dir={Path(EVIDENCE_DIR_PATH).expanduser()} file_count={len(evidence_files)}")
    log(f"ssl_verify={verify_setting}")
    log(f"results_fetch={FETCH_RESULTS_AT_END} page_size={RESULTS_PAGE_SIZE}")
    log(f"cleanup_artifacts={CLEANUP_ARTIFACTS}")

    check_health(verify_setting)

    started_at = time.perf_counter()
    results: list[RunMetrics] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=PARALLEL_RUNS) as executor:
        futures = [
            executor.submit(
                run_single_assessment,
                run_number,
                questions_file,
                evidence_files,
                verify_setting,
            )
            for run_number in range(1, PARALLEL_RUNS + 1)
        ]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

    wall_seconds = time.perf_counter() - started_at
    print_run_details(results)
    print_aggregate_summary(results, wall_seconds)


if __name__ == "__main__":
    main()
