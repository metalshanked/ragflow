"""
Observability bootstrap for Assessment API.

Provides:
- Structured logging + rotating file logs
- OpenTelemetry traces/log export
- Optional OpenInference semantic attributes on spans
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager, nullcontext
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Iterator

from .config import settings

try:
    from opentelemetry import _logs, trace
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, ConsoleLogExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
    from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased

    _OTEL_AVAILABLE = True
except Exception:  # pragma: no cover - optional runtime dependency
    _OTEL_AVAILABLE = False

    class _NoopTrace:
        @staticmethod
        def get_current_span():
            return None

        @staticmethod
        def get_tracer(_name: str):
            class _NoopTracer:
                @contextmanager
                def start_as_current_span(self, _span_name: str):
                    class _NoopSpan:
                        def set_attribute(self, _k: str, _v: Any) -> None:
                            pass

                    yield _NoopSpan()

            return _NoopTracer()

    trace = _NoopTrace()  # type: ignore[assignment]

try:
    from openinference.instrumentation import using_attributes as _oi_using_attributes

    _OPENINFERENCE_AVAILABLE = True
except Exception:  # pragma: no cover - optional runtime dependency
    _OPENINFERENCE_AVAILABLE = False
    _oi_using_attributes = None


logger = logging.getLogger(__name__)

_LOGGING_INITIALIZED = False
_TELEMETRY_INITIALIZED = False


def _safe_json_load(raw: str, *, expect: type[dict] | type[list], fallback: Any) -> Any:
    raw = (raw or "").strip()
    if not raw:
        return fallback
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return fallback
    if isinstance(parsed, expect):
        return parsed
    return fallback


def parse_key_value_map(raw: str) -> dict[str, str]:
    """Parse JSON object or comma-separated key=value list into a dict."""
    raw = (raw or "").strip()
    if not raw:
        return {}

    parsed_json = _safe_json_load(raw, expect=dict, fallback=None)
    if parsed_json is not None:
        return {str(k): str(v) for k, v in parsed_json.items()}

    out: dict[str, str] = {}
    for part in raw.split(","):
        token = part.strip()
        if not token or "=" not in token:
            continue
        key, value = token.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key:
            out[key] = value
    return out


def build_otlp_signal_endpoint(base_endpoint: str, signal: str) -> str:
    """Build signal-specific endpoint (`traces` or `logs`) from a base endpoint."""
    base = (base_endpoint or "").strip()
    if not base:
        return ""
    base = base.rstrip("/")
    if base.endswith("/v1/traces") or base.endswith("/v1/logs"):
        return base
    return f"{base}/v1/{signal}"


def _is_otlp_enabled() -> bool:
    return bool(
        settings.otel_exporter_otlp_endpoint.strip()
        or settings.otel_exporter_otlp_traces_endpoint.strip()
        or settings.otel_exporter_otlp_logs_endpoint.strip()
    )


def _trace_context_ids() -> tuple[str | None, str | None]:
    if not _OTEL_AVAILABLE:
        return None, None
    span = trace.get_current_span()
    if span is None:
        return None, None
    ctx = span.get_span_context()
    if not getattr(ctx, "is_valid", False):
        return None, None
    return f"{ctx.trace_id:032x}", f"{ctx.span_id:016x}"


class JsonFormatter(logging.Formatter):
    """Compact JSON log formatter with OTel trace/span correlation."""

    def format(self, record: logging.LogRecord) -> str:
        trace_id, span_id = _trace_context_ids()
        payload: dict[str, Any] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
        }
        if trace_id:
            payload["trace_id"] = trace_id
        if span_id:
            payload["span_id"] = span_id
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=True)


def configure_logging() -> None:
    """Configure app logging with console + rotating file handlers."""
    global _LOGGING_INITIALIZED
    if _LOGGING_INITIALIZED:
        return

    root = logging.getLogger()
    root.setLevel(getattr(logging, settings.log_level.upper(), logging.INFO))
    root.handlers.clear()

    formatter: logging.Formatter
    if settings.log_json:
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)-8s %(name)s %(message)s"
        )

    if settings.log_to_console:
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        root.addHandler(console)

    if settings.log_file_enabled:
        log_dir = Path(settings.log_dir).resolve()
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            filename=str(log_dir / settings.log_file_name),
            maxBytes=max(1, settings.log_max_bytes),
            backupCount=max(1, settings.log_backup_count),
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    # Let uvicorn and app loggers flow through the same handlers.
    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"):
        uv_logger = logging.getLogger(logger_name)
        uv_logger.handlers.clear()
        uv_logger.propagate = True

    _LOGGING_INITIALIZED = True
    logger.info("Logging initialized (level=%s)", settings.log_level.upper())


def _resource() -> Any:
    attrs: dict[str, Any] = {
        "service.name": settings.otel_service_name,
        "service.version": settings.otel_service_version,
    }
    attrs.update(_safe_json_load(settings.otel_resource_attributes_json, expect=dict, fallback={}))
    return Resource.create(attrs)


def _init_trace_provider() -> None:
    sampler = ParentBased(TraceIdRatioBased(max(0.0, min(1.0, settings.otel_sample_ratio))))
    provider = TracerProvider(resource=_resource(), sampler=sampler)

    if settings.otel_export_traces:
        traces_endpoint = (
            settings.otel_exporter_otlp_traces_endpoint.strip()
            or build_otlp_signal_endpoint(settings.otel_exporter_otlp_endpoint, "traces")
        )
        if traces_endpoint:
            exporter = OTLPSpanExporter(
                endpoint=traces_endpoint,
                headers=parse_key_value_map(settings.otel_exporter_otlp_headers),
            )
            provider.add_span_processor(BatchSpanProcessor(exporter))
        if settings.otel_enable_console_exporter:
            provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

    trace.set_tracer_provider(provider)


def _init_log_provider() -> None:
    if not settings.otel_export_logs:
        return

    logs_endpoint = (
        settings.otel_exporter_otlp_logs_endpoint.strip()
        or build_otlp_signal_endpoint(settings.otel_exporter_otlp_endpoint, "logs")
    )
    if not logs_endpoint and not settings.otel_enable_console_exporter:
        return

    provider = LoggerProvider(resource=_resource())
    if logs_endpoint:
        exporter = OTLPLogExporter(
            endpoint=logs_endpoint,
            headers=parse_key_value_map(settings.otel_exporter_otlp_headers),
        )
        provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
    if settings.otel_enable_console_exporter:
        provider.add_log_record_processor(BatchLogRecordProcessor(ConsoleLogExporter()))

    _logs.set_logger_provider(provider)
    logging.getLogger().addHandler(
        LoggingHandler(level=logging.NOTSET, logger_provider=provider)
    )


def init_telemetry(app: Any) -> None:
    """Initialize OpenTelemetry instrumentation and exporters."""
    global _TELEMETRY_INITIALIZED
    if _TELEMETRY_INITIALIZED:
        return
    if not settings.otel_enabled:
        return
    if not _OTEL_AVAILABLE:
        logger.warning(
            "OpenTelemetry is enabled but dependencies are unavailable. "
            "Install opentelemetry packages to enable tracing/log export."
        )
        return
    if not _is_otlp_enabled() and not settings.otel_enable_console_exporter:
        logger.warning(
            "OpenTelemetry is enabled but no exporter endpoint is configured. "
            "Set ASSESSMENT_OTEL_EXPORTER_OTLP_ENDPOINT (or signal-specific endpoints)."
        )
        return

    _init_trace_provider()
    _init_log_provider()

    if settings.otel_instrument_fastapi:
        FastAPIInstrumentor.instrument_app(app)
    if settings.otel_instrument_httpx:
        HTTPXClientInstrumentor().instrument()
    if settings.otel_instrument_sqlalchemy:
        try:
            from .db import _engine

            SQLAlchemyInstrumentor().instrument(engine=_engine.sync_engine)
        except Exception:
            logger.exception("Failed to instrument SQLAlchemy engine")

    _TELEMETRY_INITIALIZED = True
    logger.info("OpenTelemetry initialized")


def shutdown_telemetry() -> None:
    """Flush and shutdown telemetry providers."""
    if not _OTEL_AVAILABLE:
        return
    try:
        provider = trace.get_tracer_provider()
        if hasattr(provider, "shutdown"):
            provider.shutdown()  # type: ignore[call-arg]
    except Exception:
        logger.exception("Failed to shutdown tracer provider")
    try:
        lp = _logs.get_logger_provider()
        if hasattr(lp, "shutdown"):
            lp.shutdown()  # type: ignore[call-arg]
    except Exception:
        logger.exception("Failed to shutdown log provider")


def set_span_attributes(span: Any, attributes: dict[str, Any]) -> None:
    for key, value in attributes.items():
        if value is None:
            continue
        try:
            span.set_attribute(key, value)
        except Exception:
            # Ignore non-serialisable attribute values.
            continue


@contextmanager
def start_span(
    name: str,
    *,
    span_kind: str | None = None,
    attributes: dict[str, Any] | None = None,
) -> Iterator[Any]:
    """Start a span and optionally add OpenInference semantic attributes."""
    tracer = trace.get_tracer("assessment")
    with tracer.start_as_current_span(name) as span:
        if span_kind and settings.openinference_enabled:
            try:
                span.set_attribute("openinference.span.kind", span_kind)
            except Exception:
                pass
        if attributes:
            set_span_attributes(span, attributes)
        yield span


@contextmanager
def openinference_attributes(**attrs: Any) -> Iterator[None]:
    """Optional OpenInference context propagation helper."""
    if not settings.openinference_enabled or not _OPENINFERENCE_AVAILABLE:
        yield
        return
    # Some OpenInference versions can reject unknown keys at context creation
    # time. Fall back to a no-op context instead of failing app logic.
    try:
        context = _oi_using_attributes(**attrs)  # type: ignore[misc]
    except Exception:
        context = nullcontext()
    with context:
        yield
