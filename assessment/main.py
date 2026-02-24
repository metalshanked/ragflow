"""
Assessment API  –  FastAPI application entry point.

Run with:
    uvicorn assessment.main:app --host 0.0.0.0 --port 8000 --reload
"""

from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .config import settings
from .auth import auth_router
from .observability import configure_logging, init_telemetry, shutdown_telemetry
from .routers import router
from .ui import router as ui_router

configure_logging()

# Normalise base path: strip trailing slash, ensure leading slash if set
_raw_base = settings.api_base_path.strip().rstrip("/")
if _raw_base and not _raw_base.startswith("/"):
    _raw_base = "/" + _raw_base

app = FastAPI(
    title="RAGFlow Assessment API",
    description=(
        "Wrapper API that takes a list of assessment questions (Excel) and "
        "evidence documents, verifies each question against the evidence "
        "using RAGFlow's chat API, and returns structured Yes/No results "
        "with references. Supports async task processing, pagination, and "
        "Excel download."
    ),
    version="1.0.0",
    root_path=_raw_base,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
app.include_router(auth_router)
app.include_router(ui_router)
init_telemetry(app)


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):
    """Catch-all handler so unhandled errors return structured JSON."""
    logging.getLogger(__name__).exception(
        "Unhandled error on %s %s", request.method, request.url.path
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error. Please try again later."},
    )


@app.on_event("startup")
async def _startup() -> None:
    import asyncio
    from .db import init_db, db_purge_old_tasks

    await init_db()

    # Start periodic cleanup of old task rows (if configured).
    if settings.task_retention_days > 0:
        async def _cleanup_loop() -> None:
            interval = settings.task_cleanup_interval_hours * 3600
            while True:
                try:
                    deleted = await db_purge_old_tasks(settings.task_retention_days)
                    if deleted:
                        logging.getLogger(__name__).info(
                            "Cleanup: purged %d old task(s)", deleted
                        )
                except Exception:
                    logging.getLogger(__name__).exception("Cleanup task failed")
                await asyncio.sleep(interval)

        asyncio.create_task(_cleanup_loop())


@app.on_event("shutdown")
async def _shutdown() -> None:
    shutdown_telemetry()


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "ragflow_url": settings.ragflow_base_url,
        "base_path": _raw_base or "/",
        "auth_enabled": bool(settings.jwt_secret_key),
        "ldap_enabled": bool(settings.ldap_server_uri),
        "auth_type": "ldap" if settings.ldap_server_uri else ("jwt" if settings.jwt_secret_key else "disabled"),
        "otel_enabled": settings.otel_enabled,
        "otel_endpoint": settings.otel_exporter_otlp_endpoint or settings.otel_exporter_otlp_traces_endpoint,
        "log_file_enabled": settings.log_file_enabled,
        "log_dir": settings.log_dir if settings.log_file_enabled else None,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "assessment.main:app",
        host=settings.host,
        port=settings.port,
        reload=True,
    )
