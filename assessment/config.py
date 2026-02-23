"""
Configuration for the Assessment API application.

All settings can be overridden via environment variables or a .env file
placed in the assessment directory.
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # RAGFlow connection
    ragflow_base_url: str = "http://localhost:9380"
    ragflow_api_key: str = ""

    # Processing
    max_concurrent_questions: int = 5
    polling_interval_seconds: float = 3.0
    document_parse_timeout_seconds: float = 600.0

    # Chat assistant defaults
    default_chat_name_prefix: str = "assessment"
    default_similarity_threshold: float = 0.1
    default_top_n: int = 8

    # Questions Excel column names (1-based column numbers or header names)
    # Accessible via env: ASSESSMENT_QUESTION_ID_COLUMN, ASSESSMENT_QUESTION_COLUMN, etc.
    question_id_column: str = "A"  # Column for Question Serial No (default: column A)
    question_column: str = "B"     # Column for Question text (default: column B)
    vendor_response_column: str = "C" # Column for Vendor response (default: column C)
    vendor_comment_column: str = "D"  # Column for Vendor comments (default: column D)

    # Flag to process vendor response
    process_vendor_response: bool = False

    # When True (default), only references actually cited as [ID:N] in the
    # LLM answer are kept.  Set to False to include all retrieved chunks.
    only_cited_references: bool = True

    # SSL / TLS
    verify_ssl: bool = True  # Set False to skip SSL certificate verification
    ssl_ca_cert: str = ""    # Path to a custom CA bundle or self-signed cert (PEM)

    # Database
    # SQLite (default):  sqlite+aiosqlite:///./assessment.db
    # PostgreSQL:        postgresql+asyncpg://user:pass@host/db   (asyncpg is included by default)
    database_url: str = "sqlite+aiosqlite:///./assessment.db"

    # Auto-cleanup: delete task rows older than this many days.
    # 0 = disabled (rows kept forever).
    task_retention_days: int = 0
    # How often the cleanup job runs (in hours). Default: every 24 h.
    task_cleanup_interval_hours: float = 24.0

    # Server
    host: str = "0.0.0.0"
    port: int = 8000
    api_base_path: str = ""  # e.g. "/assessment" to serve at /assessment/api/v1/...

    # Authentication
    jwt_secret_key: str = ""  # Fixed JWT secret; leave empty to disable auth

    model_config = {"env_prefix": "ASSESSMENT_", "env_file": ".env"}


settings = Settings()
