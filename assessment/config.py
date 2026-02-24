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
    # DB bootstrap mode:
    # - create: create missing tables only (safe default)
    # - recreate: drop all assessment tables and recreate from models (destructive)
    database_bootstrap_mode: str = "create"
    # Safety guard for PostgreSQL multi-pod deployments: destructive table
    # recreation is blocked unless explicitly allowed.
    database_allow_destructive_recreate: bool = False
    # Auto-cleanup: delete task rows older than this many days.
    # 0 = disabled (rows kept forever).
    task_retention_days: int = 0
    # How often the cleanup job runs (in hours). Default: every 24 h.
    task_cleanup_interval_hours: float = 24.0

    # Server
    host: str = "0.0.0.0"
    port: int = 8000
    api_base_path: str = ""  # e.g. "/assessment" to serve at /assessment/api/v1/...

    # Authentication / JWT
    jwt_secret_key: str = ""  # JWT signing key; leave empty to disable auth
    jwt_algorithm: str = "HS256"
    jwt_access_token_ttl_minutes: int = 30
    jwt_refresh_token_ttl_minutes: int = 10080  # 7 days

    # Logging
    log_level: str = "INFO"
    log_json: bool = True
    log_to_console: bool = True
    log_file_enabled: bool = True
    log_dir: str = "./logs"
    log_file_name: str = "assessment.log"
    log_max_bytes: int = 20 * 1024 * 1024  # 20 MB
    log_backup_count: int = 30

    # OpenTelemetry / OpenInference
    otel_enabled: bool = False
    otel_service_name: str = "assessment-api"
    otel_service_version: str = "1.0.0"
    otel_resource_attributes_json: str = "{}"  # JSON object merged into OTEL resource attrs
    otel_sample_ratio: float = 1.0
    otel_export_traces: bool = True
    otel_export_logs: bool = True
    otel_exporter_otlp_protocol: str = "http/protobuf"  # "http/protobuf" | "grpc"
    otel_exporter_otlp_endpoint: str = ""  # Base OTLP endpoint (e.g. http://collector:4318)
    otel_exporter_otlp_traces_endpoint: str = ""  # Optional full endpoint override
    otel_exporter_otlp_logs_endpoint: str = ""  # Optional full endpoint override
    otel_exporter_otlp_headers: str = ""  # JSON object or comma-separated key=value list
    otel_enable_console_exporter: bool = False
    otel_instrument_fastapi: bool = True
    otel_instrument_httpx: bool = True
    otel_instrument_sqlalchemy: bool = True
    openinference_enabled: bool = True

    # LDAP / Active Directory authentication
    # Set ldap_server_uri to enable LDAP-backed login endpoints.
    ldap_server_uri: str = ""
    ldap_use_ssl: bool = False
    ldap_start_tls: bool = False
    ldap_verify_ssl: bool = True
    ldap_ca_cert: str = ""
    ldap_connect_timeout_seconds: int = 10

    # User binding/search modes:
    # 1) Direct bind with template (e.g. "user@domain.local" or "CN={username},OU=Users,DC=example,DC=com")
    ldap_user_dn_template: str = ""
    # 2) Search bind mode (service account optional; anonymous search if left empty and allowed)
    ldap_bind_dn: str = ""
    ldap_bind_password: str = ""
    ldap_user_base_dn: str = ""
    ldap_user_filter: str = "(|(sAMAccountName={username})(uid={username})(cn={username}))"
    ldap_group_member_attribute: str = "memberOf"

    # Optional group search if memberOf is not populated
    ldap_group_search_base_dn: str = ""
    ldap_group_search_filter: str = "(|(member={user_dn})(memberUid={username}))"
    ldap_group_name_attribute: str = "cn"

    # Role mapping JSON. Example:
    # {"viewer":["CN=RGF-Readers,OU=Groups,DC=example,DC=com"],"operator":["RGF-Operators"],"admin":["RGF-Admins"]}
    ldap_group_role_mapping_json: str = (
        '{"viewer":[],"operator":[],"admin":[]}'
    )
    ldap_require_mapped_roles: bool = True

    model_config = {"env_prefix": "ASSESSMENT_", "env_file": ".env"}


settings = Settings()
