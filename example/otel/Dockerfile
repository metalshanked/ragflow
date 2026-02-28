# ============================================================
# RAGFlow + OpenTelemetry + Uvicorn/Gunicorn Dockerfile
#
# Layers OTel auto-instrumentation and production-grade servers
# on top of the official image using simple sed patches.
#
# - Uvicorn replaces Quart's dev-mode app.run() for the API server
#   (uvicorn is already installed as an MCP server dependency).
# - Gunicorn replaces werkzeug's dev server for the Admin server.
#
# ------------------------------------------------------------
# ASGI/WSGI analysis (what needs patching and what doesn't):
# ------------------------------------------------------------
#
#   ragflow_server.py  (Web/API)    → Quart (ASGI) but uses dev-mode
#                                     app.run() which runs Hypercorn in
#                                     single-worker mode. PATCHED to use
#                                     Uvicorn directly with configurable
#                                     workers via UVICORN_WORKERS env var.
#
#   admin_server.py    (Admin)      → Flask (WSGI) + werkzeug.run_simple().
#                                     PATCHED to use Gunicorn with
#                                     configurable workers via
#                                     GUNICORN_WORKERS env var.
#
#   task_executor.py   (Workers)    → Long-running worker process, not a
#                                     web server — no patch needed.
#
#   mcp/server/server.py (MCP)     → Starlette app, ALREADY uses uvicorn
#                                     internally — no patch needed.
#
#   sync_data_source.py (DataSync) → Worker process — no patch needed.
#
# ragflow_server.py and admin_server.py are the two components
# using dev-mode servers that get patched.
#
# ------------------------------------------------------------
# Why Uvicorn for the API server?
# ------------------------------------------------------------
#
#   - Already installed — uvicorn is an MCP server dependency,
#     so it's already in the image. No extra package needed.
#   - Native ASGI — Uvicorn is the most widely used ASGI server.
#   - Workers support — uvicorn.run(workers=N) for multi-process.
#   - Same server as MCP — consistency across components.
#
# ------------------------------------------------------------
# Why Gunicorn for the Admin server?
# ------------------------------------------------------------
#
#   - Industry standard — Gunicorn is the de-facto production
#     WSGI server for Flask applications.
#   - Multi-worker — configurable workers for concurrency.
#   - Replaces werkzeug's dev server which is not suitable
#     for production use.
#
# ------------------------------------------------------------
# How it works
# ------------------------------------------------------------
#
#   1. Installs gunicorn, OTel packages (distro + exporter + instrumentors)
#      via uv. Uvicorn is already present in the base image.
#   2. (Optional) Bootstraps auto-discovered instrumentations
#      — disabled by default due to version conflicts.
#   3. Adds a small Python launcher wrapper with OTEL flags:
#      - RAGFLOW_OTEL_ENABLED (global on/off)
#      - per-service toggles (Postgres, Redis, ORM, HTTP, etc.)
#   4. Patches entrypoint.sh via sed — replaces "$PY" with the wrapper
#      so every long-running Python process follows those OTEL flags.
#   5. Patches ragflow_server.py via sed — replaces app.run()
#      with uvicorn.run() wrapped in OpenTelemetryMiddleware
#      for inbound request tracing + UVICORN_WORKERS support.
#      (Quart has no auto-instrumentor, so manual ASGI middleware
#      is required to generate server spans for incoming requests.)
#   6. Patches admin_server.py via sed — replaces werkzeug's
#      run_simple() with Gunicorn, supporting GUNICORN_WORKERS
#      env var for multi-worker mode.
#   7. Merges ELASTIC_INDEX_SETTINGS env var into conf/mapping.json
#      at container startup (deep-merge into "settings" section).
#   8. Patches MinIO client to respect 'secure' config setting.
#   9. Patches MinIO health check to use https when secure is on.
#
#   No wrapt, no .pth files.
#
# ------------------------------------------------------------
# Build
# ------------------------------------------------------------
#
#   docker build -f example/otel/Dockerfile -t ragflow-otel:latest .
#
# ------------------------------------------------------------
# Run examples
# ------------------------------------------------------------
#
# All-in-one (default):
#
#   docker run \
#     -e OTEL_SERVICE_NAME=ragflow \
#     -e OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317 \
#     ragflow-otel:latest
#
# Distributed — API server only:
#
#   docker run \
#     -e OTEL_SERVICE_NAME=ragflow-api \
#     -e OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317 \
#     -e UVICORN_WORKERS=4 \
#     ragflow-otel:latest --disable-taskexecutor
#
# Distributed — task executor only:
#
#   docker run \
#     -e OTEL_SERVICE_NAME=ragflow-executor \
#     -e OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317 \
#     ragflow-otel:latest --disable-webserver --workers=4
#
# With MCP server:
#
#   docker run \
#     -e OTEL_SERVICE_NAME=ragflow-api \
#     -e OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317 \
#     ragflow-otel:latest --enable-mcpserver --disable-taskexecutor
#
# With Admin server:
#
#   docker run \
#     -e OTEL_SERVICE_NAME=ragflow-admin \
#     -e OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317 \
#     -e GUNICORN_WORKERS=2 \
#     ragflow-otel:latest --enable-adminserver --disable-taskexecutor
#
# ------------------------------------------------------------
# Environment variables — Elasticsearch Index Settings
# ------------------------------------------------------------
#
#   ELASTIC_INDEX_SETTINGS  JSON string that is deep-merged into the
#                           "settings" section of conf/mapping.json at
#                           container startup.  Allows overriding or
#                           extending index settings (shards, replicas,
#                           analysis, similarity, etc.) without
#                           rebuilding the image.
#
#   Examples:
#
#     Override shard/replica counts:
#       ELASTIC_INDEX_SETTINGS='{"index":{"number_of_shards":3,"number_of_replicas":1}}'
#
#     Add a custom analyzer:
#       ELASTIC_INDEX_SETTINGS='{"analysis":{"analyzer":{"my_analyzer":{"type":"custom","tokenizer":"standard"}}}}'
#
#     Change refresh interval:
#       ELASTIC_INDEX_SETTINGS='{"index":{"refresh_interval":"5s"}}'
#
# ------------------------------------------------------------
# Environment variables — Uvicorn (API server)
# ------------------------------------------------------------
#
#   UVICORN_WORKERS     Number of Uvicorn worker processes
#                       for the API server (default: 1)
#
# ------------------------------------------------------------
# Environment variables — Gunicorn (Admin server)
# ------------------------------------------------------------
#
#   GUNICORN_WORKERS    Number of Gunicorn worker processes
#                       for the Admin server (default: 2)
#
# ------------------------------------------------------------
# Environment variables — OpenTelemetry
# ------------------------------------------------------------
#
#   OTEL_SERVICE_NAME              Logical service name shown in
#                                  your tracing backend.
#                                  Example: ragflow-api
#
#   RAGFLOW_OTEL_ENABLED           Global OTEL switch for all RAGFlow
#                                  Python services in this image.
#                                  Default: true
#                                  Set false to disable OTEL entirely.
#
#   RAGFLOW_OTEL_INSTRUMENT_POSTGRES / RAGFLOW_OTEL_DISABLE_POSTGRES
#                                  Enable/disable PostgreSQL telemetry
#                                  (psycopg2 instrumentation).
#                                  Default enabled.
#
#   RAGFLOW_OTEL_INSTRUMENT_REDIS / RAGFLOW_OTEL_DISABLE_REDIS
#                                  Enable/disable Redis telemetry.
#                                  Default enabled.
#
#   RAGFLOW_OTEL_INSTRUMENT_ELASTICSEARCH / RAGFLOW_OTEL_DISABLE_ELASTICSEARCH
#                                  Enable/disable Elasticsearch telemetry.
#                                  Default enabled.
#
#   RAGFLOW_OTEL_INSTRUMENT_ORM / RAGFLOW_OTEL_DISABLE_ORM
#                                  Enable/disable ORM telemetry (Peewee).
#                                  Default enabled.
#
#   RAGFLOW_OTEL_INSTRUMENT_HTTP / RAGFLOW_OTEL_DISABLE_HTTP
#                                  Enable/disable outbound HTTP client telemetry
#                                  for requests + urllib3 (includes MinIO HTTP calls).
#                                  Default enabled.
#
#   RAGFLOW_OTEL_INSTRUMENT_FLASK / RAGFLOW_OTEL_DISABLE_FLASK
#                                  Enable/disable Flask telemetry
#                                  (primarily Admin server).
#                                  Default enabled.
#
#   RAGFLOW_OTEL_INSTRUMENT_ASGI / RAGFLOW_OTEL_DISABLE_ASGI
#                                  Enable/disable inbound ASGI middleware
#                                  spans for ragflow_server.
#                                  Default enabled.
#
#   OTEL_EXPORTER_OTLP_ENDPOINT   OTLP collector endpoint.
#                                  Example: http://otel-collector:4317
#
#   OTEL_TRACES_EXPORTER           Exporter type (default: otlp).
#                                  Options: otlp, console, none
#
#   OTEL_METRICS_EXPORTER          Metrics exporter (default: otlp).
#                                  Options: otlp, console, none
#
#   OTEL_LOGS_EXPORTER             Logs exporter (default: otlp).
#                                  Options: otlp, console, none
#
#   OTEL_EXPORTER_OTLP_PROTOCOL   Transport protocol.
#                                  Options: grpc, http/protobuf
#                                  Default: grpc
#
#   OTEL_EXPORTER_OTLP_HEADERS    Extra headers for auth, e.g.:
#                                  "Authorization=Bearer <token>"
#
#   OTEL_EXPORTER_OTLP_INSECURE   Skip TLS verification for the
#                                  collector endpoint (true/false).
#
#   OTEL_RESOURCE_ATTRIBUTES       Additional resource attributes,
#                                  comma-separated key=value pairs.
#                                  Example: deployment.environment=prod,service.version=1.0
#
#   OTEL_PROPAGATORS               Context propagation formats.
#                                  Default: tracecontext,baggage
#                                  Options: tracecontext, baggage,
#                                           b3, b3multi, jaeger,
#                                           xray, ottrace
#
#   OTEL_TRACES_SAMPLER            Sampler type.
#                                  Default: parentbased_always_on
#                                  Options: always_on, always_off,
#                                           traceidratio,
#                                           parentbased_always_on,
#                                           parentbased_always_off,
#                                           parentbased_traceidratio
#
#   OTEL_TRACES_SAMPLER_ARG        Sampler argument (e.g. ratio
#                                  for traceidratio: 0.1 = 10%)
#
#   OTEL_BSP_SCHEDULE_DELAY_MILLIS Batch span processor export
#                                  interval in ms (default: 5000)
#
#   OTEL_BSP_MAX_EXPORT_BATCH_SIZE Max spans per export batch
#                                  (default: 512)
#
# All standard OTel SDK env vars are supported. Full reference:
#   https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/
#
# ------------------------------------------------------------
# LDAP Configuration (Environment Variables)
# ------------------------------------------------------------
#
#   LDAP_ENABLED             Set to "true" to enable LDAP authentication.
#   LDAP_SERVER              LDAP server URL (e.g., ldap://ldap.example.com or ldaps://...).
#   LDAP_BASE_DN             Base DN for user search (e.g., dc=example,dc=com).
#   LDAP_BIND_USER           (Optional) Bind DN for user search.
#   LDAP_BIND_PASSWORD       (Optional) Password for bind user.
#   LDAP_USER_FILTER         (Optional) LDAP search filter. The %s placeholder is
#                            replaced with whatever the user types in the "email"
#                            field on the RAGFlow login page. Default: (mail=%s).
#   LDAP_GROUP_MAPPING       (Optional) JSON map of LDAP group DN or CN to a
#                            RAGFlow role string. Recognised role values:
#                              "superuser" – sets the is_superuser flag (system-
#                                            wide admin; can manage all tenants).
#                              "admin"     – UserTenantRole.ADMIN inside the
#                                            group's tenant (can manage that
#                                            tenant's knowledgebases/chat, etc.).
#                              "normal"    – UserTenantRole.NORMAL (default;
#                                            regular member of the tenant).
#                            If LDAP_GROUP_MAPPING is empty or unset, every
#                            authenticated LDAP user is treated as "normal".
#   LDAP_TENANT_GROUP_PREFIX (Optional) Sync groups starting with this prefix as tenants (default: "").
#   LDAP_GROUP_EMAIL_DOMAIN  (Optional) Domain suffix for the dummy owner email created for
#                            group tenants (default: "ragflow.org").
#                            Example: LDAP_GROUP_EMAIL_DOMAIN=yourcompany.com produces
#                            ldap_group_<sanitized_cn>@yourcompany.com.
#   LDAP_VERIFY_CERTS        (Optional) Set to "false" to disable certificate verification (default: true).
#
#   LDAP_USER_FILTER examples
#   -------------------------
#   The filter controls how RAGFlow looks up users in the directory.
#   Choose the filter that matches what your users type at login:
#
#     Login with email address (user types john@corp.com):
#       LDAP_USER_FILTER=(mail=%s)                  # matches the LDAP 'mail' attribute
#       LDAP_USER_FILTER=(userPrincipalName=%s)      # matches AD UPN (usually same as email)
#
#     Login with username (user types jdoe):
#       LDAP_USER_FILTER=(sAMAccountName=%s)         # AD username
#       LDAP_USER_FILTER=(uid=%s)                    # OpenLDAP / FreeIPA username
#
#     Restrict to a specific AD group (email login, must be in 'RagFlowUsers'):
#       LDAP_USER_FILTER=(&(mail=%s)(memberOf=CN=RagFlowUsers,OU=Groups,DC=corp,DC=com))
#
#   Full examples
#   -------------
#
#     1. Active Directory – login with email, self-signed cert:
#        LDAP_ENABLED=true
#        LDAP_SERVER=ldaps://ad.corp.com:636
#        LDAP_BASE_DN=DC=corp,DC=com
#        LDAP_BIND_USER=CN=svc_ragflow,OU=ServiceAccounts,DC=corp,DC=com
#        LDAP_BIND_PASSWORD=secret
#        LDAP_USER_FILTER=(mail=%s)
#        LDAP_VERIFY_CERTS=false
#
#     2. Active Directory – login with UPN (email-style, e.g. jdoe@corp.com):
#        LDAP_ENABLED=true
#        LDAP_SERVER=ldaps://ad.corp.com:636
#        LDAP_BASE_DN=DC=corp,DC=com
#        LDAP_BIND_USER=CN=svc_ragflow,OU=ServiceAccounts,DC=corp,DC=com
#        LDAP_BIND_PASSWORD=secret
#        LDAP_USER_FILTER=(userPrincipalName=%s)
#        LDAP_VERIFY_CERTS=false
#
#     3. Active Directory – login with sAMAccountName (username, e.g. jdoe):
#        LDAP_ENABLED=true
#        LDAP_SERVER=ldaps://ad.corp.com:636
#        LDAP_BASE_DN=DC=corp,DC=com
#        LDAP_BIND_USER=CN=svc_ragflow,OU=ServiceAccounts,DC=corp,DC=com
#        LDAP_BIND_PASSWORD=secret
#        LDAP_USER_FILTER=(sAMAccountName=%s)
#        LDAP_VERIFY_CERTS=false
#
#     4. OpenLDAP – login with email:
#        LDAP_ENABLED=true
#        LDAP_SERVER=ldap://openldap.example.org
#        LDAP_BASE_DN=ou=users,dc=example,dc=org
#        LDAP_USER_FILTER=(mail=%s)
#
#     5. LDAP_GROUP_MAPPING examples (JSON):
#
#        RAGFlow has two effective user levels:
#          • superuser (is_superuser=True) – system-wide admin
#          • normal user (is_superuser=False) – regular user
#        Inside a tenant, users can be OWNER, ADMIN, or NORMAL.
#        LDAP group mapping controls both dimensions.
#
#        a) Make members of "Domain Admins" superusers, everyone else normal:
#           LDAP_GROUP_MAPPING={"CN=Domain Admins,CN=Users,DC=corp,DC=com": "superuser"}
#
#        b) Using just the CN (short name) instead of the full DN:
#           LDAP_GROUP_MAPPING={"Domain Admins": "superuser"}
#
#        c) Multiple groups – superusers + tenant admins + normal:
#           LDAP_GROUP_MAPPING={"RagFlow-Superadmins": "superuser", "RagFlow-Admins": "admin", "RagFlow-Users": "normal"}
#
#        d) Full DN example with AD-style paths:
#           LDAP_GROUP_MAPPING={"CN=RagFlow-Superadmins,OU=Groups,DC=corp,DC=com": "superuser", "CN=RagFlow-Admins,OU=Groups,DC=corp,DC=com": "admin"}
#
#        If a user is not in any mapped group and LDAP_GROUP_MAPPING is set,
#        login is denied.  If LDAP_GROUP_MAPPING is empty/unset, all
#        authenticated LDAP users are allowed with "normal" role.
#
# ------------------------------------------------------------
# RAGFlow entrypoint flags
# ------------------------------------------------------------
#
#   --disable-webserver      Disable nginx + ragflow_server (API)
#   --disable-taskexecutor   Disable task executor workers
#   --disable-datasync       Disable data source sync
#   --enable-mcpserver       Enable MCP server (Starlette + uvicorn)
#   --enable-adminserver     Enable Admin server (Flask/Gunicorn)
#   --workers=N              Number of task executor workers
#
# ============================================================

FROM infiniflow/ragflow:v0.24.0
ARG INSTALL_DOCLING=0
ARG UV_PIP_INDEX_URL=https://pypi.org/simple
# Company mirror example: https://artifactor.cera.com/api/pypi/pypi/simple
ARG APT_MIRROR_URL=https://archive.ubuntu.com/ubuntu
# Company mirror example: https://artifactory.cera.com/ubuntu-apt/ubuntu
ARG MAVEN_REPO_BASE_URL=https://repo1.maven.org/maven2
# Company mirror example: https://artifactory.cera.com/iq-maven-central-proxy
ARG NODEJS_ORG_MIRROR=https://nodejs.org
# Company mirror example: https://artifactory.cera.com/nodejs-proxy
ARG NODE_VERSION=24.14.0
ARG NODE_EXTRA_CA_CERT_PEM_B64=""
ARG REMOVE_PYTHON_PACKAGES="crawl4ai"
ARG SECURITY_PYTHON_SAFE_SPECS="setuptools==82.0.0 nltk==3.9.3 pdfminer.six==20251230 protobuf==5.29.6 google-cloud-aiplatform==1.133.0 mcp==1.23.0 pillow==12.1.1 python-multipart==0.0.22 pyasn1==0.6.2 azure-core==1.38.0"

# Global uv package index for all installs in this Dockerfile.
# Override at build time to use a custom mirror:
#   --build-arg UV_PIP_INDEX_URL=https://pypi.org/simple
#   --build-arg APT_MIRROR_URL=https://archive.ubuntu.com/ubuntu
#   --build-arg MAVEN_REPO_BASE_URL=https://repo1.maven.org/maven2
#   --build-arg NODEJS_ORG_MIRROR=https://nodejs.org
#   --build-arg NODE_VERSION=24.14.0
#   --build-arg NODE_EXTRA_CA_CERT_PEM_B64=<base64_pem>
#   --build-arg REMOVE_PYTHON_PACKAGES="crawl4ai,package-two"  (default: crawl4ai)
#   --build-arg SECURITY_PYTHON_SAFE_SPECS="<space-separated package pins>"
# Exports both env var names for compatibility across uv versions.
ENV UV_NO_CONFIG=1 \
    UV_INDEX_STRATEGY=first-index \
    UV_DEFAULT_INDEX=${UV_PIP_INDEX_URL} \
    UV_INDEX_URL=${UV_PIP_INDEX_URL} \
    UV_CACHE_DIR=/tmp/uv-cache \
    PIP_INDEX_URL=${UV_PIP_INDEX_URL}

# Central mirror controls for security remediation steps.
ENV APT_MIRROR_URL=${APT_MIRROR_URL} \
    MAVEN_REPO_BASE_URL=${MAVEN_REPO_BASE_URL} \
    NODEJS_ORG_MIRROR=${NODEJS_ORG_MIRROR} \
    NODE_VERSION=${NODE_VERSION} \
    REMOVE_PYTHON_PACKAGES=${REMOVE_PYTHON_PACKAGES} \
    SECURITY_PYTHON_SAFE_SPECS=${SECURITY_PYTHON_SAFE_SPECS}


# Use C.UTF-8 by default because en_US.UTF-8 may not exist without locale packages.
ENV LANG=C.UTF-8 \
    LANGUAGE=en_US:en \
    LC_ALL=C.UTF-8 \
    RAGFLOW_SET_ENGLISH=true \
    RAGFLOW_LANG_SHIM_DEBUG=false

# Install a runtime language shim through sitecustomize (upgrade-safe):
# - normalizes missing/non-English lang to English
# - patches LLMBundle, TenantLLMService.model_instance, and direct CvModel/Seq2txtModel constructors
RUN python3 - <<'PY'
import site
import textwrap
from pathlib import Path

shim = textwrap.dedent(
    """
    import builtins
    import functools
    import inspect
    import logging
    import os
    import sys

    def _truthy(value):
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def _debug_enabled():
        return _truthy(os.getenv("RAGFLOW_LANG_SHIM_DEBUG", "false"))

    def _debug(msg, *args):
        if _debug_enabled():
            logging.warning(msg, *args)

    def _set_english_enabled():
        return _truthy(os.getenv("RAGFLOW_SET_ENGLISH", "true"))

    def _is_english_lang(value):
        s = str(value).strip().lower()
        if not s:
            return False
        if s in {"english", "en", "eng", "en-us", "en_us", "en-gb", "en_gb"}:
            return True
        return s.startswith("en-") or s.startswith("en_")

    def _normalize_lang(lang):
        if not _set_english_enabled():
            return lang
        if lang is None:
            return "English"
        s = str(lang).strip()
        if not s:
            return "English"
        if _is_english_lang(s):
            return "English"
        return "English"

    def _patch_ctor_with_lang_param(cls):
        init = getattr(cls, "__init__", None)
        if not callable(init) or getattr(init, "__ragflow_lang_patch__", False):
            return

        try:
            sig = inspect.signature(init)
        except Exception:
            return

        params = list(sig.parameters.keys())
        if "lang" not in params:
            return

        lang_index = params.index("lang") - 1  # self removed from *args
        original = init

        @functools.wraps(original)
        def wrapped(self, *args, **kwargs):
            before = kwargs.get("lang")
            after = None
            if "lang" in kwargs:
                kwargs["lang"] = _normalize_lang(kwargs["lang"])
                after = kwargs["lang"]
            elif lang_index >= 0 and len(args) > lang_index:
                mutable = list(args)
                before = mutable[lang_index]
                mutable[lang_index] = _normalize_lang(mutable[lang_index])
                args = tuple(mutable)
                after = mutable[lang_index]
            else:
                kwargs["lang"] = _normalize_lang(None)
                before = None
                after = kwargs["lang"]
            _debug("ragflow lang shim ctor patch: %s lang %r -> %r", cls.__name__, before, after)
            return original(self, *args, **kwargs)

        wrapped.__ragflow_lang_patch__ = True
        cls.__init__ = wrapped

    def _patch_llm_bundle():
        mod = sys.modules.get("api.db.services.llm_service")
        if not mod:
            return
        bundle = getattr(mod, "LLMBundle", None)
        if bundle is None:
            return
        _patch_ctor_with_lang_param(bundle)

    def _patch_tenant_model_instance():
        mod = sys.modules.get("api.db.services.tenant_llm_service")
        if not mod:
            return
        svc = getattr(mod, "TenantLLMService", None)
        if svc is None:
            return

        method = getattr(svc, "model_instance", None)
        base = getattr(method, "__func__", None)
        if not callable(base) or getattr(base, "__ragflow_lang_patch__", False):
            return

        @functools.wraps(base)
        def wrapped(cls, tenant_id, llm_type, llm_name=None, lang=None, **kwargs):
            normalized = _normalize_lang(lang)
            _debug("ragflow lang shim tenant model_instance: %r -> %r", lang, normalized)
            return base(cls, tenant_id, llm_type, llm_name, normalized, **kwargs)

        wrapped.__ragflow_lang_patch__ = True
        svc.model_instance = classmethod(wrapped)

    def _patch_model_registries():
        llm_mod = sys.modules.get("rag.llm")
        if llm_mod:
            for attr in ("CvModel", "Seq2txtModel"):
                registry = getattr(llm_mod, attr, None)
                if isinstance(registry, dict):
                    for cls in set(registry.values()):
                        _patch_ctor_with_lang_param(cls)

        for mod_name in ("rag.llm.cv_model", "rag.llm.sequence2txt_model"):
            mod = sys.modules.get(mod_name)
            if not mod:
                continue
            for _, cls in inspect.getmembers(mod, inspect.isclass):
                _patch_ctor_with_lang_param(cls)

    def _apply_patches():
        _patch_llm_bundle()
        _patch_tenant_model_instance()
        _patch_model_registries()

    _orig_import = builtins.__import__

    @functools.wraps(_orig_import)
    def _import_hook(name, globals=None, locals=None, fromlist=(), level=0):
        module = _orig_import(name, globals, locals, fromlist, level)
        try:
            if name.startswith(("api.db.services", "rag.llm")):
                _apply_patches()
            elif fromlist:
                for item in fromlist:
                    if isinstance(item, str) and item.startswith("rag.llm"):
                        _apply_patches()
                        break
        except Exception as e:
            logging.debug("ragflow lang shim import hook skipped: %s", e)
        return module

    builtins.__import__ = _import_hook
    _apply_patches()
    _debug("ragflow language shim loaded (set_english=%s)", _set_english_enabled())
    """
)

site_pkg = Path(site.getsitepackages()[0])
shim_target = site_pkg / "ragflow_lang_shim.py"
shim_target.write_text(shim, encoding="utf-8")

sitecustomize = site_pkg / "sitecustomize.py"
marker = "import ragflow_lang_shim  # ragflow-lang-shim"
if sitecustomize.exists():
    existing = sitecustomize.read_text(encoding="utf-8", errors="ignore")
    if marker not in existing:
        if existing and not existing.endswith("\n"):
            existing += "\n"
        sitecustomize.write_text(existing + marker + "\n", encoding="utf-8")
else:
    sitecustomize.write_text(marker + "\n", encoding="utf-8")

print(f"Installed language shim module at: {shim_target}")
print(f"Ensured sitecustomize loader at: {sitecustomize}")
PY

# Inline LDAP Auth Script
# -----------------------
# Hardened implementation that:
# 1. Enabled only if LDAP_ENABLED=true (env var).
# 2. Uses 'user_register' to reuse upstream logic for user/tenant creation.
# 3. Features robust argument extraction to resist upstream signature changes.
# 4. Gracefully falls back to original auth if LDAP fails or is disabled.
# 5. Requires 'ldap3' package (installed via uv).
RUN printf '%s\n' \
    "" \
    "" \
    "import os" \
    "import json" \
    "import logging" \
    "import ssl" \
    "import base64" \
    "from api.db.services.user_service import UserService, UserTenantService, TenantService" \
    "from api.db.services.api_service import APITokenService" \
    "from api.db import UserTenantRole" \
    "from api.db.db_models import UserTenant, User" \
    "from common.constants import StatusEnum" \
    "from common.misc_utils import get_uuid" \
    "" \
    "# user_register is imported lazily inside functions that need it, because" \
    "# importing api.apps.user_app at module level triggers Flask/Quart Blueprint" \
    "# decorators (@manager.route) which fail if the app hasn't been initialised yet." \
    "# ldap_auth.py is imported very early (line 1 of ragflow_server.py)." \
    "" \
    "# Try to import ldap3, if not available, log warning and skip" \
    "try:" \
    "    from ldap3 import Server, Connection, ALL, SUBTREE, Tls" \
    "    from ldap3.utils.conv import escape_filter_chars" \
    "    LDAP_AVAILABLE = True" \
    "except ImportError:" \
    "    LDAP_AVAILABLE = False" \
    "    logging.warning(\"ldap3 module not found. LDAP authentication will be disabled.\")" \
    "" \
    "# Check environment" \
    "LDAP_ENABLED = os.environ.get(\"LDAP_ENABLED\", \"False\").lower() == \"true\"" \
    "" \
    "# Save originals before patching" \
    "original_query_user = UserService.query_user" \
    "original_get_joined_tenants = getattr(TenantService, 'get_joined_tenants_by_user_id', None)" \
    "" \
    "def ldap_login(email, password):" \
    "    if not LDAP_ENABLED:" \
    "        return None" \
    "    " \
    "    if not LDAP_AVAILABLE:" \
    "        logging.error(\"LDAP_ENABLED is true but ldap3 module is not installed.\")" \
    "        return None" \
    "" \
    "    ldap_server_url = os.environ.get(\"LDAP_SERVER\")" \
    "    ldap_bind_user = os.environ.get(\"LDAP_BIND_USER\")" \
    "    ldap_bind_password = os.environ.get(\"LDAP_BIND_PASSWORD\")" \
    "    ldap_base_dn = os.environ.get(\"LDAP_BASE_DN\")" \
    "    ldap_user_filter = os.environ.get(\"LDAP_USER_FILTER\", \"(mail=%s)\")" \
    "    ldap_group_mapping_str = os.environ.get(\"LDAP_GROUP_MAPPING\", \"{}\")" \
    "    ldap_verify_certs = os.environ.get(\"LDAP_VERIFY_CERTS\", \"True\").lower() == \"true\"" \
    "" \
    "    if not ldap_server_url or not ldap_base_dn:" \
    "        logging.error(\"LDAP configuration incomplete (LDAP_SERVER, LDAP_BASE_DN required).\")" \
    "        return None" \
    "" \
    "    try:" \
    "        group_mapping = json.loads(ldap_group_mapping_str)" \
    "    except json.JSONDecodeError:" \
    "        logging.error(\"Invalid JSON in LDAP_GROUP_MAPPING.\")" \
    "        return None" \
    "" \
    "    tls_ctx = None" \
    "    if ldap_server_url.lower().startswith(\"ldaps://\") and not ldap_verify_certs:" \
    "        try:" \
    "            tls_ctx = Tls(validate=ssl.CERT_NONE)" \
    "            logging.warning(\"LDAP: Certificate verification disabled (LDAPS).\")" \
    "        except Exception as e:" \
    "            logging.error(f\"LDAP: Failed to create TLS context: {e}\")" \
    "" \
    "    server = Server(ldap_server_url, get_info=ALL, tls=tls_ctx)" \
    "    " \
    "    # 1. Bind with service account (or anonymous) to search for user" \
    "    try:" \
    "        if ldap_bind_user and ldap_bind_password:" \
    "            conn = Connection(server, user=ldap_bind_user, password=ldap_bind_password, auto_bind=True)" \
    "        else:" \
    "            conn = Connection(server, auto_bind=True)" \
    "    except Exception as e:" \
    "        logging.error(f\"LDAP Bind failed: {e}\")" \
    "        return None" \
    "" \
    "    # 2. Search for user" \
    "    # Escape user input to prevent LDAP injection (*, (, ), \\, NUL, etc.)" \
    "    search_filter = ldap_user_filter % escape_filter_chars(email)" \
    "    try:" \
    "        conn.search(ldap_base_dn, search_filter, attributes=['cn', 'mail', 'memberOf', 'sAMAccountName', 'displayName'])" \
    "        " \
    "        if not conn.entries:" \
    "            logging.info(f\"LDAP user not found for email: {email}\")" \
    "            return \"user_not_found\"" \
    "        " \
    "        user_entry = conn.entries[0]" \
    "        user_dn = user_entry.entry_dn" \
    "        user_attributes = user_entry.entry_attributes_as_dict" \
    "        " \
    "        # Get nickname from cn or displayName or sAMAccountName or mail" \
    "        nickname = user_attributes.get('displayName', [None])[0] or \\" \
    "                   user_attributes.get('cn', [None])[0] or \\" \
    "                   user_attributes.get('sAMAccountName', [None])[0] or \\" \
    "                   email.split('@')[0]" \
    "" \
    "        # 3. Authenticate user (Bind with user DN and password)" \
    "        # The password arriving here has been through decrypt() in user_app.py," \
    "        # which returns base64(original_password) — NOT the raw password." \
    "        # We must base64-decode it to get the real password for the LDAP bind." \
    "        try:" \
    "            actual_password = base64.b64decode(password).decode('utf-8')" \
    "        except Exception:" \
    "            actual_password = password  # fallback: use as-is if not base64" \
    "        try:" \
    "            user_conn = Connection(server, user=user_dn, password=actual_password, auto_bind=True)" \
    "            user_conn.unbind()" \
    "        except Exception as e:" \
    "            logging.warning(f\"LDAP authentication failed for user {user_dn}: {e}\")" \
    "            return \"bad_password\"" \
    "" \
    "        # 4. Check groups" \
    "        user_groups = user_attributes.get('memberOf', [])" \
    "        " \
    "        # Some LDAP setups don't populate memberOf on user objects" \
    "        if not user_groups:" \
    "            conn.search(ldap_base_dn, f\"(member={user_dn})\", attributes=['dn'])" \
    "            user_groups = [entry.entry_dn for entry in conn.entries]" \
    "    finally:" \
    "        conn.unbind()" \
    "" \
    "    # Normalize groups for mapping — pick the highest role across all groups" \
    "    ROLE_PRIORITY = {\"normal\": 0, \"admin\": 1, \"superuser\": 2}" \
    "    matched_role = None" \
    "    is_superuser = False" \
    "    best_priority = -1" \
    "    " \
    "    if not group_mapping:" \
    "        matched_role = \"normal\"" \
    "    else:" \
    "        # Build a case-insensitive lookup: lowercase key -> original value" \
    "        ci_mapping = {k.lower(): v for k, v in group_mapping.items()}" \
    "        for group in user_groups:" \
    "            role = None" \
    "            if group.lower() in ci_mapping:" \
    "                role = ci_mapping[group.lower()]" \
    "            else:" \
    "                cn = group.split(',')[0]" \
    "                if cn.upper().startswith(\"CN=\"):" \
    "                    cn = cn[3:]" \
    "                if cn.lower() in ci_mapping:" \
    "                    role = ci_mapping[cn.lower()]" \
    "            " \
    "            if role:" \
    "                priority = ROLE_PRIORITY.get(role.lower(), 0)" \
    "                if priority > best_priority:" \
    "                    best_priority = priority" \
    "                    matched_role = role" \
    "                if role.lower() == \"superuser\":" \
    "                    is_superuser = True" \
    "    " \
    "    if not matched_role:" \
    "        logging.info(f\"LDAP user {email} authenticated but not in any mapped group.\")" \
    "        if group_mapping:" \
    "             return \"not_in_group\"" \
    "        matched_role = \"normal\"" \
    "" \
    "    return {" \
    "        \"email\": email," \
    "        \"nickname\": nickname," \
    "        \"is_superuser\": is_superuser," \
    "        \"groups\": user_groups" \
    "    }" \
    "" \
    "def get_or_create_group_tenant(group_dn):" \
    "    \"\"\"Get or create a tenant for an LDAP group using upstream user_register.\"\"\"" \
    "    cn = group_dn.split(',')[0]" \
    "    if cn.upper().startswith('CN='):" \
    "        cn = cn[3:]" \
    "    if not cn:" \
    "        return None" \
    "    tenant_name = cn[:100]" \
    "    tenant_display_name = tenant_name + \"'s Kingdom\"" \
    "    try:" \
    "        tenant = TenantService.model.select().where(TenantService.model.name == tenant_display_name).first()" \
    "    except Exception as e:" \
    "        logging.error(f\"Error querying tenant {tenant_name}: {e}\")" \
    "        return None" \
    "    if tenant:" \
    "        return tenant" \
    "    logging.info(f\"Creating new tenant for LDAP group: {tenant_name}\")" \
    "    safe_name = ''.join([c if c.isalnum() else '_' for c in tenant_name])" \
    "    group_email_domain = os.environ.get('LDAP_GROUP_EMAIL_DOMAIN', 'ragflow.org')" \
    "    dummy_email = f\"ldap_group_{safe_name}@{group_email_domain}\"" \
    "    dummy_user = UserService.query_user_by_email(dummy_email)" \
    "    if dummy_user:" \
    "        dummy_user = dummy_user[0]" \
    "        tenant = TenantService.get_info_by(dummy_user.id)" \
    "        if tenant:" \
    "             t_id = tenant[0]['tenant_id']" \
    "             return TenantService.model.get_by_id(t_id)" \
    "    import sys" \
    "    user_register = sys.modules['api.apps.user'].user_register" \
    "    user_id = get_uuid()" \
    "    user_dict = {" \
    "        \"email\": dummy_email," \
    "        \"nickname\": tenant_name," \
    "        \"password\": get_uuid()," \
    "        \"is_superuser\": False," \
    "    }" \
    "    try:" \
    "        result = user_register(user_id, user_dict)" \
    "        if not result:" \
    "            logging.error(f\"user_register failed for group tenant {tenant_name}\")" \
    "            return None" \
    "        return TenantService.model.get_by_id(user_id)" \
    "    except Exception as e:" \
    "        logging.error(f\"Error creating tenant for group {tenant_name}: {e}\")" \
    "        return None" \
    "" \
    "def sync_user_to_group_tenants(user, groups):" \
    "    if not groups:" \
    "        return" \
    "" \
    "    # Load mapping and prefix configuration" \
    "    ldap_group_mapping_str = os.environ.get(\"LDAP_GROUP_MAPPING\", \"{}\")" \
    "    try:" \
    "        group_mapping = json.loads(ldap_group_mapping_str)" \
    "    except Exception:" \
    "        group_mapping = {}" \
    "" \
    "    tenant_group_prefix = os.environ.get(\"LDAP_TENANT_GROUP_PREFIX\", \"\")" \
    "" \
    "    # Identify Target Tenants and Roles" \
    "    # Map: tenant_cn -> { \"role\": Enum, \"group_dn\": str }" \
    "    tenant_targets = {}" \
    "" \
    "    # Build case-insensitive lookup for group mapping" \
    "    ci_mapping = {k.lower(): v for k, v in group_mapping.items()}" \
    "    ci_prefix = tenant_group_prefix.lower()" \
    "" \
    "    for group_dn in groups:" \
    "        is_match = False" \
    "        target_role = UserTenantRole.NORMAL" \
    "        " \
    "        # Check mapping" \
    "        role_str = None" \
    "        if group_dn.lower() in ci_mapping:" \
    "            role_str = ci_mapping[group_dn.lower()]" \
    "            is_match = True" \
    "        " \
    "        cn = group_dn.split(',')[0]" \
    "        if cn.upper().startswith(\"CN=\"):" \
    "            cn = cn[3:]" \
    "            " \
    "        if not role_str and cn.lower() in ci_mapping:" \
    "            role_str = ci_mapping[cn.lower()]" \
    "            is_match = True" \
    "            " \
    "        if not is_match and ci_prefix and cn.lower().startswith(ci_prefix):" \
    "            is_match = True" \
    "            " \
    "        if is_match:" \
    "            if role_str:" \
    "                if role_str.lower() in ('superuser', 'admin'):" \
    "                    target_role = UserTenantRole.ADMIN" \
    "            " \
    "            # Update target" \
    "            if cn not in tenant_targets:" \
    "                tenant_targets[cn] = { \"role\": target_role, \"group_dn\": group_dn }" \
    "            else:" \
    "                # Upgrade role if needed" \
    "                if tenant_targets[cn][\"role\"] == UserTenantRole.NORMAL and target_role == UserTenantRole.ADMIN:" \
    "                    tenant_targets[cn][\"role\"] = UserTenantRole.ADMIN" \
    "" \
    "    if not tenant_targets:" \
    "        logging.info(f\"User {user.email} belongs to {len(groups)} LDAP groups, but none matched the tenant filter.\")" \
    "    else:" \
    "        logging.info(f\"Syncing user {user.email} to {len(tenant_targets)} matched group tenants.\")" \
    "" \
    "    synced_tenant_ids = set()" \
    "" \
    "    for cn, info in tenant_targets.items():" \
    "        group_dn = info[\"group_dn\"]" \
    "        target_role = info[\"role\"]" \
    "        " \
    "        tenant = get_or_create_group_tenant(group_dn)" \
    "        if not tenant:" \
    "            continue" \
    "" \
    "        synced_tenant_ids.add(tenant.id)" \
    "" \
    "        try:" \
    "            ut = UserTenantService.filter_by_tenant_and_user_id(tenant.id, user.id)" \
    "            if not ut:" \
    "                logging.info(f\"Adding user {user.email} to tenant {tenant.name} with role {target_role}\")" \
    "                UserTenantService.save(" \
    "                    id=get_uuid()," \
    "                    user_id=user.id," \
    "                    tenant_id=tenant.id," \
    "                    role=target_role," \
    "                    invited_by=user.id," \
    "                    status=\"1\"" \
    "                )" \
    "            else:" \
    "                if ut.role != UserTenantRole.OWNER and ut.role != target_role:" \
    "                    logging.info(f\"Updating user {user.email} role in tenant {tenant.name} to {target_role}\")" \
    "                    ut.role = target_role" \
    "                    ut.save()" \
    "        except Exception as e:" \
    "            logging.error(f\"Error syncing user {user.email} to tenant {tenant.name}: {e}\")" \
    "" \
    "    # Remove user from LDAP group tenants they no longer belong to." \
    "    # LDAP group tenants are identified by their owner email starting with 'ldap_group_'." \
    "    try:" \
    "        stale = list(" \
    "            UserTenant.select()" \
    "            .join(User, on=(User.id == UserTenant.tenant_id))" \
    "            .where(" \
    "                (UserTenant.user_id == user.id)" \
    "                & (UserTenant.role != UserTenantRole.OWNER)" \
    "                & (UserTenant.status == StatusEnum.VALID.value)" \
    "                & (User.email.startswith('ldap_group_'))" \
    "            )" \
    "        )" \
    "        for ut in stale:" \
    "            if ut.tenant_id not in synced_tenant_ids:" \
    "                logging.info(f\"Removing user {user.email} from stale LDAP group tenant {ut.tenant_id}\")" \
    "                ut.delete_instance()" \
    "    except Exception as e:" \
    "        logging.error(f\"Error cleaning stale LDAP group memberships for {user.email}: {e}\")" \
    "" \
    "def cleanup_orphaned_group_tenants():" \
    "    \"\"\"Remove LDAP group tenants whose groups no longer appear in config." \
    "" \
    "    Uses LDAP_GROUP_MAPPING and LDAP_TENANT_GROUP_PREFIX as the source of truth." \
    "    Must only be called after a successful LDAP bind so we know the directory is" \
    "    reachable — avoids accidental cleanup caused by network/credential failures." \
    "    \"\"\"" \
    "    ldap_group_mapping_str = os.environ.get('LDAP_GROUP_MAPPING', '{}')" \
    "    try:" \
    "        group_mapping = json.loads(ldap_group_mapping_str)" \
    "    except Exception:" \
    "        return" \
    "" \
    "    tenant_group_prefix = os.environ.get('LDAP_TENANT_GROUP_PREFIX', '')" \
    "" \
    "    # Build set of valid group CNs from mapping keys (lowercased for case-insensitive match)" \
    "    valid_cns = set()" \
    "    for key in group_mapping:" \
    "        cn = key.split(',')[0]" \
    "        if cn.upper().startswith('CN='):" \
    "            cn = cn[3:]" \
    "        valid_cns.add(cn.lower())" \
    "" \
    "    ci_prefix = tenant_group_prefix.lower()" \
    "" \
    "    # Find all ldap_group_ dummy owner users" \
    "    try:" \
    "        dummy_users = list(" \
    "            User.select().where(User.email.startswith('ldap_group_'))" \
    "        )" \
    "    except Exception as e:" \
    "        logging.error(f'Error querying ldap_group_ users for orphan cleanup: {e}')" \
    "        return" \
    "" \
    "    for dummy_user in dummy_users:" \
    "        original_cn = dummy_user.nickname" \
    "" \
    "        # Check if still valid in current config (case-insensitive)" \
    "        is_valid = original_cn.lower() in valid_cns" \
    "        if not is_valid and ci_prefix and original_cn.lower().startswith(ci_prefix):" \
    "            is_valid = True" \
    "" \
    "        if is_valid:" \
    "            continue" \
    "" \
    "        # Use upstream delete_user_data() so new features (agents, MCP servers, etc.)" \
    "        # are automatically cleaned up without maintaining a manual list." \
    "        logging.info(f'Removing orphaned LDAP group tenant: {dummy_user.email} (group={original_cn})')" \
    "        try:" \
    "            # delete_user_data requires: is_active=INACTIVE, is_superuser=False" \
    "            from common.constants import ActiveEnum" \
    "            if dummy_user.is_active == ActiveEnum.ACTIVE.value:" \
    "                UserService.update_by_id(dummy_user.id, {'is_active': ActiveEnum.INACTIVE.value})" \
    "            if dummy_user.is_superuser:" \
    "                UserService.update_by_id(dummy_user.id, {'is_superuser': False})" \
    "" \
    "            from api.db.joint_services.user_account_service import delete_user_data" \
    "            result = delete_user_data(dummy_user.id)" \
    "            if result.get('success'):" \
    "                logging.info(f'Successfully removed orphaned group tenant {original_cn} ({dummy_user.email})')" \
    "            else:" \
    "                logging.error(f'delete_user_data failed for {dummy_user.email}: {result.get(\"message\")}')" \
    "        except Exception as e:" \
    "            logging.error(f'Error removing orphaned group tenant {dummy_user.email}: {e}')" \
    "" \
    "def patched_query_user(cls, email, password):" \
    "    ldap_info = ldap_login(email, password)" \
    "    if isinstance(ldap_info, dict):" \
    "        logging.info(f\"LDAP Login success for {email}\")" \
    "        users = cls.query_user_by_email(email)" \
    "        user = None" \
    "        if not users:" \
    "            logging.info(f\"Creating new user from LDAP: {email}\")" \
    "            import sys" \
    "            user_register = sys.modules['api.apps.user'].user_register" \
    "            user_id = get_uuid()" \
    "            user_dict = {" \
    "                \"email\": email," \
    "                \"nickname\": ldap_info[\"nickname\"]," \
    "                \"password\": password," \
    "                \"is_superuser\": ldap_info[\"is_superuser\"]," \
    "                \"login_channel\": \"ldap\"," \
    "            }" \
    "            try:" \
    "                result = user_register(user_id, user_dict)" \
    "                if not result:" \
    "                    logging.error(f\"user_register failed for LDAP user {email}\")" \
    "                    return None" \
    "                user = cls.query_user_by_email(email)[0]" \
    "            except Exception as e:" \
    "                logging.error(f\"Error registering LDAP user {email}: {e}\")" \
    "                return None" \
    "        else:" \
    "            user = users[0]" \
    "            updates = {}" \
    "            if user.is_superuser != ldap_info[\"is_superuser\"]:" \
    "                updates[\"is_superuser\"] = ldap_info[\"is_superuser\"]" \
    "                logging.info(f\"Updating superuser status for {email} to {ldap_info['is_superuser']}\")" \
    "            if user.nickname != ldap_info[\"nickname\"]:" \
    "                updates[\"nickname\"] = ldap_info[\"nickname\"]" \
    "                logging.info(f\"Updating nickname for {email} to {ldap_info['nickname']}\")" \
    "            if getattr(user, 'login_channel', '') != 'ldap':" \
    "                updates[\"login_channel\"] = \"ldap\"" \
    "            if updates:" \
    "                UserService.update_user(user.id, updates)" \
    "                for k, v in updates.items():" \
    "                    setattr(user, k, v)" \
    "" \
    "            # One-way password sync: keep local DB hash in sync with LDAP." \
    "            # After a successful LDAP bind the password arg holds the current" \
    "            # LDAP password (base64-encoded by decrypt()).  We call the *original*" \
    "            # update_user_password (not the patched one, which blocks LDAP users)" \
    "            # so the local hash always matches the directory." \
    "            try:" \
    "                from werkzeug.security import check_password_hash as _chk" \
    "                if not _chk(user.password, str(password)):" \
    "                    logging.info(f\"Syncing LDAP password to local DB for {email}\")" \
    "                    original_credential_updater(user.id, password)" \
    "            except Exception as e:" \
    "                logging.warning(f\"Failed to sync LDAP password for {email}: {e}\")" \
    "" \
    "        if \"groups\" in ldap_info:" \
    "            sync_user_to_group_tenants(user, ldap_info[\"groups\"])" \
    "        # Clean up group tenants for groups removed from LDAP_GROUP_MAPPING." \
    "        # Safe to call here — we just completed a successful LDAP bind." \
    "        try:" \
    "            cleanup_orphaned_group_tenants()" \
    "        except Exception as e:" \
    "            logging.error(f'Error during orphaned group tenant cleanup: {e}')" \
    "        return user" \
    "" \
    "    # LDAP login returned a failure reason." \
    "    # Only revoke API tokens when the user is definitively denied access" \
    "    # (removed from directory or not in any mapped group).  Do NOT revoke" \
    "    # on a simple wrong-password attempt — that would punish typos." \
    "    if ldap_info in ('user_not_found', 'not_in_group'):" \
    "        try:" \
    "            existing = cls.query_user_by_email(email)" \
    "            if existing and getattr(existing[0], 'login_channel', '') == 'ldap':" \
    "                user_id = existing[0].id" \
    "                deleted = APITokenService.delete_by_tenant_id(user_id)" \
    "                if deleted:" \
    "                    logging.info(" \
    "                        f\"Revoked {deleted} API token(s) for LDAP user {email} \"" \
    "                        f\"(reason: {ldap_info}).\"" \
    "                    )" \
    "        except Exception as e:" \
    "            logging.error(f\"Error revoking API tokens for {email}: {e}\")" \
    "" \
    "    # Fallback to local — but NOT for LDAP-managed users." \
    "    # If an LDAP user's password changed in the directory, ldap_login() correctly" \
    "    # rejects the old password.  Without this guard the old password (still hashed" \
    "    # in the local DB) would pass original_query_user, creating a stale-password" \
    "    # backdoor." \
    "    try:" \
    "        existing = cls.query_user_by_email(email)" \
    "        if existing and getattr(existing[0], 'login_channel', '') == 'ldap':" \
    "            logging.warning(f\"LDAP authentication failed for {email}; local fallback blocked for LDAP-managed user.\")" \
    "            return None" \
    "    except Exception:" \
    "        pass" \
    "    return original_query_user(email, password)" \
    "" \
    "def patched_get_joined_tenants_by_user_id(cls, user_id):" \
    "    \"\"\"Widen upstream NORMAL-only query to include ADMIN memberships." \
    "" \
    "    Dynamically reads all Tenant model fields via peewee metadata" \
    "    so upstream column additions are automatically picked up." \
    "    \"\"\"" \
    "    from api.db.db_models import DB" \
    "    try:" \
    "        with DB.connection_context():" \
    "            fields = []" \
    "            for f in cls.model._meta.sorted_fields:" \
    "                if f.name == 'id':" \
    "                    fields.append(f.alias('tenant_id'))" \
    "                else:" \
    "                    fields.append(f)" \
    "            fields.append(UserTenant.role)" \
    "            return list(" \
    "                cls.model.select(*fields)" \
    "                .join(UserTenant, on=(" \
    "                    (cls.model.id == UserTenant.tenant_id)" \
    "                    & (UserTenant.user_id == user_id)" \
    "                    & (UserTenant.status == StatusEnum.VALID.value)" \
    "                    & (UserTenant.role << [UserTenantRole.NORMAL, UserTenantRole.ADMIN])" \
    "                ))" \
    "                .where(cls.model.status == StatusEnum.VALID.value)" \
    "                .dicts())" \
    "    except Exception:" \
    "        return original_get_joined_tenants(user_id) if original_get_joined_tenants else []" \
    "" \
    "# --- Patch UserService.query so first-time LDAP users pass the" \
    "#     'is email registered?' gate in the login endpoint (user_app.py" \
    "#     line ~102).  Without this, the endpoint returns 'Email ... is not" \
    "#     registered!' before query_user (where LDAP auto-provisioning" \
    "#     lives) is ever called." \
    "original_query = UserService.query" \
    "" \
    "def patched_query(cls, *args, **kwargs):" \
    "    \"\"\"Wrapper around UserService.query." \
    "" \
    "    When LDAP is enabled and an email-only query returns no rows," \
    "    return a lightweight placeholder so the login flow proceeds to" \
    "    query_user() where the real LDAP bind + auto-registration happens." \
    "    \"\"\"" \
    "    result = original_query(*args, **kwargs)" \
    "    if result:" \
    "        return result" \
    "    # Only intercept email-only lookups (the login-gate pattern)" \
    "    if LDAP_ENABLED and kwargs.get('email') and len(kwargs) == 1 and not args:" \
    "        from api.db.db_models import User" \
    "        placeholder = User()" \
    "        placeholder.email = kwargs['email']" \
    "        placeholder.id = 'ldap_placeholder'" \
    "        return [placeholder]" \
    "    return result" \
    "" \
    "# --- Patch UserService.update_user_password to block password resets for LDAP users." \
    "#     The /forget/reset-password endpoint and admin CLI both call" \
    "#     update_user_password directly (bypassing update_by_id)." \
    "original_credential_updater = getattr(UserService, \"update_user_password\")" \
    "" \
    "def patched_update_user_password(cls, user_id, new_password):" \
    "    \"\"\"Block password changes for LDAP users.\"\"\"" \
    "    try:" \
    "        user = User.get_or_none(User.id == user_id)" \
    "        if user and getattr(user, 'login_channel', '') == 'ldap':" \
    "            logging.info(f\"Blocked password change for LDAP user {user_id}.\")" \
    "            raise ValueError('Password changes are disabled for LDAP-managed users.')" \
    "    except ValueError:" \
    "        raise" \
    "    except Exception:" \
    "        pass" \
    "    return original_credential_updater(user_id, new_password)" \
    "" \
    "# --- Patch UserService.update_by_id to block profile edits for LDAP users." \
    "#     The /setting endpoint calls update_by_id; LDAP sync uses update_user" \
    "#     (a different method), so this patch only affects UI-initiated edits." \
    "original_update_by_id = UserService.update_by_id" \
    "" \
    "def patched_user_update_by_id(cls, pid, data):" \
    "    \"\"\"Block profile edits for LDAP users.\"\"\"" \
    "    try:" \
    "        user = User.get_or_none(User.id == pid)" \
    "        if user and getattr(user, 'login_channel', '') == 'ldap':" \
    "            ldap_managed = {'nickname', 'password'}" \
    "            blocked = ldap_managed & set(data.keys())" \
    "            if blocked:" \
    "                logging.info(f\"Blocked UI update of LDAP-managed fields {blocked} for user {pid}\")" \
    "                raise ValueError('Profile editing is disabled for LDAP-managed users.')" \
    "    except ValueError:" \
    "        raise" \
    "    except Exception:" \
    "        pass" \
    "    return original_update_by_id(pid, data)" \
    "" \
    "# Apply Patches" \
    "if LDAP_ENABLED and original_query_user:" \
    "    UserService.query_user = classmethod(patched_query_user)" \
    "    logging.info(\"LDAP Auth Patch Applied to UserService.query_user\")" \
    "" \
    "    UserService.query = classmethod(patched_query)" \
    "    logging.info(\"LDAP Auth Patch Applied to UserService.query (login gate bypass)\")" \
    "" \
    "    UserService.update_by_id = classmethod(patched_user_update_by_id)" \
    "    logging.info(\"LDAP Auth Patch Applied to UserService.update_by_id (profile edit protection)\")" \
    "" \
    "    UserService.update_user_password = classmethod(patched_update_user_password)" \
    "    logging.info(\"LDAP Auth Patch Applied to UserService.update_user_password (password reset protection)\")" \
    "" \
    "    if original_get_joined_tenants is not None:" \
    "        TenantService.get_joined_tenants_by_user_id = classmethod(patched_get_joined_tenants_by_user_id)" \
    "        logging.info(\"TenantService.get_joined_tenants_by_user_id Patched for LDAP Admin visibility\")" \
    "" \
    > /ragflow/api/ldap_auth.py

# 1. Install Gunicorn + OTel instrumentation + OTLP exporter
#    - gunicorn               → production WSGI server for the Admin server
#    - opentelemetry-distro   → provides the 'opentelemetry-instrument' CLI
#    - opentelemetry-exporter-otlp → sends traces/metrics/logs to your collector
#    - opentelemetry-instrumentation-asgi    → ASGI middleware for inbound HTTP spans
#    - opentelemetry-instrumentation-flask   → auto-instruments Flask (Admin server)
#    - opentelemetry-instrumentation-requests → auto-instruments outbound HTTP
#    - opentelemetry-instrumentation-urllib3 → auto-instruments urllib3 clients
#      (including MinIO SDK traffic)
#    - opentelemetry-instrumentation-redis   → auto-instruments Redis calls
#
#    IMPORTANT: opentelemetry-distro alone provides only the CLI tool and SDK
#    configuration — it includes ZERO instrumentor libraries. Without the
#    -instrumentation-* packages below, opentelemetry-instrument discovers
#    nothing and generates no spans.
#
#    Uvicorn is already in the base image (MCP server dependency).
#    Uses 'uv' because it's pre-installed in the base image (pip may not be).
RUN uv pip install --no-config --default-index "${UV_PIP_INDEX_URL}" --index-strategy first-index --no-cache \
      uvicorn \
      gunicorn \
      opentelemetry-distro \
      opentelemetry-exporter-otlp \
      opentelemetry-instrumentation-asgi \
      opentelemetry-instrumentation-flask \
      opentelemetry-instrumentation-requests \
      opentelemetry-instrumentation-redis \
      opentelemetry-instrumentation-elasticsearch \
      opentelemetry-instrumentation-psycopg2 \
      opentelemetry-instrumentation-urllib3 \
      opentelemetry-instrumentation-peewee \
      ldap3

# Ensure shim loaders remain present after pip installs.
# opentelemetry-instrument injects its own sitecustomize; chain our shim there too.
RUN python3 - <<'PY'
import importlib.util
import site
from pathlib import Path

site_pkg = Path(site.getsitepackages()[0])
shim_module = site_pkg / "ragflow_lang_shim.py"
if not shim_module.exists():
    raise RuntimeError(f"Missing shim module: {shim_module}")

sitecustomize = site_pkg / "sitecustomize.py"
marker = "import ragflow_lang_shim  # ragflow-lang-shim"
content = sitecustomize.read_text(encoding="utf-8", errors="ignore") if sitecustomize.exists() else ""
if marker not in content:
    if content and not content.endswith("\n"):
        content += "\n"
    sitecustomize.write_text(content + marker + "\n", encoding="utf-8")
    print(f"Ensured ragflow language shim loader in: {sitecustomize}")

spec = importlib.util.find_spec("opentelemetry.instrumentation.auto_instrumentation.sitecustomize")
if not spec or not spec.origin:
    print("OTel sitecustomize not found; skipping ragflow language shim chaining")
else:
    target = Path(spec.origin)
    content = target.read_text(encoding="utf-8", errors="ignore")
    if marker not in content:
        if content and not content.endswith("\n"):
            content += "\n"
        target.write_text(content + marker + "\n", encoding="utf-8")
        print(f"Chained ragflow language shim in: {target}")
    else:
        print(f"Ragflow language shim already chained in: {target}")
PY

# Optional: install Docling using the same default version pinned in /ragflow/entrypoint.sh.
# Build examples:
#   --build-arg INSTALL_DOCLING=1   (install docling)
#   --build-arg INSTALL_DOCLING=0   (default, skip docling install)
RUN case "${INSTALL_DOCLING}" in \
      1|true|TRUE|yes|YES|on|ON) \
        DOCLING_PIN="$(sed -n 's/.*DOCLING_VERSION:-==\([0-9][0-9.]*\).*/\1/p' /ragflow/entrypoint.sh | head -n1)" && \
        test -n "${DOCLING_PIN}" && \
        uv pip install --no-config --default-index "${UV_PIP_INDEX_URL}" --index-strategy first-index --no-cache "docling==${DOCLING_PIN}" ;; \
      *) \
        echo "Skipping docling install (INSTALL_DOCLING=${INSTALL_DOCLING})" ;; \
    esac

#
#    (Optional) Add more instrumentations if needed, e.g.:
#      opentelemetry-instrumentation-grpc \
#      opentelemetry-instrumentation-peewee \
#      opentelemetry-instrumentation-asyncio

# 2. Auto-discover instrumentations (optional — disabled by default)
#    opentelemetry-bootstrap -a install uses pip internally and installs
#    each discovered instrumentation one-at-a-time without full dependency
#    resolution, which causes version conflicts with packages already in
#    the base image (e.g. opentelemetry-api 1.36.0 vs 1.39.1).
#    The explicit installs above (via uv) are sufficient — uv resolves
#    the full dependency graph correctly in a single pass.
#    Uncomment the next line only if you need additional auto-discovered
#    instrumentations AND have resolved any version conflicts:
# RUN uv run opentelemetry-bootstrap -a install

# 3. Add a tiny launcher wrapper so OTEL can be toggled globally/per-service.
#    This wrapper:
#    - bypasses opentelemetry-instrument if RAGFLOW_OTEL_ENABLED=false
#    - maps service flags to OTEL_PYTHON_DISABLED_INSTRUMENTATIONS
RUN cat > /usr/local/bin/ragflow-otel-python <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

to_lower() {
    echo "${1:-}" | tr '[:upper:]' '[:lower:]'
}

is_false() {
    local v
    v="$(to_lower "${1:-}")"
    [[ "${v}" == "0" || "${v}" == "false" || "${v}" == "no" || "${v}" == "off" ]]
}

is_true() {
    local v
    v="$(to_lower "${1:-}")"
    [[ "${v}" == "1" || "${v}" == "true" || "${v}" == "yes" || "${v}" == "on" ]]
}

should_disable_instrumentation() {
    local enable_var="$1"
    local disable_var="$2"

    if [[ -n "${!disable_var:-}" ]] && is_true "${!disable_var}"; then
        return 0
    fi

    if [[ -n "${!enable_var:-}" ]] && is_false "${!enable_var}"; then
        return 0
    fi

    return 1
}

append_disabled() {
    local name="$1"
    if [[ -z "${disabled_instrumentations}" ]]; then
        disabled_instrumentations="${name}"
    else
        disabled_instrumentations="${disabled_instrumentations},${name}"
    fi
}

if is_false "${RAGFLOW_OTEL_ENABLED:-true}"; then
    exec python3 "$@"
fi

disabled_instrumentations="${OTEL_PYTHON_DISABLED_INSTRUMENTATIONS:-}"

if should_disable_instrumentation "RAGFLOW_OTEL_INSTRUMENT_POSTGRES" "RAGFLOW_OTEL_DISABLE_POSTGRES"; then append_disabled "psycopg2"; fi
if should_disable_instrumentation "RAGFLOW_OTEL_INSTRUMENT_REDIS" "RAGFLOW_OTEL_DISABLE_REDIS"; then append_disabled "redis"; fi
if should_disable_instrumentation "RAGFLOW_OTEL_INSTRUMENT_ELASTICSEARCH" "RAGFLOW_OTEL_DISABLE_ELASTICSEARCH"; then append_disabled "elasticsearch"; fi
if should_disable_instrumentation "RAGFLOW_OTEL_INSTRUMENT_ORM" "RAGFLOW_OTEL_DISABLE_ORM"; then append_disabled "peewee"; fi
if should_disable_instrumentation "RAGFLOW_OTEL_INSTRUMENT_HTTP" "RAGFLOW_OTEL_DISABLE_HTTP"; then
    append_disabled "requests"
    append_disabled "urllib3"
fi
if should_disable_instrumentation "RAGFLOW_OTEL_INSTRUMENT_FLASK" "RAGFLOW_OTEL_DISABLE_FLASK"; then append_disabled "flask"; fi

# Normalize ASGI toggle semantics for ragflow_server.py middleware guard.
if should_disable_instrumentation "RAGFLOW_OTEL_INSTRUMENT_ASGI" "RAGFLOW_OTEL_DISABLE_ASGI"; then
    export RAGFLOW_OTEL_INSTRUMENT_ASGI="false"
    export RAGFLOW_OTEL_DISABLE_ASGI="true"
else
    export RAGFLOW_OTEL_INSTRUMENT_ASGI="${RAGFLOW_OTEL_INSTRUMENT_ASGI:-true}"
    export RAGFLOW_OTEL_DISABLE_ASGI="false"
fi

if [[ -n "${disabled_instrumentations}" ]]; then
    export OTEL_PYTHON_DISABLED_INSTRUMENTATIONS="${disabled_instrumentations// /}"
fi

exec opentelemetry-instrument python3 "$@"
EOF
RUN sed -i 's/\r$//' /usr/local/bin/ragflow-otel-python && chmod +x /usr/local/bin/ragflow-otel-python

# 4. Patch entrypoint.sh — run every Python process through /usr/local/bin/ragflow-otel-python.
#    "$PY" is defined as python3 in entrypoint.sh.
#    The pattern skips lines containing '-c "' (e.g. the docling import check)
#    to avoid unnecessary OTEL startup on one-shot commands.
RUN sed -i '/-c "/!s/"$PY"/\/usr\/local\/bin\/ragflow-otel-python/g' /ragflow/entrypoint.sh

# 5. Patch ragflow_server.py — replace Quart's dev-mode app.run() with Uvicorn
#    + wrap the app in OpenTelemetryMiddleware for inbound request tracing.
#    Quart has no auto-instrumentor, so manual ASGI middleware is required
#    to generate server spans for incoming HTTP requests.
#    Uvicorn is already installed (MCP server dependency).
#
#    a) Inject LDAP auth patch at the very beginning
#    b) Add "import uvicorn" and ASGI middleware import after "import os"
#    c) Replace app.run() with uvicorn.run(OpenTelemetryMiddleware(app))
RUN sed -i '1i import api.ldap_auth' /ragflow/api/ragflow_server.py && \
    sed -i '/^import os$/a import uvicorn\nfrom opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware' /ragflow/api/ragflow_server.py && \
    sed -i 's/app\.run(host=settings\.HOST_IP, port=settings\.HOST_PORT)/uvicorn.run(OpenTelemetryMiddleware(app) if str(os.environ.get("RAGFLOW_OTEL_ENABLED", "true")).lower() in ("1", "true", "yes", "on") and not (str(os.environ.get("RAGFLOW_OTEL_DISABLE_ASGI", "false")).lower() in ("1", "true", "yes", "on") or str(os.environ.get("RAGFLOW_OTEL_INSTRUMENT_ASGI", "true")).lower() in ("0", "false", "no", "off")) else app, host=settings.HOST_IP, port=int(settings.HOST_PORT), access_log=True)/' /ragflow/api/ragflow_server.py

# 6. Patch admin_server.py — replace werkzeug's dev server with Gunicorn
#    Gunicorn is the standard production WSGI server for Flask apps.
#    This replaces run_simple() with a programmatic Gunicorn application.
#
#    a) Create a small helper module with a Gunicorn app class
#    b) Add import for the helper after "from werkzeug.serving import run_simple"
#    c) Replace run_simple(...) with Gunicorn launch
RUN printf '%s\n' \
    'import gunicorn.app.base' \
    '' \
    '' \
    'class GunicornApp(gunicorn.app.base.BaseApplication):' \
    '    """Minimal programmatic Gunicorn application wrapper."""' \
    '' \
    '    def __init__(self, app, options=None):' \
    '        self.options = options or {}' \
    '        self.application = app' \
    '        super().__init__()' \
    '' \
    '    def load_config(self):' \
    '        for key, value in self.options.items():' \
    '            if key in self.cfg.settings:' \
    '                self.cfg.set(key, value)' \
    '' \
    '    def load(self):' \
    '        return self.application' \
    > /ragflow/admin/server/_gunicorn_app.py

RUN sed -i '/^from werkzeug.serving import run_simple$/a from _gunicorn_app import GunicornApp' /ragflow/admin/server/admin_server.py && \
    sed -i '/run_simple(/,/)/c\        GunicornApp(app, {"bind": "0.0.0.0:9381", "workers": int(os.environ.get("GUNICORN_WORKERS", "2")), "accesslog": "-", "timeout": 120}).run()' /ragflow/admin/server/admin_server.py

# 7. Merge ELASTIC_INDEX_SETTINGS env var into conf/mapping.json at startup
#    Creates a small Python helper script and injects a call into entrypoint.sh
#    right before services start.  The script deep-merges the JSON value of
#    ELASTIC_INDEX_SETTINGS into the "settings" key of mapping.json.
#    If the env var is unset or empty the file is left untouched.
RUN mkdir -p /ragflow/scripts && \
    printf '%s\n' \
    'import json, os, sys, copy' \
    '' \
    'def deep_merge(base, override):' \
    '    """Recursively merge override into base (mutates base)."""' \
    '    for key, value in override.items():' \
    '        if key in base and isinstance(base[key], dict) and isinstance(value, dict):' \
    '            deep_merge(base[key], value)' \
    '        else:' \
    '            base[key] = value' \
    '    return base' \
    '' \
    'def main():' \
    '    settings_json = os.environ.get("ELASTIC_INDEX_SETTINGS", "").strip()' \
    '    if not settings_json:' \
    '        return' \
    '    mapping_path = os.path.join(os.path.dirname(__file__), "..", "conf", "mapping.json")' \
    '    mapping_path = os.path.normpath(mapping_path)' \
    '    try:' \
    '        override = json.loads(settings_json)' \
    '    except json.JSONDecodeError as e:' \
    '        print(f"ERROR: ELASTIC_INDEX_SETTINGS is not valid JSON: {e}", file=sys.stderr)' \
    '        return' \
    '    if not isinstance(override, dict):' \
    '        print("ERROR: ELASTIC_INDEX_SETTINGS must be a JSON object", file=sys.stderr)' \
    '        return' \
    '    with open(mapping_path, "r") as f:' \
    '        mapping = json.load(f)' \
    '    original = copy.deepcopy(mapping.get("settings", {}))' \
    '    mapping["settings"] = deep_merge(mapping.get("settings", {}), override)' \
    '    with open(mapping_path, "w") as f:' \
    '        json.dump(mapping, f, indent=2)' \
    '    merged = mapping.get("settings", {})' \
    '    print(f"ELASTIC_INDEX_SETTINGS merged into {mapping_path}")' \
    '    print(f"  before: {json.dumps(original)}")' \
    '    print(f"  after:  {json.dumps(merged)}")' \
    '' \
    'if __name__ == "__main__":' \
    '    main()' \
    > /ragflow/scripts/merge_es_settings.py

# Inject the merge call as the first command in entrypoint.sh (right after
# "set -e") so mapping.json is always patched before anything else runs.
# This is immune to upstream renames/refactors of functions or comments.
RUN sed -i '/^set -e$/a python3 /ragflow/scripts/merge_es_settings.py' /ragflow/entrypoint.sh

# 8. Patch MinIO client to respect 'secure' configuration instead of hardcoding secure=False
#    Also handles self-signed certificates via MINIO_CERT_CHECK env var (set to "false" to skip verification)
RUN sed -i 's/secure=False/secure=str(settings.MINIO.get("secure", False)).lower() == "true"/' /ragflow/rag/utils/minio_conn.py

# 9. Patch MinIO health check to use https when secure is enabled
#    a) Replace hardcoded 'http://' with a dynamic scheme based on MINIO secure setting
#    b) Add verify= parameter to handle self-signed certificates
#    NOTE: The sed patterns include 'MINIO' context to avoid modifying other
#    health check URLs in the same file (e.g. check_ragflow_server_alive).
RUN sed -i "s|f'http://{settings\.MINIO|f'{\"https\" if str(settings.MINIO.get(\"secure\", False)).lower() == \"true\" else \"http\"}://{settings.MINIO|" /ragflow/api/utils/health_utils.py && \
    sed -i "s|/minio/health/live')|/minio/health/live', verify=not (str(settings.MINIO.get(\"secure\", False)).lower() == \"true\"))|" /ragflow/api/utils/health_utils.py

# 10. Patch db_models.py — suppress PostgreSQL "column already exists" migration errors
#    The alter_db_add_column function only handles MySQL duplicate column errors
#    (error code 1060 / message 'Duplicate column name'). PostgreSQL raises a
#    different exception type with "already exists" message, which falls through
#    to the generic Exception handler and logs as CRITICAL on every restart.
#    This patch adds a guard to skip the CRITICAL log when the error is just a
#    harmless "column already exists" during migration.
RUN sed -i '/except Exception as ex:/{n;/Failed to add/s/        logging.critical/        if "already exists" not in str(ex): logging.critical/}' /ragflow/api/db/db_models.py

# 11. Make crawl4ai optional for agent startup by lazy-importing AsyncWebCrawler.
RUN python3 - <<'PY'
from pathlib import Path

path = Path("/ragflow/agent/tools/crawler.py")
text = path.read_text()

text = text.replace("from crawl4ai import AsyncWebCrawler\n", "")

needle = (
    '    async def get_web(self, url):\n'
    '        if self.check_if_canceled("Crawler async operation"):\n'
    '            return\n'
    '\n'
)
insert = (
    needle
    + "        try:\n"
    + "            from crawl4ai import AsyncWebCrawler\n"
    + "        except ImportError:\n"
    + "            return \"Crawler tool is unavailable because optional package 'crawl4ai' is not installed.\"\n"
    + "\n"
)

if needle in text:
    text = text.replace(needle, insert, 1)

path.write_text(text)
PY

## REDIS (VALKEY) OTEL DOES NOT WORK
## 12. Patch redis_conn.py — instrument Valkey (Redis)
##    RAGFlow uses 'valkey' as the Redis client, but imports it as 'redis'.
##    Standard OTel Redis instrumentation targets the 'redis' library.
##    We manually trigger the RedisInstrumentor on the 'valkey' module.
#RUN sed -i '/import valkey as redis/a try:\n    import sys\n    sys.modules["redis"] = redis\n    from opentelemetry.instrumentation.redis import RedisInstrumentor\n    print("OTEL: Attempting to instrument Valkey (Redis)...")\n    RedisInstrumentor().instrument(redis_module=redis)\n    print("OTEL: Valkey instrumented successfully")\nexcept Exception as e:\n    print(f"OTEL: Valkey instrumentation failed: {e}")' /ragflow/rag/utils/redis_conn.py

# 13. Security and compliance remediation bundle (single maintenance section):
#    Keep this block updated as new Prisma findings arrive.
#    - OS/OpenSSL refresh (including legacy libssl1.1 removal)
#    - Node.js runtime refresh from NODEJS_ORG_MIRROR (fixes Node engine CVEs)
#    - Keep bundled Apache Tika 3.2.3 + replace embedded jackson-core for GHSA fixes
#    - Targeted Python package minimum versions for known CVEs
# 13.1 OS/OpenSSL refresh (including legacy libssl1.1 removal)
# 13.2 Node.js runtime refresh from NODEJS_ORG_MIRROR (implemented in the same RUN below)
RUN set -eux; \
    export DEBIAN_FRONTEND=noninteractive; \
    APT_MIRROR_HOST="$(echo "${APT_MIRROR_URL}" | sed -E 's#https?://([^/]+)/?.*#\1#')"; \
    APT_EXTRA_OPTS="-o Acquire::https::${APT_MIRROR_HOST}::Verify-Peer=false -o Acquire::Check-Valid-Until=false"; \
    if [ -f /etc/apt/sources.list ]; then \
      sed -i "s#https\\{0,1\\}://\\(us\\.\\)\\{0,1\\}security.ubuntu.com/ubuntu/\\{0,1\\}#${APT_MIRROR_URL}#g" /etc/apt/sources.list; \
      sed -i "s#https\\{0,1\\}://\\(us\\.\\)\\{0,1\\}archive.ubuntu.com/ubuntu/\\{0,1\\}#${APT_MIRROR_URL}#g" /etc/apt/sources.list; \
    fi; \
    if [ -f /etc/apt/sources.list.d/ubuntu.sources ]; then \
      sed -i -E "s#^URIs: .*#URIs: ${APT_MIRROR_URL}#g" /etc/apt/sources.list.d/ubuntu.sources; \
    fi; \
    apt-get update ${APT_EXTRA_OPTS}; \
    apt-get install -y ${APT_EXTRA_OPTS} --no-install-recommends ca-certificates; \
    apt-get -y ${APT_EXTRA_OPTS} -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" upgrade; \
    NEED_PKGS=""; \
    command -v curl >/dev/null 2>&1 || NEED_PKGS="${NEED_PKGS} curl"; \
    if [ -n "${NEED_PKGS}" ]; then apt-get install -y ${APT_EXTRA_OPTS} --no-install-recommends ${NEED_PKGS}; fi; \
    apt-get install -y ${APT_EXTRA_OPTS} --no-install-recommends openssl libcap2-bin xz-utils; \
    if dpkg-query -W -f='${Status}' nodejs 2>/dev/null | grep -q "ok installed"; then apt-get purge -y ${APT_EXTRA_OPTS} nodejs; fi; \
    if dpkg-query -W -f='${Status}' npm 2>/dev/null | grep -q "ok installed"; then apt-get purge -y ${APT_EXTRA_OPTS} npm; fi; \
    if dpkg-query -W -f='${Status}' libssl1.1 2>/dev/null | grep -q "ok installed"; then apt-get purge -y ${APT_EXTRA_OPTS} libssl1.1; fi; \
    if dpkg-query -W -f='${Status}' libssl1.1 2>/dev/null | grep -q "ok installed"; then echo "ERROR: libssl1.1 is still installed after purge attempt" >&2; exit 1; fi; \
    rm -rf /root/.nvm /usr/lib/node_modules /usr/local/lib/node_modules /usr/include/node /usr/local/include/node; \
    rm -f /usr/bin/node /usr/bin/npm /usr/bin/npx /usr/local/bin/node /usr/local/bin/npm /usr/local/bin/npx; \
    NODE_CA_CERT_FILE="/usr/local/share/ca-certificates/cera-root-node-extra.crt"; \
    if [ -n "${NODE_EXTRA_CA_CERT_PEM_B64}" ]; then \
      echo "${NODE_EXTRA_CA_CERT_PEM_B64}" | base64 -d > "${NODE_CA_CERT_FILE}"; \
      update-ca-certificates; \
      export NODE_EXTRA_CA_CERTS="${NODE_CA_CERT_FILE}"; \
    fi; \
    NODE_ARCH="$(uname -m)"; \
    if [ "${NODE_ARCH}" = "x86_64" ]; then NODE_ARCH="x64"; \
    elif [ "${NODE_ARCH}" = "aarch64" ]; then NODE_ARCH="arm64"; \
    else echo "Unsupported Node architecture: ${NODE_ARCH}" >&2; exit 1; fi; \
    NODE_BASE_URL="${NODEJS_ORG_MIRROR%/}"; \
    NODE_DIST_URL="${NODE_BASE_URL}/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-${NODE_ARCH}.tar.xz"; \
    if ! curl -fsSLI "${NODE_DIST_URL}" >/dev/null; then \
      NODE_DIST_URL="${NODE_BASE_URL}/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-${NODE_ARCH}.tar.xz"; \
    fi; \
    curl -fsSL "${NODE_DIST_URL}" -o /tmp/node.tar.xz; \
    rm -rf /opt/node && mkdir -p /opt/node; \
    tar -xJf /tmp/node.tar.xz -C /opt/node --strip-components=1; \
    find / -xdev -type d -name 'node-v*' ! -path '/opt/node*' -prune -exec rm -rf {} + 2>/dev/null || true; \
    ln -sf /opt/node/bin/node /usr/local/bin/node; \
    rm -rf /opt/node/lib/node_modules/npm; \
    rm -f /opt/node/bin/npm /opt/node/bin/npx; \
    find /usr /usr/local /opt /ragflow /root -type f \( -name node -o -name nodejs \) 2>/dev/null | while read -r b; do \
      [ -x "${b}" ] || continue; \
      v="$("${b}" -v 2>/dev/null || true)"; \
      case "${v}" in v*) \
        if [ "${v}" != "v${NODE_VERSION}" ] && [ "${b}" != "/opt/node/bin/node" ]; then \
          echo "Removing stale Node binary ${b} (${v})"; rm -f "${b}"; \
        fi ;; \
      esac; \
    done; \
    node --version; \
    if [ "$(node -v)" != "v${NODE_VERSION}" ]; then echo "ERROR: expected Node v${NODE_VERSION}, got $(node -v)" >&2; exit 1; fi; \
    if find /usr /usr/local /opt /ragflow /root -type f \( -name node -o -name nodejs \) 2>/dev/null | while read -r b; do \
      [ -x "${b}" ] || continue; \
      v="$("${b}" -v 2>/dev/null || true)"; \
      case "${v}" in v*) \
        if [ "${v}" != "v${NODE_VERSION}" ] && [ "${b}" != "/opt/node/bin/node" ]; then \
          echo "ERROR: stale Node binary remains: ${b} (${v})" >&2; exit 1; \
        fi ;; \
      esac; \
    done; then :; else exit 1; fi; \
    rm -f /tmp/node.tar.xz; \
	    apt-get autoremove -y; \
	    rm -rf /var/lib/apt/lists/*

# 13.3 Keep bundled Apache Tika 3.2.3 + replace embedded jackson-core for GHSA fixes
RUN set -eux; \
	    JACKSON_CORE_SAFE_VERSION="2.21.1"; \
	    JACKSON_CORE_URL="${MAVEN_REPO_BASE_URL%/}/com/fasterxml/jackson/core/jackson-core/${JACKSON_CORE_SAFE_VERSION}/jackson-core-${JACKSON_CORE_SAFE_VERSION}.jar"; \
	    curl -fsSLo /tmp/jackson-core.jar "${JACKSON_CORE_URL}"

RUN python3 - <<'PY'
import glob
import os
import re
import shutil
import zipfile

patched_component = "/tmp/jackson-core.jar"
tmp_output = "/tmp/tika-server-standard.patched.jar"
stable_tika_link = "/ragflow/tika-server-standard.jar"

versioned_candidates = glob.glob("/ragflow/tika-server-standard-*.jar")

if versioned_candidates:
    def version_key(path: str):
        name = os.path.basename(path)
        m = re.match(r"tika-server-standard-(.+)\.jar$", name)
        if not m:
            return ()
        return tuple(int(x) for x in re.findall(r"\d+", m.group(1)))
    tika_jar = max(versioned_candidates, key=version_key)
elif os.path.exists(stable_tika_link):
    tika_jar = os.path.realpath(stable_tika_link)
else:
    raise SystemExit("Unable to find bundled Tika jar in /ragflow")

drop_prefixes = (
    "com/fasterxml/jackson/core/",
    "META-INF/versions/11/com/fasterxml/jackson/core/",
    "META-INF/versions/17/com/fasterxml/jackson/core/",
    "META-INF/versions/21/com/fasterxml/jackson/core/",
    "META-INF/maven/com.fasterxml.jackson.core/jackson-core/",
)
drop_exact = {
    "META-INF/services/com.fasterxml.jackson.core.ObjectCodec",
    "META-INF/services/com.fasterxml.jackson.core.JsonFactory",
}

with zipfile.ZipFile(patched_component, "r") as jcore:
    skip_overlay_exact = {
        "META-INF/LICENSE",
        "META-INF/NOTICE",
        "META-INF/thirdparty-LICENSE",
        "META-INF/FastDoubleParser-LICENSE",
        "META-INF/FastDoubleParser-NOTICE",
    }
    core_entries = {
        name: jcore.read(name)
        for name in jcore.namelist()
        if not name.endswith("/") and name != "META-INF/MANIFEST.MF" and name not in skip_overlay_exact
    }

removed = 0
with zipfile.ZipFile(tika_jar, "r") as zin, zipfile.ZipFile(tmp_output, "w") as zout:
    for info in zin.infolist():
        name = info.filename
        if name in drop_exact or any(name.startswith(prefix) for prefix in drop_prefixes):
            removed += 1
            continue
        zout.writestr(info, zin.read(name))

    for name, data in core_entries.items():
        zout.writestr(name, data, compress_type=zipfile.ZIP_DEFLATED)

if removed == 0:
    raise SystemExit("No jackson-core entries were found to replace in bundled Tika jar")

shutil.move(tmp_output, tika_jar)
if os.path.lexists(stable_tika_link) and os.path.realpath(stable_tika_link) != os.path.realpath(tika_jar):
    os.remove(stable_tika_link)
if not os.path.lexists(stable_tika_link):
    os.symlink(tika_jar, stable_tika_link)
md5_file = tika_jar + ".md5"
if os.path.exists(md5_file):
    os.remove(md5_file)
print(f"Replaced jackson-core package in {tika_jar}: removed={removed}, added={len(core_entries)}")
PY

RUN rm -f /tmp/jackson-core.jar
ENV TIKA_SERVER_JAR="file:///ragflow/tika-server-standard.jar"

# 13.4 Targeted Python package minimum versions for known CVEs
RUN /ragflow/.venv/bin/python3 - <<'PY'
import importlib.metadata as md
import os
import subprocess

def norm_name(name: str) -> str:
    return name.strip().lower().replace("_", "-")

remove_pkgs = {
    norm_name(pkg)
    for pkg in os.environ.get("REMOVE_PYTHON_PACKAGES", "").replace(",", " ").split()
    if pkg.strip()
}

safe_specs_raw = [s.strip() for s in os.environ.get("SECURITY_PYTHON_SAFE_SPECS", "").split() if s.strip()]
safe_specs_by_name = {}
for spec in safe_specs_raw:
    pkg_name = norm_name(spec.split("==", 1)[0].split(">=", 1)[0].split("<=", 1)[0])
    safe_specs_by_name[pkg_name] = spec

installed_names = set()
for dist in md.distributions():
    try:
        name = dist.metadata["Name"]
    except Exception:
        continue
    if name:
        installed_names.add(norm_name(name))

to_remove = sorted([pkg for pkg in remove_pkgs if pkg in installed_names])
to_reinstall_specs = []
for name, spec in safe_specs_by_name.items():
    if name in remove_pkgs:
        continue
    if name in installed_names:
        to_reinstall_specs.append(spec)

if not to_remove and not to_reinstall_specs:
    print("No Python removals or security reinstalls required.")
    raise SystemExit(0)

if to_remove:
    print("Removing Python packages from base image:", ", ".join(to_remove))
    subprocess.check_call(["/ragflow/.venv/bin/python3", "-m", "pip", "uninstall", "-y", *to_remove])

if to_reinstall_specs:
    reinstall_names = [norm_name(s.split("==", 1)[0].split(">=", 1)[0].split("<=", 1)[0]) for s in to_reinstall_specs]
    print("Reinstalling Python security-pinned packages:", ", ".join(to_reinstall_specs))
    subprocess.check_call(["/ragflow/.venv/bin/python3", "-m", "pip", "uninstall", "-y", *reinstall_names])
    subprocess.check_call([
        "/ragflow/.venv/bin/python3",
        "-m",
        "pip",
        "install",
        "--index-url",
        os.environ["UV_PIP_INDEX_URL"],
        "--no-cache-dir",
        "--upgrade",
        *to_reinstall_specs,
    ])
PY


# 14. Compliance hardening:
#    - remove bundled RAGFlow keypair from image (mount keys at runtime instead)
#    - remove known non-runtime private keys shipped by third-party packages
RUN set -eux; \
    rm -f /ragflow/conf/private.pem /ragflow/conf/public.pem; \
    find /ragflow/.venv/lib -type f -path '*/site-packages/future/backports/test/*' \( -name '*.pem' -o -name '*.key' \) -delete; \
    find /ragflow/.venv/lib -type f -path '*/site-packages/seleniumwire/ca.key' -delete


USER ragflow
ENTRYPOINT ["/ragflow/entrypoint.sh"]

