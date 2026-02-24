"""
Authentication and authorization for the Assessment API.

Supports two modes:
1. JWT validation for API access (enabled when ASSESSMENT_JWT_SECRET_KEY is set)
2. LDAP/Active Directory login endpoints that mint JWT access/refresh tokens
"""

from __future__ import annotations

import asyncio
import fnmatch
import json
import re
import ssl
from datetime import datetime, timedelta, timezone
from typing import Any

import jwt
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

from .config import settings

try:
    from ldap3 import Connection, Server, Tls
    from ldap3.core.exceptions import LDAPException
    from ldap3.utils.conv import escape_filter_chars
except Exception:  # pragma: no cover - handled at runtime by config checks
    Connection = None  # type: ignore[assignment]
    Server = None  # type: ignore[assignment]
    Tls = None  # type: ignore[assignment]
    LDAPException = Exception  # type: ignore[assignment]

    def escape_filter_chars(value: str) -> str:  # type: ignore[override]
        return value

ROLE_VIEWER = "viewer"
ROLE_OPERATOR = "operator"
ROLE_ADMIN = "admin"
KNOWN_ROLES = (ROLE_VIEWER, ROLE_OPERATOR, ROLE_ADMIN)

_bearer_scheme = HTTPBearer(auto_error=False)

auth_router = APIRouter(prefix="/api/v1/auth", tags=["auth"])


class LoginRequest(BaseModel):
    username: str
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    refresh_expires_in: int
    username: str
    roles: list[str] = Field(default_factory=list)
    groups: list[str] = Field(default_factory=list)


class VerifyResponse(BaseModel):
    valid: bool
    username: str
    roles: list[str] = Field(default_factory=list)
    groups: list[str] = Field(default_factory=list)
    expires_at: int | None = None


def _jwt_auth_enabled() -> bool:
    return bool(settings.jwt_secret_key)


def _ldap_login_enabled() -> bool:
    return bool(settings.ldap_server_uri.strip())


def _load_role_mapping() -> dict[str, list[str]]:
    raw = settings.ldap_group_role_mapping_json.strip() or "{}"
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Invalid ASSESSMENT_LDAP_GROUP_ROLE_MAPPING_JSON: {exc}",
        )

    if not isinstance(parsed, dict):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ASSESSMENT_LDAP_GROUP_ROLE_MAPPING_JSON must be a JSON object",
        )

    mapping: dict[str, list[str]] = {}
    for role in KNOWN_ROLES:
        value = parsed.get(role, [])
        if isinstance(value, str):
            mapping[role] = [value]
        elif isinstance(value, list):
            mapping[role] = [str(v) for v in value if str(v).strip()]
        else:
            mapping[role] = []
    return mapping


def _group_candidate_values(groups: list[str]) -> set[str]:
    """Expand group values for robust matching (full DN + CN + raw names)."""
    candidates: set[str] = set()
    for g in groups:
        raw = (g or "").strip()
        if not raw:
            continue
        low = raw.lower()
        candidates.add(low)
        if "\\" in raw:
            candidates.add(raw.split("\\", 1)[1].strip().lower())
        if "," in raw and "=" in raw:
            first_rdn = raw.split(",", 1)[0]
            if "=" in first_rdn:
                candidates.add(first_rdn.split("=", 1)[1].strip().lower())
        if "=" in raw and "," not in raw:
            parts = raw.split("=", 1)
            if len(parts) == 2:
                candidates.add(parts[1].strip().lower())
    return candidates


def _resolve_roles(groups: list[str]) -> list[str]:
    """Resolve roles from LDAP groups using env-configured mapping JSON."""
    mapping = _load_role_mapping()
    candidates = _group_candidate_values(groups)
    roles: set[str] = set()

    for role, patterns in mapping.items():
        for pattern in patterns:
            p = pattern.strip().lower()
            if not p:
                continue
            matched = False
            if any(ch in p for ch in "*?[]"):
                matched = any(fnmatch.fnmatch(c, p) for c in candidates)
            else:
                matched = p in candidates
            if matched:
                roles.add(role)
                break

    return [r for r in KNOWN_ROLES if r in roles]


def _required_roles_for_request(method: str, path: str) -> set[str]:
    """Return minimum roles allowed for this API request."""
    m = method.upper()
    p = path.lower()

    if "/api/v1/ragflow/" in p:
        return {ROLE_ADMIN}

    if m == "DELETE":
        return {ROLE_ADMIN}
    if m in {"POST", "PUT", "PATCH"}:
        return {ROLE_OPERATOR, ROLE_ADMIN}
    return {ROLE_VIEWER, ROLE_OPERATOR, ROLE_ADMIN}


def _ensure_token_signing_key() -> None:
    if not settings.jwt_secret_key:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="JWT signing key is not configured (ASSESSMENT_JWT_SECRET_KEY).",
        )


def _create_token(
    *,
    username: str,
    roles: list[str],
    groups: list[str],
    token_type: str,
    ttl_minutes: int,
) -> str:
    _ensure_token_signing_key()
    now = datetime.now(timezone.utc)
    exp = now + timedelta(minutes=ttl_minutes)
    payload = {
        "sub": username,
        "roles": roles,
        "groups": groups,
        "type": token_type,
        "iat": int(now.timestamp()),
        "exp": int(exp.timestamp()),
    }
    return jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def _decode_token(token: str, *, expected_type: str | None = None) -> dict[str, Any]:
    _ensure_token_signing_key()
    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret_key,
            algorithms=[settings.jwt_algorithm],
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.InvalidTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token: {exc}",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if expected_type and payload.get("type") != expected_type:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token type. Expected '{expected_type}'.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return payload


def _ldap_server() -> Server:
    if Server is None or Connection is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="LDAP support is unavailable. Install dependency: ldap3",
        )

    tls = None
    if settings.ldap_use_ssl or settings.ldap_start_tls:
        tls = Tls(
            validate=ssl.CERT_REQUIRED if settings.ldap_verify_ssl else ssl.CERT_NONE,
            ca_certs_file=settings.ldap_ca_cert or None,
        )
    return Server(
        settings.ldap_server_uri,
        use_ssl=settings.ldap_use_ssl,
        tls=tls,
        connect_timeout=settings.ldap_connect_timeout_seconds,
        get_info=None,
    )


def _maybe_start_tls(conn: Connection) -> None:
    if settings.ldap_start_tls:
        if getattr(conn, "closed", True) and not conn.open():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Unable to open LDAP connection for StartTLS.",
            )
        conn.start_tls()


def _extract_groups_from_entry(entry: Any) -> list[str]:
    groups: list[str] = []
    if entry is None:
        return groups

    attrs = entry.entry_attributes_as_dict
    key = settings.ldap_group_member_attribute
    val = attrs.get(key) or attrs.get(key.lower()) or attrs.get(key.upper())
    if isinstance(val, str):
        groups.append(val)
    elif isinstance(val, list):
        groups.extend(str(v) for v in val if str(v).strip())
    return groups


def _extract_cn_from_dn(dn: str) -> str:
    m = re.search(r"(?i)\bcn=([^,]+)", dn or "")
    return m.group(1).strip() if m else ""


def _search_user(conn: Connection, username: str) -> tuple[str, list[str]]:
    if not settings.ldap_user_base_dn:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ASSESSMENT_LDAP_USER_BASE_DN is required for LDAP search mode.",
        )

    safe_username = escape_filter_chars(username)
    filter_expr = settings.ldap_user_filter.format(username=safe_username)
    attributes = [settings.ldap_group_member_attribute, "cn"]
    ok = conn.search(
        search_base=settings.ldap_user_base_dn,
        search_filter=filter_expr,
        attributes=attributes,
        size_limit=1,
    )
    if not ok or not conn.entries:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
        )

    entry = conn.entries[0]
    user_dn = str(entry.entry_dn)
    groups = _extract_groups_from_entry(entry)
    return user_dn, groups


def _search_groups(conn: Connection, username: str, user_dn: str) -> list[str]:
    if not settings.ldap_group_search_base_dn:
        return []

    safe_username = escape_filter_chars(username)
    safe_user_dn = escape_filter_chars(user_dn)
    filter_expr = settings.ldap_group_search_filter.format(
        username=safe_username,
        user_dn=safe_user_dn,
    )
    attrs = [settings.ldap_group_name_attribute]
    ok = conn.search(
        search_base=settings.ldap_group_search_base_dn,
        search_filter=filter_expr,
        attributes=attrs,
    )
    if not ok:
        return []

    found: list[str] = []
    for entry in conn.entries:
        dn = str(entry.entry_dn)
        if dn:
            found.append(dn)

        group_attr = settings.ldap_group_name_attribute
        values = entry.entry_attributes_as_dict.get(group_attr, [])
        if isinstance(values, str):
            found.append(values)
        elif isinstance(values, list):
            found.extend(str(v) for v in values if str(v).strip())

        cn = _extract_cn_from_dn(dn)
        if cn:
            found.append(cn)
    return found


def _ldap_authenticate_sync(username: str, password: str) -> tuple[list[str], list[str]]:
    if not username.strip() or not password:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Both username and password are required",
        )

    if not _ldap_login_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="LDAP authentication is not configured.",
        )
    if Connection is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="LDAP support is unavailable. Install dependency: ldap3",
        )

    server = _ldap_server()
    groups: list[str] = []
    user_dn = ""
    search_conn: Connection | None = None

    try:
        if settings.ldap_user_dn_template.strip():
            user_dn = settings.ldap_user_dn_template.format(username=username)
            user_conn = Connection(
                server,
                user=user_dn,
                password=password,
                auto_bind=False,
                raise_exceptions=True,
            )
            _maybe_start_tls(user_conn)
            user_conn.bind()
            user_conn.unbind()

            # Optional post-auth lookup for groups.
            if settings.ldap_user_base_dn.strip():
                if settings.ldap_bind_dn.strip():
                    search_conn = Connection(
                        server,
                        user=settings.ldap_bind_dn,
                        password=settings.ldap_bind_password,
                        auto_bind=False,
                        raise_exceptions=True,
                    )
                else:
                    search_conn = Connection(server, auto_bind=False, raise_exceptions=True)
                _maybe_start_tls(search_conn)
                search_conn.bind()
                _, groups = _search_user(search_conn, username)
                groups.extend(_search_groups(search_conn, username, user_dn))
        else:
            if settings.ldap_bind_dn.strip():
                search_conn = Connection(
                    server,
                    user=settings.ldap_bind_dn,
                    password=settings.ldap_bind_password,
                    auto_bind=False,
                    raise_exceptions=True,
                )
            else:
                search_conn = Connection(server, auto_bind=False, raise_exceptions=True)
            _maybe_start_tls(search_conn)
            search_conn.bind()
            user_dn, groups = _search_user(search_conn, username)
            groups.extend(_search_groups(search_conn, username, user_dn))

            # Password verification by binding as the user.
            user_conn = Connection(
                server,
                user=user_dn,
                password=password,
                auto_bind=False,
                raise_exceptions=True,
            )
            _maybe_start_tls(user_conn)
            user_conn.bind()
            user_conn.unbind()

        # Deduplicate while preserving order.
        dedup_groups = list(dict.fromkeys(g for g in groups if g and str(g).strip()))
        roles = _resolve_roles(dedup_groups)

        if settings.ldap_require_mapped_roles and not roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User is authenticated but has no mapped roles.",
            )
        return dedup_groups, roles
    except HTTPException:
        raise
    except LDAPException:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
        )
    finally:
        try:
            if search_conn is not None and search_conn.bound:
                search_conn.unbind()
        except Exception:
            pass


async def _ldap_authenticate(username: str, password: str) -> tuple[list[str], list[str]]:
    return await asyncio.to_thread(_ldap_authenticate_sync, username, password)


async def verify_jwt(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
) -> dict[str, Any] | None:
    """
    FastAPI dependency that validates bearer JWT and enforces role-based access.

    If JWT signing key is not configured, auth is disabled (pass-through).
    """
    if not _jwt_auth_enabled():
        return None

    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    payload = _decode_token(credentials.credentials, expected_type="access")
    roles = [str(r).strip().lower() for r in payload.get("roles", []) if str(r).strip()]

    # Legacy/manual tokens without role claims: keep compatibility only when
    # LDAP login mode is not enabled.
    if not roles and not _ldap_login_enabled():
        roles = [ROLE_ADMIN]
        payload["roles"] = roles

    required = _required_roles_for_request(request.method, request.url.path)
    if required and not (set(roles) & required):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                "Insufficient permissions for this endpoint. "
                f"Required any of: {sorted(required)}"
            ),
        )

    return payload


@auth_router.post("/token", response_model=TokenResponse)
async def get_token(req: LoginRequest):
    """Authenticate LDAP credentials and issue access + refresh JWT tokens."""
    if not _jwt_auth_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="JWT auth is not configured (missing ASSESSMENT_JWT_SECRET_KEY).",
        )
    if not _ldap_login_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="LDAP auth is not configured (missing ASSESSMENT_LDAP_SERVER_URI).",
        )

    groups, roles = await _ldap_authenticate(req.username.strip(), req.password)
    access_ttl = max(1, settings.jwt_access_token_ttl_minutes)
    refresh_ttl = max(1, settings.jwt_refresh_token_ttl_minutes)

    access_token = _create_token(
        username=req.username.strip(),
        roles=roles,
        groups=groups,
        token_type="access",
        ttl_minutes=access_ttl,
    )
    refresh_token = _create_token(
        username=req.username.strip(),
        roles=roles,
        groups=groups,
        token_type="refresh",
        ttl_minutes=refresh_ttl,
    )

    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=access_ttl * 60,
        refresh_expires_in=refresh_ttl * 60,
        username=req.username.strip(),
        roles=roles,
        groups=groups,
    )


@auth_router.post("/refresh", response_model=TokenResponse)
async def refresh_token(req: RefreshRequest):
    """Exchange a refresh token for a new access/refresh token pair."""
    if not _jwt_auth_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="JWT auth is not configured (missing ASSESSMENT_JWT_SECRET_KEY).",
        )

    payload = _decode_token(req.refresh_token, expected_type="refresh")
    username = str(payload.get("sub", "")).strip()
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token payload.",
        )

    roles = [str(r).strip().lower() for r in payload.get("roles", []) if str(r).strip()]
    groups = [str(g) for g in payload.get("groups", []) if str(g).strip()]

    access_ttl = max(1, settings.jwt_access_token_ttl_minutes)
    refresh_ttl = max(1, settings.jwt_refresh_token_ttl_minutes)
    access_token = _create_token(
        username=username,
        roles=roles,
        groups=groups,
        token_type="access",
        ttl_minutes=access_ttl,
    )
    next_refresh_token = _create_token(
        username=username,
        roles=roles,
        groups=groups,
        token_type="refresh",
        ttl_minutes=refresh_ttl,
    )

    return TokenResponse(
        access_token=access_token,
        refresh_token=next_refresh_token,
        expires_in=access_ttl * 60,
        refresh_expires_in=refresh_ttl * 60,
        username=username,
        roles=roles,
        groups=groups,
    )


@auth_router.get("/verify", response_model=VerifyResponse)
async def verify_token(claims: dict[str, Any] | None = Depends(verify_jwt)):
    """Validate access token and return decoded identity metadata."""
    if claims is None:
        return VerifyResponse(valid=True, username="anonymous", roles=[], groups=[])

    return VerifyResponse(
        valid=True,
        username=str(claims.get("sub", "")),
        roles=[str(r) for r in claims.get("roles", [])],
        groups=[str(g) for g in claims.get("groups", [])],
        expires_at=claims.get("exp"),
    )
