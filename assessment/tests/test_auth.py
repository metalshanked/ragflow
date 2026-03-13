from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from fastapi import HTTPException
from unittest.mock import AsyncMock, patch

import jwt
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from assessment import auth

TEST_JWT_SECRET = "test-secret-key-with-32-bytes-minimum!!"


@contextmanager
def override_settings(**kwargs):
    original: dict[str, object] = {}
    for key, value in kwargs.items():
        original[key] = getattr(auth.settings, key)
        setattr(auth.settings, key, value)
    try:
        yield
    finally:
        for key, value in original.items():
            setattr(auth.settings, key, value)


def make_app() -> FastAPI:
    app = FastAPI()
    app.include_router(auth.auth_router)

    @app.get("/api/v1/protected-read")
    async def protected_read(_: dict | None = Depends(auth.verify_jwt)):
        return {"ok": True}

    @app.post("/api/v1/protected-write")
    async def protected_write(_: dict | None = Depends(auth.verify_jwt)):
        return {"ok": True}

    @app.delete("/api/v1/protected-delete")
    async def protected_delete(_: dict | None = Depends(auth.verify_jwt)):
        return {"ok": True}

    @app.get("/api/v1/native/test-endpoint")
    async def protected_native(_: dict | None = Depends(auth.verify_jwt)):
        return {"ok": True}

    return app


def create_access_token(username: str, roles: list[str]) -> str:
    return auth._create_token(
        username=username,
        roles=roles,
        groups=[],
        token_type="access",
        ttl_minutes=30,
    )


class _FakeEntry:
    def __init__(self, dn: str, attrs: dict[str, object]):
        self.entry_dn = dn
        self.entry_attributes_as_dict = attrs


class _FakeConnection:
    def __init__(self, entry: _FakeEntry):
        self.entry = entry
        self.entries = [entry]
        self.last_search: dict[str, object] | None = None

    def search(self, **kwargs):
        self.last_search = kwargs
        return True


def test_resolve_roles_handles_multiple_groups_and_formats():
    mapping = (
        '{"viewer":["rgf-readers"],'
        '"operator":["rgf-operators"],'
        '"admin":["RGF-Admins","cn=platform-admins,*"]}'
    )
    groups = [
        "CN=RGF-Readers,OU=Groups,DC=example,DC=local",
        "EXAMPLE\\rgf-operators",
        "CN=Platform-Admins,OU=Security,DC=example,DC=local",
    ]

    with override_settings(ldap_group_role_mapping_json=mapping):
        roles = auth._resolve_roles(groups)

    assert roles == [auth.ROLE_VIEWER, auth.ROLE_OPERATOR, auth.ROLE_ADMIN]


def test_resolve_roles_and_groups_keeps_only_mapped_group_names():
    mapping = (
        '{"viewer":["CN=RGF-Readers,OU=Groups,DC=example,DC=local"],'
        '"operator":["RGF-Operators"],'
        '"admin":[]}'
    )
    groups = [
        "CN=RGF-Readers,OU=Groups,DC=example,DC=local",
        "EXAMPLE\\RGF-Operators",
        "CN=Very-Large-Unrelated-Group,OU=Groups,DC=example,DC=local",
    ]

    with override_settings(ldap_group_role_mapping_json=mapping):
        roles, matched_groups = auth._resolve_roles_and_groups(groups)

    assert roles == [auth.ROLE_VIEWER, auth.ROLE_OPERATOR]
    assert matched_groups == ["RGF-Readers", "RGF-Operators"]


def test_create_token_omits_empty_groups_and_iat():
    with override_settings(
        jwt_secret_key=TEST_JWT_SECRET,
        jwt_algorithm="HS256",
    ):
        token = auth._create_token(
            username="alice",
            roles=["admin"],
            groups=[],
            token_type="access",
            ttl_minutes=30,
        )

    payload = jwt.decode(token, TEST_JWT_SECRET, algorithms=["HS256"])
    assert payload["sub"] == "alice"
    assert payload["roles"] == ["admin"]
    assert payload["type"] == "access"
    assert "groups" not in payload
    assert "iat" not in payload


def test_search_user_avoids_memberof_when_targeted_group_lookup_enabled():
    entry = _FakeEntry(
        "CN=Alice,OU=Users,DC=example,DC=local",
        {"memberOf": ["CN=Huge-Group,OU=Groups,DC=example,DC=local"], "cn": ["Alice"]},
    )
    conn = _FakeConnection(entry)
    mapping = '{"viewer":["RGF-Readers"],"operator":[],"admin":[]}'

    with override_settings(
        ldap_user_base_dn="OU=Users,DC=example,DC=local",
        ldap_group_search_base_dn="OU=Groups,DC=example,DC=local",
        ldap_group_name_attribute="cn",
        ldap_group_role_mapping_json=mapping,
    ):
        user_dn, groups = auth._search_user(conn, "alice")

    assert user_dn == "CN=Alice,OU=Users,DC=example,DC=local"
    assert groups == []
    assert conn.last_search is not None
    assert conn.last_search["attributes"] == ["cn"]


def test_auth_token_verify_refresh_happy_path():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(
            jwt_secret_key=TEST_JWT_SECRET,
            jwt_algorithm="HS256",
            ldap_server_uri="ldap://dc1.example.local:389",
            jwt_access_token_ttl_minutes=30,
            jwt_refresh_token_ttl_minutes=60,
        ):
            with patch(
                "assessment.auth._ldap_authenticate",
                AsyncMock(return_value=(["RGF-Readers", "RGF-Operators"], ["viewer", "operator"])),
            ):
                token_resp = client.post(
                    "/api/v1/auth/token",
                    json={"username": "alice", "password": "correct-password"},
                )
                assert token_resp.status_code == 200
                payload = token_resp.json()
                assert payload["username"] == "alice"
                assert set(payload["roles"]) == {"viewer", "operator"}
                assert payload["access_token"]
                assert payload["refresh_token"]

                verify_resp = client.get(
                    "/api/v1/auth/verify",
                    headers={"Authorization": f"Bearer {payload['access_token']}"},
                )
                assert verify_resp.status_code == 200
                verify_data = verify_resp.json()
                assert verify_data["valid"] is True
                assert verify_data["username"] == "alice"
                assert set(verify_data["roles"]) == {"viewer", "operator"}

                refresh_resp = client.post(
                    "/api/v1/auth/refresh",
                    json={"refresh_token": payload["refresh_token"]},
                )
                assert refresh_resp.status_code == 200
                refresh_data = refresh_resp.json()
                assert refresh_data["access_token"]
                assert refresh_data["refresh_token"]
                assert set(refresh_data["roles"]) == {"viewer", "operator"}


def test_rbac_for_viewer_operator_admin_and_native_passthrough():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(
            jwt_secret_key=TEST_JWT_SECRET,
            jwt_algorithm="HS256",
            ldap_server_uri="ldap://dc1.example.local:389",
        ):
            viewer_token = create_access_token("viewer_user", ["viewer"])
            operator_token = create_access_token("operator_user", ["operator"])
            admin_token = create_access_token("admin_user", ["admin"])

            viewer_h = {"Authorization": f"Bearer {viewer_token}"}
            operator_h = {"Authorization": f"Bearer {operator_token}"}
            admin_h = {"Authorization": f"Bearer {admin_token}"}

            assert client.get("/api/v1/protected-read", headers=viewer_h).status_code == 200
            assert client.post("/api/v1/protected-write", headers=viewer_h).status_code == 403
            assert client.delete("/api/v1/protected-delete", headers=viewer_h).status_code == 403
            assert client.get("/api/v1/native/test-endpoint", headers=viewer_h).status_code == 403

            assert client.get("/api/v1/protected-read", headers=operator_h).status_code == 200
            assert client.post("/api/v1/protected-write", headers=operator_h).status_code == 200
            assert client.delete("/api/v1/protected-delete", headers=operator_h).status_code == 403
            assert client.get("/api/v1/native/test-endpoint", headers=operator_h).status_code == 403

            assert client.get("/api/v1/protected-read", headers=admin_h).status_code == 200
            assert client.post("/api/v1/protected-write", headers=admin_h).status_code == 200
            assert client.delete("/api/v1/protected-delete", headers=admin_h).status_code == 200
            assert client.get("/api/v1/native/test-endpoint", headers=admin_h).status_code == 200


def test_roleless_token_rejected_when_ldap_disabled():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(
            jwt_secret_key=TEST_JWT_SECRET,
            jwt_algorithm="HS256",
            ldap_server_uri="",
        ):
            payload = {
                "sub": "legacy-user",
                "type": "access",
                "exp": int(time.time()) + 3600,
            }
            token = jwt.encode(payload, auth.settings.jwt_secret_key, algorithm=auth.settings.jwt_algorithm)
            resp = client.delete(
                "/api/v1/protected-delete",
                headers={"Authorization": f"Bearer {token}"},
            )
            assert resp.status_code == 403
            assert "missing required role claims" in resp.json()["detail"].lower()


def test_roleless_token_rejected_when_ldap_enabled():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(
            jwt_secret_key=TEST_JWT_SECRET,
            jwt_algorithm="HS256",
            ldap_server_uri="ldap://dc1.example.local:389",
        ):
            payload = {
                "sub": "roleless-user",
                "type": "access",
                "exp": int(time.time()) + 3600,
            }
            token = jwt.encode(payload, auth.settings.jwt_secret_key, algorithm=auth.settings.jwt_algorithm)
            resp = client.get(
                "/api/v1/protected-read",
                headers={"Authorization": f"Bearer {token}"},
            )
            assert resp.status_code == 403
            assert "missing required role claims" in resp.json()["detail"].lower()


def test_auth_token_endpoint_requires_ldap_configuration():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(
            jwt_secret_key=TEST_JWT_SECRET,
            jwt_algorithm="HS256",
            ldap_server_uri="",
        ):
            resp = client.post(
                "/api/v1/auth/token",
                json={"username": "alice", "password": "pw"},
            )
            assert resp.status_code == 503
            assert "LDAP auth is not configured" in resp.json()["detail"]


def test_auth_token_endpoint_logs_failed_ldap_login(caplog):
    app = make_app()
    with TestClient(app) as client:
        with override_settings(
            jwt_secret_key=TEST_JWT_SECRET,
            jwt_algorithm="HS256",
            ldap_server_uri="ldap://dc1.example.local:389",
        ):
            with caplog.at_level(logging.WARNING):
                with patch(
                    "assessment.auth._ldap_authenticate",
                    AsyncMock(side_effect=HTTPException(status_code=401, detail="Invalid username or password")),
                ):
                    resp = client.post(
                        "/api/v1/auth/token",
                        json={"username": "alice", "password": "wrong-password"},
                    )

            assert resp.status_code == 401
            assert "LDAP login request denied for user=alice status=401 detail=Invalid username or password" in caplog.text


def test_refresh_rejects_access_token_type():
    app = make_app()
    with TestClient(app) as client:
        with override_settings(
            jwt_secret_key=TEST_JWT_SECRET,
            jwt_algorithm="HS256",
            ldap_server_uri="ldap://dc1.example.local:389",
        ):
            access_token = auth._create_token(
                username="alice",
                roles=["viewer"],
                groups=[],
                token_type="access",
                ttl_minutes=30,
            )
            resp = client.post("/api/v1/auth/refresh", json={"refresh_token": access_token})
            assert resp.status_code == 401
            assert "Invalid token type" in resp.json()["detail"]
