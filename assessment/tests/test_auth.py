from __future__ import annotations

import time
from contextlib import contextmanager
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


def test_legacy_token_without_roles_allowed_when_ldap_disabled():
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
            assert resp.status_code == 200


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
