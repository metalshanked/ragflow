from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from assessment import ui


def make_app() -> FastAPI:
    app = FastAPI()
    app.include_router(ui.router)
    return app


def test_ui_page_includes_icon_links_and_header_logo():
    app = make_app()

    with TestClient(app) as client:
        response = client.get("/ui")

    assert response.status_code == 200
    assert 'rel="icon" href="/favicon.ico"' in response.text
    assert 'rel="shortcut icon" href="/favicon.ico"' in response.text
    assert 'rel="apple-touch-icon" href="/favicon.ico"' in response.text
    assert '<img src="/favicon.ico" alt=""/>' in response.text
    assert 'id="login-screen"' in response.text
    assert 'id="btn-logout"' in response.text
    assert "AUTH_MODE = authType;" in response.text
    assert "Refresh Token" not in response.text
    assert ">Verify<" not in response.text


def test_icon_routes_serve_svg_and_png_assets():
    app = make_app()

    with TestClient(app) as client:
        icon_response = client.get("/icon.svg")
        favicon_png_response = client.get("/favicon.png")
        favicon_response = client.get("/favicon.ico")

    assert icon_response.status_code == 200
    assert icon_response.headers["content-type"].startswith("image/svg+xml")
    assert "<svg" in icon_response.text

    assert favicon_png_response.status_code == 200
    assert favicon_png_response.headers["content-type"].startswith("image/png")
    assert len(favicon_png_response.content) > 0

    assert favicon_response.status_code == 200
    assert favicon_response.headers["content-type"].startswith("image/x-icon")
    assert favicon_response.content[:4] == b"\x00\x00\x01\x00"
