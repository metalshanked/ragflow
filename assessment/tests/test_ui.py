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
    assert 'rel="icon" href="/icon.svg"' in response.text
    assert 'rel="shortcut icon" href="/icon.svg"' in response.text
    assert '<img src="/icon.svg" alt=""/>' in response.text


def test_icon_routes_serve_svg_asset():
    app = make_app()

    with TestClient(app) as client:
        icon_response = client.get("/icon.svg")
        favicon_response = client.get("/favicon.ico")

    assert icon_response.status_code == 200
    assert icon_response.headers["content-type"].startswith("image/svg+xml")
    assert "<svg" in icon_response.text

    assert favicon_response.status_code == 200
    assert favicon_response.headers["content-type"].startswith("image/svg+xml")
    assert favicon_response.text == icon_response.text
