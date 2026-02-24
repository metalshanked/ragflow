from __future__ import annotations

from assessment.observability import (
    build_otlp_signal_endpoint,
    openinference_attributes,
    parse_key_value_map,
)


def test_parse_key_value_map_from_json():
    parsed = parse_key_value_map('{"Authorization":"Bearer x","x-tenant":"abc"}')
    assert parsed == {"Authorization": "Bearer x", "x-tenant": "abc"}


def test_parse_key_value_map_from_csv():
    parsed = parse_key_value_map("Authorization=Bearer x, x-tenant=abc")
    assert parsed == {"Authorization": "Bearer x", "x-tenant": "abc"}


def test_parse_key_value_map_invalid_returns_empty():
    assert parse_key_value_map("") == {}
    assert parse_key_value_map("just-text-without-equals") == {}


def test_build_otlp_signal_endpoint():
    assert (
        build_otlp_signal_endpoint("http://collector:4318", "traces")
        == "http://collector:4318/v1/traces"
    )
    assert (
        build_otlp_signal_endpoint("http://collector:4318/", "logs")
        == "http://collector:4318/v1/logs"
    )
    # Full signal endpoint should be kept as-is.
    assert (
        build_otlp_signal_endpoint("http://collector:4318/v1/traces", "traces")
        == "http://collector:4318/v1/traces"
    )


def test_openinference_attributes_context_noop_safe():
    # Should not raise even if OpenInference package/version is unavailable.
    with openinference_attributes(session_id="task-1", user_id="alice"):
        pass
