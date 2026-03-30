"""Smoke tests for ops endpoints."""

from __future__ import annotations

import pytest

pytest.importorskip("httpx")

from fastapi.testclient import TestClient

def test_health_returns_ok(mos_client: TestClient) -> None:
    """GET /health returns healthy service metadata."""
    mos_response = mos_client.get("/health")
    assert mos_response.status_code == 200
    mos_body = mos_response.json()
    assert mos_body["status"] == "healthy"
    assert mos_body["service"] == "medinovai-integration-gateway"
    assert "timestamp" in mos_body
    assert mos_body.get("database") in ("healthy", "unhealthy", "unknown")


def test_ready_returns_ok(mos_client: TestClient) -> None:
    """GET /ready returns readiness stub."""
    mos_response = mos_client.get("/ready")
    assert mos_response.status_code == 200
    mos_body = mos_response.json()
    assert mos_body["status"] == "ready"
    assert "checks" in mos_body
    assert "datastore" in mos_body["checks"]
