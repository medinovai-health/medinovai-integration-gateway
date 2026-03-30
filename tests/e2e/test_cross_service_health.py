"""Host-level health checks for all microservices in the E2E compose stack."""

from __future__ import annotations

import os

import httpx
import pytest

# Ports must match docker-compose.e2e.yml host mappings.
E2E_SERVICE_PORTS: list[tuple[str, int]] = [
    ("medinovai-integration-gateway", 18000),
    ("medinovai-omop-lakehouse", 18001),
    ("medinovai-connector-framework", 18002),
    ("medinovai-evidence-store", 18003),
    ("medinovai-governance-engine", 18004),
    ("medinovai-cohort-studio", 18005),
    ("medinovai-workspace-operator", 18006),
]


@pytest.mark.parametrize("mos_service,mos_port", E2E_SERVICE_PORTS)
def test_stack_service_health(mos_service: str, mos_port: int) -> None:
    """Each service must expose GET /health with JSON containing status."""
    mos_host = os.environ.get("E2E_BASE_HOST", "127.0.0.1")
    mos_url = f"http://{mos_host}:{mos_port}/health"
    with httpx.Client(timeout=30.0) as mos_client:
        mos_resp = mos_client.get(mos_url)
    assert mos_resp.status_code == 200, f"{mos_service}: {mos_resp.text}"
    mos_body = mos_resp.json()
    assert "status" in mos_body, f"{mos_service}: missing status in {mos_body}"
