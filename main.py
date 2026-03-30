"""FastAPI entrypoint for medinovai-integration-gateway (Phase E ingress)."""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

_MOS_SRC = Path(__file__).resolve().parent / "src"
if str(_MOS_SRC) not in sys.path:
    sys.path.insert(0, str(_MOS_SRC))

from datetime import datetime, timezone
from typing import Dict

import structlog
from fastapi import FastAPI
from pydantic import BaseModel, Field

from routes_ingest import router as mos_ingest_router

E_SERVICE_NAME = "medinovai-integration-gateway"
E_DEFAULT_PORT = "8000"

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(20),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)
mos_logger = structlog.get_logger()


class HealthResponse(BaseModel):
    """Health check payload (no PHI)."""

    status: str = Field(description="Service liveness status")
    service: str = Field(description="Logical service name")
    timestamp: str = Field(description="UTC ISO-8601 timestamp")


class ReadyResponse(BaseModel):
    """Readiness payload; dependency checks expand in later features."""

    status: str
    service: str
    checks: Dict[str, str] = Field(default_factory=dict)


def create_app() -> FastAPI:
    """Build FastAPI application with platform routes (scaffold).

    Returns:
        Configured FastAPI app instance.
    """
    mos_app = FastAPI(
        title=E_SERVICE_NAME,
        version="0.2.0",
        description="Integration Platform ingress gateway (Phase E Sprint 5 — FHIR/HL7 ingest).",
    )
    mos_app.include_router(mos_ingest_router)

    @mos_app.get("/health", response_model=HealthResponse, tags=["ops"])
    async def health() -> HealthResponse:
        """Liveness probe: process is up."""
        mos_correlation_id = str(uuid.uuid4())
        mos_logger.info(
            "health_check",
            correlation_id=mos_correlation_id,
            tenant_id="system",
            actor_id="anonymous",
            category="SYSTEM",
            audit_event="health",
            phi_safe=True,
        )
        return HealthResponse(
            status="healthy",
            service=E_SERVICE_NAME,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

    @mos_app.get("/ready", response_model=ReadyResponse, tags=["ops"])
    async def ready() -> ReadyResponse:
        """Readiness probe: extend with EPG and datastore checks."""
        mos_correlation_id = str(uuid.uuid4())
        mos_checks: Dict[str, str] = {
            "epg": "skipped_stub",
            "datastore": "skipped_stub",
        }
        mos_logger.info(
            "ready_check",
            correlation_id=mos_correlation_id,
            tenant_id="system",
            actor_id="anonymous",
            category="SYSTEM",
            audit_event="ready",
            phi_safe=True,
            checks=mos_checks,
        )
        return ReadyResponse(status="ready", service=E_SERVICE_NAME, checks=mos_checks)

    return mos_app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    mos_port = int(os.environ.get("PORT", E_DEFAULT_PORT))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=mos_port,
        log_level="info",
    )
