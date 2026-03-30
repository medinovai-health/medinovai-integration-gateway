"""FastAPI entrypoint for medinovai-integration-gateway (Phase E ingress)."""

from __future__ import annotations

import os
import sys
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

_MOS_SRC = Path(__file__).resolve().parent / "src"
if str(_MOS_SRC) not in sys.path:
    sys.path.insert(0, str(_MOS_SRC))

from datetime import datetime, timezone
from typing import AsyncGenerator, Dict

import structlog
from fastapi import FastAPI, Request
from pydantic import BaseModel, Field

from db.connection import check_database_health, close_database_pool, init_database_pool
from ingest_service import IngestOrchestrator
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
    database: str = Field(
        default="unknown",
        description="PostgreSQL connectivity (healthy / unhealthy / unknown)",
    )


class ReadyResponse(BaseModel):
    """Readiness payload; dependency checks expand in later features."""

    status: str
    service: str
    checks: Dict[str, str] = Field(default_factory=dict)


@asynccontextmanager
async def mos_app_lifespan(mos_app: FastAPI) -> AsyncGenerator[None, None]:
    """Initialize async DB pool and orchestrator; dispose on shutdown.

    Args:
        mos_app: FastAPI application instance.

    Yields:
        Control back to the runtime after startup hooks complete.
    """
    mos_engine, mos_session_factory = await init_database_pool()
    mos_app.state.mos_engine = mos_engine
    mos_app.state.mos_session_factory = mos_session_factory
    mos_app.state.mos_orchestrator = IngestOrchestrator(mos_session_factory)
    mos_logger.info(
        "app_startup",
        correlation_id=str(uuid.uuid4()),
        tenant_id="system",
        actor_id="system",
        category="SYSTEM",
        audit_event="lifespan_start",
        phi_safe=True,
    )
    yield
    await close_database_pool(mos_engine)
    mos_logger.info(
        "app_shutdown",
        correlation_id=str(uuid.uuid4()),
        tenant_id="system",
        actor_id="system",
        category="SYSTEM",
        audit_event="lifespan_end",
        phi_safe=True,
    )


def create_app() -> FastAPI:
    """Build FastAPI application with platform routes (scaffold).

    Returns:
        Configured FastAPI app instance.
    """
    mos_app = FastAPI(
        title=E_SERVICE_NAME,
        version="0.2.0",
        description="Integration Platform ingress gateway (Phase E Sprint 5 — FHIR/HL7 ingest).",
        lifespan=mos_app_lifespan,
    )
    mos_app.include_router(mos_ingest_router)

    @mos_app.get("/health", response_model=HealthResponse, tags=["ops"])
    async def health(request: Request) -> HealthResponse:
        """Liveness probe: process is up and database pool can query."""
        mos_correlation_id = str(uuid.uuid4())
        mos_engine = getattr(request.app.state, "mos_engine", None)
        mos_db_status = "unknown"
        if mos_engine is not None:
            mos_db_status = (
                "healthy" if await check_database_health(mos_engine) else "unhealthy"
            )
        mos_logger.info(
            "health_check",
            correlation_id=mos_correlation_id,
            tenant_id="system",
            actor_id="anonymous",
            category="SYSTEM",
            audit_event="health",
            phi_safe=True,
            database=mos_db_status,
        )
        return HealthResponse(
            status="healthy",
            service=E_SERVICE_NAME,
            timestamp=datetime.now(timezone.utc).isoformat(),
            database=mos_db_status,
        )

    @mos_app.get("/ready", response_model=ReadyResponse, tags=["ops"])
    async def ready(request: Request) -> ReadyResponse:
        """Readiness probe: extend with EPG and datastore checks."""
        mos_correlation_id = str(uuid.uuid4())
        mos_engine = getattr(request.app.state, "mos_engine", None)
        mos_datastore = "unknown"
        if mos_engine is not None:
            mos_datastore = (
                "healthy" if await check_database_health(mos_engine) else "unhealthy"
            )
        mos_checks: Dict[str, str] = {
            "epg": "skipped_stub",
            "datastore": mos_datastore,
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
