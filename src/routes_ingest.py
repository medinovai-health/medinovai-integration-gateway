"""HTTP ingress routes for FHIR, HL7v2, and batch ingestion."""

from __future__ import annotations

import os
import uuid
from typing import Any, List, Literal, Optional

import structlog
from fastapi import APIRouter, Header, HTTPException, Request, Response, status
from pydantic import BaseModel, Field

from ingest_service import IngestOrchestrator

E_MAX_BATCH_ITEMS = int(os.environ.get("MOS_INGEST_BATCH_MAX", "10000"))
_mos_skip = os.environ.get("MOS_SKIP_INGEST_AUTH", "true").lower()
E_SKIP_AUTH = _mos_skip in {"1", "true", "yes"}

mos_logger = structlog.get_logger()
_mos_orch = IngestOrchestrator()


def get_orchestrator() -> IngestOrchestrator:
    return _mos_orch


router = APIRouter(prefix="/api/v1/ingest", tags=["ingest"])


class BatchIngestItem(BaseModel):
    """Single batch member."""

    type: Literal["fhir", "hl7"]
    body: str = Field(
        description="Raw message body (ER7 for HL7, JSON string for FHIR)",
    )
    contentType: Optional[str] = Field(
        default=None,
        description="FHIR content type when type=fhir",
    )


class BatchIngestRequest(BaseModel):
    """Batch envelope for mixed FHIR/HL7 items."""

    items: List[BatchIngestItem] = Field(min_length=1)


def _require_auth(mos_authorization: Optional[str]) -> None:
    if E_SKIP_AUTH:
        return
    if not mos_authorization or not mos_authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing_bearer_token",
        )


@router.post("/fhir", status_code=status.HTTP_202_ACCEPTED)
async def ingest_fhir(
    request: Request,
    response: Response,
    authorization: Optional[str] = Header(default=None),
    x_correlation_id: Optional[str] = Header(
        default=None,
        alias="X-Correlation-ID",
    ),
    x_tenant_id: Optional[str] = Header(default=None, alias="X-Tenant-ID"),
) -> dict[str, Any]:
    """Accept a FHIR R4 Bundle (JSON or XML body)."""
    _require_auth(authorization)
    mos_correlation_id = x_correlation_id or str(uuid.uuid4())
    mos_tenant_id = x_tenant_id or "unknown-tenant"
    response.headers["X-Correlation-ID"] = mos_correlation_id
    mos_body = await request.body()
    mos_ct = request.headers.get("content-type")
    mos_logger.info(
        "ingest_fhir_start",
        correlation_id=mos_correlation_id,
        tenant_id=mos_tenant_id,
        actor_id="api",
        category="audit",
        phi_safe=True,
        content_length=len(mos_body),
    )
    try:
        mos_result = await get_orchestrator().ingest_fhir_bundle(
            mos_body, mos_ct, mos_tenant_id, mos_correlation_id
        )
    except Exception as mos_exc:  # noqa: BLE001
        mos_logger.error(
            "ingest_fhir_failed",
            correlation_id=mos_correlation_id,
            tenant_id=mos_tenant_id,
            category="audit",
            phi_safe=True,
            error_type=type(mos_exc).__name__,
        )
        raise HTTPException(
            status_code=400,
            detail="fhir_ingest_failed",
        ) from mos_exc
    return {"correlation_id": mos_correlation_id, **mos_result}


@router.post("/hl7", status_code=status.HTTP_202_ACCEPTED)
async def ingest_hl7(
    request: Request,
    response: Response,
    authorization: Optional[str] = Header(default=None),
    x_correlation_id: Optional[str] = Header(
        default=None,
        alias="X-Correlation-ID",
    ),
    x_tenant_id: Optional[str] = Header(default=None, alias="X-Tenant-ID"),
) -> dict[str, Any]:
    """Accept an HL7v2 ER7 message (text/plain or raw body)."""
    _require_auth(authorization)
    mos_correlation_id = x_correlation_id or str(uuid.uuid4())
    mos_tenant_id = x_tenant_id or "unknown-tenant"
    response.headers["X-Correlation-ID"] = mos_correlation_id
    mos_body = await request.body()
    mos_logger.info(
        "ingest_hl7_start",
        correlation_id=mos_correlation_id,
        tenant_id=mos_tenant_id,
        actor_id="api",
        category="audit",
        phi_safe=True,
        content_length=len(mos_body),
    )
    try:
        mos_result = await get_orchestrator().ingest_hl7(
            mos_body,
            mos_tenant_id,
            mos_correlation_id,
        )
    except Exception as mos_exc:  # noqa: BLE001
        mos_logger.error(
            "ingest_hl7_failed",
            correlation_id=mos_correlation_id,
            tenant_id=mos_tenant_id,
            category="audit",
            phi_safe=True,
            error_type=type(mos_exc).__name__,
        )
        raise HTTPException(
            status_code=400,
            detail="hl7_ingest_failed",
        ) from mos_exc
    return {"correlation_id": mos_correlation_id, **mos_result}


@router.post("/batch", status_code=status.HTTP_202_ACCEPTED)
async def ingest_batch(
    mos_payload: BatchIngestRequest,
    response: Response,
    authorization: Optional[str] = Header(default=None),
    x_correlation_id: Optional[str] = Header(
        default=None,
        alias="X-Correlation-ID",
    ),
    x_tenant_id: Optional[str] = Header(default=None, alias="X-Tenant-ID"),
) -> dict[str, Any]:
    """Process multiple FHIR/HL7 items; returns job_id for status polling."""
    _require_auth(authorization)
    mos_correlation_id = x_correlation_id or str(uuid.uuid4())
    mos_tenant_id = x_tenant_id or "unknown-tenant"
    response.headers["X-Correlation-ID"] = mos_correlation_id
    if len(mos_payload.items) > E_MAX_BATCH_ITEMS:
        raise HTTPException(status_code=413, detail="batch_too_large")
    mos_items = [mos_i.model_dump() for mos_i in mos_payload.items]
    mos_result = await get_orchestrator().ingest_batch(
        mos_items,
        mos_tenant_id,
        mos_correlation_id,
    )
    return {"correlation_id": mos_correlation_id, **mos_result}


@router.get("/status/{job_id}")
async def ingest_status(
    job_id: str,
    authorization: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    """Return ingestion job state (in-memory store)."""
    _require_auth(authorization)
    mos_rec = await get_orchestrator().jobs.get_job(job_id)
    if mos_rec is None:
        raise HTTPException(status_code=404, detail="job_not_found")
    return {
        "job_id": mos_rec.mos_job_id,
        "state": mos_rec.mos_state.value,
        "created_at": mos_rec.mos_created_at,
        "updated_at": mos_rec.mos_updated_at,
        "summary": mos_rec.mos_summary,
        "errors": mos_rec.mos_errors,
    }
