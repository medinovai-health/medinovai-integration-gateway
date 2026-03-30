"""OMOP lakehouse HTTP client with retries (stub URL when unset)."""

from __future__ import annotations

import os
from typing import Any, Dict

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

E_OMOP_LAKEHOUSE_URL_ENV = "MOS_OMOP_LAKEHOUSE_URL"

mos_logger = structlog.get_logger()


class OmopLakehouseClient:
    """Persist OMOP-shaped rows to data-services / lakehouse REST ingress."""

    def __init__(self) -> None:
        self._mos_base = os.environ.get(E_OMOP_LAKEHOUSE_URL_ENV, "").rstrip("/")

    @retry(wait=wait_exponential(multiplier=0.5, min=0.5, max=8), stop=stop_after_attempt(3))
    async def write_omop_batch(
        self,
        mos_rows: Dict[str, Any],
        mos_correlation_id: str,
        mos_tenant_id: str,
    ) -> Dict[str, str]:
        """POST batch rows; no-op stub when URL missing."""
        if not self._mos_base:
            mos_logger.info(
                "omop_write_skipped",
                correlation_id=mos_correlation_id,
                tenant_id=mos_tenant_id,
                category="SYSTEM",
                phi_safe=True,
            )
            return {"status": "skipped"}
        mos_url = f"{self._mos_base}/api/v1/omop/batch"
        mos_body = {"tenant_id": mos_tenant_id, "rows": mos_rows}
        async with httpx.AsyncClient(timeout=60.0) as mos_client:
            mos_resp = await mos_client.post(mos_url, json=mos_body)
            mos_resp.raise_for_status()
        return {"status": "written"}
