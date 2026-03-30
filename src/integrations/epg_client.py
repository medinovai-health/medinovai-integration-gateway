"""Edge Privacy Gateway client with optional mTLS (client cert from env)."""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

E_EPG_URL_ENV = "MOS_EPG_URL"
E_EPG_CERT_ENV = "MOS_EPG_CLIENT_CERT_PATH"
E_EPG_KEY_ENV = "MOS_EPG_CLIENT_KEY_PATH"
E_EPG_CA_ENV = "MOS_EPG_CA_BUNDLE_PATH"

mos_logger = structlog.get_logger()


class EpgMtlsClient:
    """Forward de-identification or policy checks to EPG over mutual TLS when configured."""

    def __init__(self) -> None:
        self._mos_base = os.environ.get(E_EPG_URL_ENV, "").rstrip("/")
        self._mos_cert = os.environ.get(E_EPG_CERT_ENV)
        self._mos_key = os.environ.get(E_EPG_KEY_ENV)
        self._mos_ca = os.environ.get(E_EPG_CA_ENV)

    def _mos_client(self) -> httpx.AsyncClient:
        mos_verify: Any = self._mos_ca or True
        mos_cert: Optional[tuple] = None
        if self._mos_cert and self._mos_key:
            mos_cert = (self._mos_cert, self._mos_key)
        return httpx.AsyncClient(timeout=30.0, verify=mos_verify, cert=mos_cert)

    @retry(wait=wait_exponential(multiplier=0.5, min=0.5, max=8), stop=stop_after_attempt(3))
    async def forward_for_privacy_review(
        self,
        mos_payload_summary: Dict[str, Any],
        mos_correlation_id: str,
        mos_tenant_id: str,
    ) -> Dict[str, str]:
        """POST metadata summary to EPG (no raw payload). Stub no-op when URL unset."""
        if not self._mos_base:
            return {"status": "skipped", "reason": "epg_url_unset"}
        mos_url = f"{self._mos_base}/api/v1/privacy/ingress-preview"
        mos_body = {
            "correlation_id": mos_correlation_id,
            "tenant_id": mos_tenant_id,
            "summary": mos_payload_summary,
        }
        async with self._mos_client() as mos_client:
            mos_resp = await mos_client.post(mos_url, json=mos_body)
            mos_resp.raise_for_status()
        mos_logger.info(
            "epg_forward_ok",
            correlation_id=mos_correlation_id,
            tenant_id=mos_tenant_id,
            category="SYSTEM",
            phi_safe=True,
        )
        return {"status": "accepted"}
