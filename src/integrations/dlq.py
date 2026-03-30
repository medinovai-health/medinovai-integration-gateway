"""Dead-letter persistence for failed transforms (PostgreSQL)."""

from __future__ import annotations

import hashlib
import json
import os
import uuid as mos_uuid_mod
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from db.repository import DeadLetterRepository

E_DLQ_MAX_ENV = "MOS_DLQ_MAX_ITEMS"
E_DLQ_DEFAULT_MAX = 5000
E_DLQ_MAX_RETRIES_ENV = "MOS_DLQ_MAX_RETRIES"
E_DLQ_DEFAULT_MAX_RETRIES = 3


def _mos_payload_fingerprint(mos_entry: Dict[str, Any]) -> str:
    """Build a phi_safe SHA-256 fingerprint for dedupe and audit (no raw payloads).

    Args:
        mos_entry: Metadata dict (kind, error, correlation_id, tenant_id, etc.).

    Returns:
        Hex-encoded SHA-256 digest.
    """
    mos_safe = {
        "kind": str(mos_entry.get("kind") or ""),
        "error": str(mos_entry.get("error") or ""),
        "correlation_id": str(mos_entry.get("correlation_id") or ""),
        "tenant_id": str(mos_entry.get("tenant_id") or ""),
    }
    mos_blob = json.dumps(mos_safe, sort_keys=True, ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(mos_blob).hexdigest()


class DeadLetterQueue:
    """Stores failure metadata via :class:`DeadLetterRepository` (no raw PHI)."""

    def __init__(
        self,
        mos_session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._mos_session_factory = mos_session_factory
        self._mos_repo = DeadLetterRepository()
        self._mos_max_items = int(os.environ.get(E_DLQ_MAX_ENV, str(E_DLQ_DEFAULT_MAX)))
        self._mos_max_retries = int(
            os.environ.get(E_DLQ_MAX_RETRIES_ENV, str(E_DLQ_DEFAULT_MAX_RETRIES))
        )

    async def push(
        self,
        mos_entry: Dict[str, Any],
        *,
        mos_job_id: Optional[str] = None,
    ) -> None:
        """Persist a dead-letter row.

        Args:
            mos_entry: Failure metadata (kind, error, correlation_id, tenant_id).
            mos_job_id: Optional related ingestion job UUID string.
        """
        mos_jid: Optional[mos_uuid_mod.UUID] = None
        if mos_job_id:
            try:
                mos_jid = mos_uuid_mod.UUID(mos_job_id)
            except ValueError:
                mos_jid = None
        mos_format = str(mos_entry.get("kind") or "unknown").lower()
        if mos_format not in {"fhir", "hl7"}:
            mos_format = "unknown"
        mos_hash = _mos_payload_fingerprint(mos_entry)
        mos_err_type = str(mos_entry.get("error_type") or "ingest_failure")
        mos_err_msg = str(mos_entry.get("error") or "unknown_error")
        async with self._mos_session_factory() as mos_session:
            await self._mos_repo.create(
                mos_session,
                job_id=mos_jid,
                source_format=mos_format,
                raw_payload_hash=mos_hash,
                error_type=mos_err_type,
                error_message=mos_err_msg,
                max_retries=self._mos_max_retries,
            )
            await mos_session.commit()

    async def snapshot(self, mos_limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return recent dead-letter rows as plain dicts (bounded).

        Args:
            mos_limit: Max rows; defaults to ``MOS_DLQ_MAX_ITEMS`` cap.

        Returns:
            List of serializable metadata dicts.
        """
        mos_cap = mos_limit if mos_limit is not None else self._mos_max_items
        mos_cap = min(mos_cap, self._mos_max_items)
        async with self._mos_session_factory() as mos_session:
            mos_rows = await self._mos_repo.list_recent(mos_session, limit=mos_cap)
        mos_out: List[Dict[str, Any]] = []
        for mos_r in mos_rows:
            mos_out.append(
                {
                    "id": str(mos_r.id),
                    "job_id": str(mos_r.job_id) if mos_r.job_id else None,
                    "source_format": mos_r.source_format,
                    "raw_payload_hash": mos_r.raw_payload_hash,
                    "error_type": mos_r.error_type,
                    "error_message": mos_r.error_message,
                    "retry_count": mos_r.retry_count,
                    "max_retries": mos_r.max_retries,
                    "created_at": mos_r.created_at.isoformat(),
                }
            )
        return mos_out
