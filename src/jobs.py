"""PostgreSQL-backed ingestion job registry."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from db.repository import IngestionJobRepository


class IngestJobState(str, Enum):
    """Lifecycle states for ingest jobs."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class IngestJobRecord:
    """Single job status payload (phi_safe summaries only)."""

    mos_job_id: str
    mos_state: IngestJobState
    mos_created_at: str
    mos_updated_at: str
    mos_summary: Dict[str, Any] = field(default_factory=dict)
    mos_errors: List[str] = field(default_factory=list)


class IngestJobStore:
    """Async job store using :class:`IngestionJobRepository`."""

    def __init__(
        self,
        mos_session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._mos_session_factory = mos_session_factory
        self._mos_repo = IngestionJobRepository()

    async def create_job(
        self,
        mos_summary: Optional[Dict[str, Any]] = None,
        *,
        mos_job_type: str = "batch",
        mos_tenant_id: str,
        mos_correlation_id: str,
        mos_total_records: int = 0,
    ) -> str:
        """Create a job row and return its id string.

        Args:
            mos_summary: Optional extra hints (e.g. item_count); folded into totals.
            mos_job_type: Domain job type label.
            mos_tenant_id: Tenant scope.
            mos_correlation_id: Correlation id for tracing.
            mos_total_records: Expected units of work.

        Returns:
            Stringified UUID primary key.
        """
        mos_extra = dict(mos_summary or {})
        mos_total = int(mos_extra.get("item_count", mos_total_records))
        async with self._mos_session_factory() as mos_session:
            mos_jid = await self._mos_repo.create(
                mos_session,
                job_type=mos_job_type,
                status=IngestJobState.PENDING.value,
                tenant_id=mos_tenant_id,
                correlation_id=mos_correlation_id,
                total_records=mos_total,
            )
            await mos_session.commit()
        return str(mos_jid)

    async def update_job(
        self,
        mos_job_id: str,
        mos_state: IngestJobState,
        mos_summary: Optional[Dict[str, Any]] = None,
        mos_errors: Optional[List[str]] = None,
    ) -> None:
        """Update status, counters, and optional error text.

        Args:
            mos_job_id: Job id string (UUID).
            mos_state: New lifecycle state.
            mos_summary: Optional dict with accepted/failed/item keys.
            mos_errors: Optional error strings merged into ``error_message``.
        """
        try:
            mos_uuid = uuid.UUID(mos_job_id)
        except ValueError:
            return
        mos_proc: Optional[int] = None
        mos_fail: Optional[int] = None
        if mos_summary is not None:
            if "accepted" in mos_summary:
                mos_proc = int(mos_summary["accepted"])
            if "failed" in mos_summary:
                mos_fail = int(mos_summary["failed"])
        mos_err_text: Optional[str] = None
        if mos_errors:
            mos_err_text = "\n".join(mos_errors)
        mos_completed = mos_state in (IngestJobState.COMPLETED, IngestJobState.FAILED)
        async with self._mos_session_factory() as mos_session:
            await self._mos_repo.update_status(
                mos_session,
                mos_uuid,
                status=mos_state.value,
                processed_records=mos_proc,
                failed_records=mos_fail,
                error_message=mos_err_text,
                set_completed=mos_completed,
            )
            await mos_session.commit()

    async def get_job(self, mos_job_id: str) -> Optional[IngestJobRecord]:
        """Load job and map to :class:`IngestJobRecord`.

        Args:
            mos_job_id: Job id string.

        Returns:
            Record if found, else ``None``.
        """
        try:
            mos_uuid = uuid.UUID(mos_job_id)
        except ValueError:
            return None
        async with self._mos_session_factory() as mos_session:
            mos_row = await self._mos_repo.get_by_id(mos_session, mos_uuid)
        if mos_row is None:
            return None
        mos_state = IngestJobState(mos_row.status)
        mos_summary = {
            "item_count": mos_row.total_records,
            "accepted": mos_row.processed_records,
            "failed": mos_row.failed_records,
        }
        mos_errors: List[str] = []
        if mos_row.error_message:
            mos_errors = [mos_row.error_message]
        return IngestJobRecord(
            mos_job_id=str(mos_row.id),
            mos_state=mos_state,
            mos_created_at=mos_row.created_at.isoformat(),
            mos_updated_at=mos_row.updated_at.isoformat(),
            mos_summary=mos_summary,
            mos_errors=mos_errors,
        )
