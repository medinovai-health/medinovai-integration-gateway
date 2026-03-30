"""In-memory ingestion job registry for async-style status polling."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


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
    """Thread-safe asyncio store for job metadata."""

    def __init__(self) -> None:
        self._mos_lock = asyncio.Lock()
        self._mos_jobs: Dict[str, IngestJobRecord] = {}

    async def create_job(self, mos_summary: Optional[Dict[str, Any]] = None) -> str:
        mos_job_id = str(uuid.uuid4())
        mos_now = datetime.now(timezone.utc).isoformat()
        async with self._mos_lock:
            self._mos_jobs[mos_job_id] = IngestJobRecord(
                mos_job_id=mos_job_id,
                mos_state=IngestJobState.PENDING,
                mos_created_at=mos_now,
                mos_updated_at=mos_now,
                mos_summary=dict(mos_summary or {}),
            )
        return mos_job_id

    async def update_job(
        self,
        mos_job_id: str,
        mos_state: IngestJobState,
        mos_summary: Optional[Dict[str, Any]] = None,
        mos_errors: Optional[List[str]] = None,
    ) -> None:
        mos_now = datetime.now(timezone.utc).isoformat()
        async with self._mos_lock:
            mos_rec = self._mos_jobs.get(mos_job_id)
            if mos_rec is None:
                return
            mos_rec.mos_state = mos_state
            mos_rec.mos_updated_at = mos_now
            if mos_summary is not None:
                mos_rec.mos_summary.update(mos_summary)
            if mos_errors is not None:
                mos_rec.mos_errors.extend(mos_errors)

    async def get_job(self, mos_job_id: str) -> Optional[IngestJobRecord]:
        async with self._mos_lock:
            return self._mos_jobs.get(mos_job_id)
