"""Async repositories for ingestion jobs, records, and dead-letter entries."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import Select, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import DeadLetterEntry, IngestionJob, IngestionRecord


class IngestionJobRepository:
    """Persistence for :class:`IngestionJob` rows."""

    async def create(
        self,
        mos_session: AsyncSession,
        *,
        job_type: str,
        status: str,
        tenant_id: str,
        correlation_id: str,
        total_records: int = 0,
    ) -> uuid.UUID:
        """Insert a new ingestion job.

        Args:
            mos_session: Active async session.
            job_type: Logical job category (e.g. ``batch``).
            status: Initial lifecycle status string.
            tenant_id: Tenant scope identifier.
            correlation_id: Request correlation identifier.
            total_records: Expected work units (e.g. batch item count).

        Returns:
            Primary key of the created job.
        """
        mos_job = IngestionJob(
            job_type=job_type,
            status=status,
            tenant_id=tenant_id,
            correlation_id=correlation_id,
            total_records=total_records,
            processed_records=0,
            failed_records=0,
        )
        mos_session.add(mos_job)
        await mos_session.flush()
        return mos_job.id

    async def get_by_id(
        self,
        mos_session: AsyncSession,
        mos_job_id: uuid.UUID,
    ) -> Optional[IngestionJob]:
        """Load a job by primary key.

        Args:
            mos_session: Active async session.
            mos_job_id: Job UUID.

        Returns:
            Model instance or ``None`` if missing.
        """
        return await mos_session.get(IngestionJob, mos_job_id)

    async def update_status(
        self,
        mos_session: AsyncSession,
        mos_job_id: uuid.UUID,
        *,
        status: str,
        processed_records: Optional[int] = None,
        failed_records: Optional[int] = None,
        error_message: Optional[str] = None,
        set_completed: bool = False,
    ) -> None:
        """Update job counters and status.

        Args:
            mos_session: Active async session.
            mos_job_id: Job UUID.
            status: New status value.
            processed_records: Optional successful count override.
            failed_records: Optional failure count override.
            error_message: Optional aggregated error text.
            set_completed: When True, stamps ``completed_at`` to UTC now.
        """
        mos_values: Dict[str, Any] = {
            "status": status,
            "updated_at": datetime.now(timezone.utc),
        }
        if processed_records is not None:
            mos_values["processed_records"] = processed_records
        if failed_records is not None:
            mos_values["failed_records"] = failed_records
        if error_message is not None:
            mos_values["error_message"] = error_message
        if set_completed:
            mos_values["completed_at"] = datetime.now(timezone.utc)
        await mos_session.execute(
            update(IngestionJob).where(IngestionJob.id == mos_job_id).values(**mos_values)
        )

    async def list_by_tenant(
        self,
        mos_session: AsyncSession,
        mos_tenant_id: str,
        *,
        limit: int = 100,
    ) -> Sequence[IngestionJob]:
        """Return recent jobs for a tenant ordered by ``created_at`` desc.

        Args:
            mos_session: Active async session.
            mos_tenant_id: Tenant filter.
            limit: Maximum rows to return.

        Returns:
            Sequence of matching jobs.
        """
        mos_stmt: Select[tuple[IngestionJob]] = (
            select(IngestionJob)
            .where(IngestionJob.tenant_id == mos_tenant_id)
            .order_by(IngestionJob.created_at.desc())
            .limit(limit)
        )
        mos_result = await mos_session.execute(mos_stmt)
        return mos_result.scalars().all()


class IngestionRecordRepository:
    """Persistence for :class:`IngestionRecord` rows."""

    async def create_batch(
        self,
        mos_session: AsyncSession,
        mos_records: List[IngestionRecord],
    ) -> None:
        """Bulk insert ingestion record rows.

        Args:
            mos_session: Active async session.
            mos_records: In-memory record instances to persist.
        """
        mos_session.add_all(mos_records)
        await mos_session.flush()

    async def get_by_job_id(
        self,
        mos_session: AsyncSession,
        mos_job_id: uuid.UUID,
    ) -> Sequence[IngestionRecord]:
        """List records belonging to a job.

        Args:
            mos_session: Active async session.
            mos_job_id: Parent job UUID.

        Returns:
            Sequence of child records.
        """
        mos_stmt = select(IngestionRecord).where(IngestionRecord.job_id == mos_job_id)
        mos_result = await mos_session.execute(mos_stmt)
        return mos_result.scalars().all()

    async def count_by_status(
        self,
        mos_session: AsyncSession,
        mos_job_id: uuid.UUID,
        mos_status: str,
    ) -> int:
        """Count records for a job in a given status.

        Args:
            mos_session: Active async session.
            mos_job_id: Parent job UUID.
            mos_status: Status value to match.

        Returns:
            Number of matching rows.
        """
        mos_stmt = select(func.count()).select_from(IngestionRecord).where(
            IngestionRecord.job_id == mos_job_id,
            IngestionRecord.status == mos_status,
        )
        mos_result = await mos_session.execute(mos_stmt)
        return int(mos_result.scalar_one())


class DeadLetterRepository:
    """Persistence for :class:`DeadLetterEntry` rows."""

    async def create(
        self,
        mos_session: AsyncSession,
        *,
        job_id: Optional[uuid.UUID],
        source_format: str,
        raw_payload_hash: str,
        error_type: str,
        error_message: str,
        max_retries: int = 3,
    ) -> uuid.UUID:
        """Insert a dead-letter row (metadata only).

        Args:
            mos_session: Active async session.
            job_id: Optional related ingestion job.
            source_format: Ingest format label (e.g. ``fhir``).
            raw_payload_hash: SHA-256 (or similar) fingerprint, not raw PHI.
            error_type: Short error classifier.
            error_message: Safe error description.
            max_retries: Upper bound for retry attempts.

        Returns:
            Primary key of the new entry.
        """
        mos_row = DeadLetterEntry(
            job_id=job_id,
            source_format=source_format,
            raw_payload_hash=raw_payload_hash,
            error_type=error_type,
            error_message=error_message,
            retry_count=0,
            max_retries=max_retries,
        )
        mos_session.add(mos_row)
        await mos_session.flush()
        return mos_row.id

    async def list_recent(
        self,
        mos_session: AsyncSession,
        *,
        limit: int = 100,
    ) -> Sequence[DeadLetterEntry]:
        """Return newest dead-letter rows first.

        Args:
            mos_session: Active async session.
            limit: Maximum rows.

        Returns:
            Recent entries ordered by ``created_at`` descending.
        """
        mos_stmt = (
            select(DeadLetterEntry)
            .order_by(DeadLetterEntry.created_at.desc())
            .limit(limit)
        )
        mos_result = await mos_session.execute(mos_stmt)
        return mos_result.scalars().all()

    async def get_retryable(
        self,
        mos_session: AsyncSession,
        *,
        limit: int = 100,
    ) -> Sequence[DeadLetterEntry]:
        """Return entries eligible for retry.

        Args:
            mos_session: Active async session.
            limit: Maximum rows.

        Returns:
            Rows where ``retry_count < max_retries``.
        """
        mos_stmt = (
            select(DeadLetterEntry)
            .where(DeadLetterEntry.retry_count < DeadLetterEntry.max_retries)
            .order_by(DeadLetterEntry.created_at.asc())
            .limit(limit)
        )
        mos_result = await mos_session.execute(mos_stmt)
        return mos_result.scalars().all()

    async def mark_retried(
        self,
        mos_session: AsyncSession,
        mos_entry_id: uuid.UUID,
    ) -> None:
        """Increment retry counter and stamp ``last_retry_at``.

        Args:
            mos_session: Active async session.
            mos_entry_id: Dead-letter row UUID.
        """
        mos_row = await mos_session.get(DeadLetterEntry, mos_entry_id)
        if mos_row is None:
            return
        mos_row.retry_count += 1
        mos_row.last_retry_at = datetime.now(timezone.utc)
        await mos_session.flush()
