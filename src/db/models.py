"""SQLAlchemy 2.0 declarative models for ingestion persistence."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import List, Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


class IngestionJob(Base):
    """Batch or async ingestion job metadata (phi_safe aggregates only)."""

    __tablename__ = "ingestion_jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    job_type: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    tenant_id: Mapped[str] = mapped_column(String(256), nullable=False)
    correlation_id: Mapped[str] = mapped_column(String(128), nullable=False)
    total_records: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    processed_records: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_records: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    records: Mapped[List["IngestionRecord"]] = relationship(
        "IngestionRecord",
        back_populates="job",
        cascade="all, delete-orphan",
    )
    dead_letters: Mapped[List["DeadLetterEntry"]] = relationship(
        "DeadLetterEntry",
        back_populates="job",
    )


class IngestionRecord(Base):
    """Per-resource ingest outcome linked to a job."""

    __tablename__ = "ingestion_records"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ingestion_jobs.id", ondelete="CASCADE"),
        nullable=False,
    )
    resource_type: Mapped[str] = mapped_column(String(128), nullable=False)
    source_format: Mapped[str] = mapped_column(String(32), nullable=False)
    omop_table: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    omop_record_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    error_detail: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    phi_detected: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    job: Mapped["IngestionJob"] = relationship("IngestionJob", back_populates="records")


class DeadLetterEntry(Base):
    """Failed transform metadata; no raw PHI stored."""

    __tablename__ = "dead_letter_entries"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    job_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ingestion_jobs.id", ondelete="SET NULL"),
        nullable=True,
    )
    source_format: Mapped[str] = mapped_column(String(32), nullable=False)
    raw_payload_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    error_type: Mapped[str] = mapped_column(String(128), nullable=False)
    error_message: Mapped[str] = mapped_column(Text, nullable=False)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    last_retry_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    job: Mapped[Optional["IngestionJob"]] = relationship(
        "IngestionJob",
        back_populates="dead_letters",
    )
