"""Async PostgreSQL access layer for medinovai-integration-gateway."""

from __future__ import annotations

from db.connection import (
    check_database_health,
    close_database_pool,
    init_database_pool,
)
from db.models import Base, DeadLetterEntry, IngestionJob, IngestionRecord
from db.repository import (
    DeadLetterRepository,
    IngestionJobRepository,
    IngestionRecordRepository,
)

__all__ = [
    "Base",
    "DeadLetterEntry",
    "DeadLetterRepository",
    "IngestionJob",
    "IngestionJobRepository",
    "IngestionRecord",
    "IngestionRecordRepository",
    "check_database_health",
    "close_database_pool",
    "init_database_pool",
]
