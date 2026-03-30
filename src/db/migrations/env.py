"""Alembic environment with async SQLAlchemy (asyncpg)."""

from __future__ import annotations

import asyncio
import os
import sys
from logging.config import fileConfig
from pathlib import Path
from urllib.parse import quote_plus

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

# Ensure `src` is importable when running alembic from repo root
_MOS_SRC = Path(__file__).resolve().parents[2]
if str(_MOS_SRC) not in sys.path:
    sys.path.insert(0, str(_MOS_SRC))

from db.models import Base  # noqa: E402

E_DB_HOST_ENV = "MOS_DB_HOST"
E_DB_PORT_ENV = "MOS_DB_PORT"
E_DB_NAME_ENV = "MOS_DB_NAME"
E_DB_USER_ENV = "MOS_DB_USER"
E_DB_PASS_ENV = "MOS_DB_PASS"

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _mos_async_url() -> str:
    """Build asyncpg URL matching :mod:`db.connection` defaults."""
    mos_host = os.environ.get(E_DB_HOST_ENV, "localhost")
    mos_port = os.environ.get(E_DB_PORT_ENV, "5432")
    mos_name = os.environ.get(E_DB_NAME_ENV, "integration_gateway")
    mos_user = os.environ.get(E_DB_USER_ENV, "gateway")
    mos_pass = os.environ.get(E_DB_PASS_ENV, "gateway")
    return (
        f"postgresql+asyncpg://{quote_plus(mos_user)}:{quote_plus(mos_pass)}"
        f"@{mos_host}:{mos_port}/{mos_name}"
    )


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (SQL script generation only)."""
    mos_url = _mos_async_url().replace("+asyncpg", "")
    context.configure(
        url=mos_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(mos_connection: Connection) -> None:
    """Configure Alembic context for a live connection."""
    context.configure(connection=mos_connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Run migrations in 'online' mode using an async engine."""
    mos_section = config.get_section(config.config_ini_section) or {}
    mos_section["sqlalchemy.url"] = _mos_async_url()

    mos_connectable = async_engine_from_config(
        mos_section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with mos_connectable.connect() as mos_async_conn:
        await mos_async_conn.run_sync(do_run_migrations)

    await mos_connectable.dispose()


def run_async_migrations() -> None:
    """Entry point for asyncio event loop."""
    asyncio.run(run_migrations_online())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_async_migrations()
