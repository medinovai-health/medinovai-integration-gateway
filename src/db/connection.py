"""Async SQLAlchemy engine and session factory (asyncpg)."""

from __future__ import annotations

import os
from typing import AsyncGenerator, Tuple
from urllib.parse import quote_plus

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

E_DB_HOST_ENV = "MOS_DB_HOST"
E_DB_PORT_ENV = "MOS_DB_PORT"
E_DB_NAME_ENV = "MOS_DB_NAME"
E_DB_USER_ENV = "MOS_DB_USER"
E_DB_PASS_ENV = "MOS_DB_PASS"
E_POOL_MIN = 2
E_POOL_MAX = 10
E_POOL_OVERFLOW = E_POOL_MAX - E_POOL_MIN


def _mos_build_database_url() -> str:
    """Build async PostgreSQL URL from environment (password URL-encoded).

    Returns:
        SQLAlchemy asyncpg connection URL string.
    """
    mos_host = os.environ.get(E_DB_HOST_ENV, "localhost")
    mos_port = os.environ.get(E_DB_PORT_ENV, "5432")
    mos_name = os.environ.get(E_DB_NAME_ENV, "integration_gateway")
    mos_user = os.environ.get(E_DB_USER_ENV, "gateway")
    mos_pass = os.environ.get(E_DB_PASS_ENV, "gateway")
    mos_safe_user = quote_plus(mos_user)
    mos_safe_pass = quote_plus(mos_pass)
    return (
        f"postgresql+asyncpg://{mos_safe_user}:{mos_safe_pass}"
        f"@{mos_host}:{mos_port}/{mos_name}"
    )


async def init_database_pool() -> Tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    """Create async engine and session factory with bounded pool size.

    Returns:
        Tuple of ``AsyncEngine`` and ``async_sessionmaker`` for dependency injection.

    Raises:
        Exception: If the engine cannot be created (misconfiguration, network).
    """
    mos_url = _mos_build_database_url()
    mos_engine = create_async_engine(
        mos_url,
        pool_size=E_POOL_MIN,
        max_overflow=E_POOL_OVERFLOW,
        pool_pre_ping=True,
    )
    mos_session_factory = async_sessionmaker(
        mos_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )
    return mos_engine, mos_session_factory


async def close_database_pool(mos_engine: AsyncEngine) -> None:
    """Dispose of the connection pool.

    Args:
        mos_engine: Engine returned from :func:`init_database_pool`.
    """
    await mos_engine.dispose()


async def check_database_health(mos_engine: AsyncEngine) -> bool:
    """Verify database connectivity with ``SELECT 1``.

    Args:
        mos_engine: Active async SQLAlchemy engine.

    Returns:
        True if the query succeeds; False on any failure.
    """
    try:
        async with mos_engine.connect() as mos_conn:
            await mos_conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


async def mos_session_scope(
    mos_session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession, None]:
    """Yield a session for request-scoped usage (optional helper).

    Args:
        mos_session_factory: Factory from :func:`init_database_pool`.

    Yields:
        AsyncSession bound to a transaction context.
    """
    async with mos_session_factory() as mos_session:
        yield mos_session
