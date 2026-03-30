"""Shared fixtures: TestClient must enter lifespan so DB pool and orchestrator exist."""

from __future__ import annotations

from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient

from main import app


@pytest.fixture(scope="module")
def mos_client() -> Generator[TestClient, None, None]:
    """Yield a TestClient with async lifespan executed (startup/shutdown).

    Yields:
        Configured :class:`TestClient` instance.
    """
    with TestClient(app) as mos_c:
        yield mos_c
