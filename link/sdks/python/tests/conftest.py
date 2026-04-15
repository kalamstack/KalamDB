"""Shared pytest fixtures for the kalamdb SDK tests.

Integration tests talk to a real KalamDB server and will FAIL (not skip) if:
- The server is unreachable at KALAMDB_TEST_URL
- KALAMDB_TEST_PASSWORD is not set

This is intentional — a silent skip would hide real problems in CI.

    KALAMDB_TEST_URL       (default: http://localhost:8088)
    KALAMDB_TEST_USER      (default: admin)
    KALAMDB_TEST_PASSWORD  (no default — must be set)
"""

import os
import urllib.error
import uuid
from urllib.request import urlopen

import pytest
import pytest_asyncio

from kalamdb import KalamClient, Auth


KALAMDB_URL = os.environ.get("KALAMDB_TEST_URL", "http://localhost:8088")
KALAMDB_USER = os.environ.get("KALAMDB_TEST_USER", "admin")
KALAMDB_PASSWORD = os.environ.get("KALAMDB_TEST_PASSWORD")


def _server_is_up(url: str) -> bool:
    """Return True if *something* is listening at the given URL."""
    try:
        urlopen(f"{url}/v1/api/healthcheck", timeout=2)
    except urllib.error.HTTPError:
        # Any HTTP response (including 403 from localhost-only endpoint)
        # proves a server is running.
        return True
    except (urllib.error.URLError, TimeoutError, OSError):
        return False
    return True


def require_integration_env() -> str:
    """Fail the current test with a clear message if prerequisites are missing."""
    if not _server_is_up(KALAMDB_URL):
        pytest.fail(
            f"KalamDB server not reachable at {KALAMDB_URL}. "
            "Start it with `docker compose -f ../../../docker/run/single/docker-compose.yml up -d`."
        )
    if not KALAMDB_PASSWORD:
        pytest.fail(
            "KALAMDB_TEST_PASSWORD env var is required for integration tests. "
            "Run with `KALAMDB_TEST_PASSWORD=... pytest`."
        )
    return KALAMDB_PASSWORD


@pytest_asyncio.fixture
async def client():
    """Yield a connected KalamClient; disconnect automatically when the test finishes."""
    password = require_integration_env()
    c = KalamClient(KALAMDB_URL, Auth.basic(KALAMDB_USER, password))
    yield c
    await c.disconnect()


@pytest_asyncio.fixture
async def temp_namespace(client):
    """Provide a unique namespace per test and drop it on teardown.

    Prevents integration tests from leaving tables behind in the shared DB.
    """
    namespace = f"test_{uuid.uuid4().hex[:12]}"
    await client.query(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    yield namespace
    try:
        await client.query(f"DROP NAMESPACE IF EXISTS {namespace} CASCADE")
    except Exception:
        pass
