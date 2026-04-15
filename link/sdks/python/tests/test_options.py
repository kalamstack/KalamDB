"""Integration tests for ClientOptions — timeouts, retries."""

import pytest

from kalamdb import KalamClient, Auth, KalamError


@pytest.mark.asyncio
async def test_client_accepts_options_dict():
    """Construction with options should not reject valid keys."""
    from conftest import KALAMDB_URL, KALAMDB_USER, require_integration_env

    password = require_integration_env()

    async with KalamClient(
        KALAMDB_URL,
        Auth.basic(KALAMDB_USER, password),
        options={"timeout_seconds": 10.0, "max_retries": 2},
    ) as c:
        result = await c.query("SELECT 1 AS one")
        assert int(result["results"][0]["rows"][0][0]) == 1


@pytest.mark.asyncio
async def test_short_timeout_against_unreachable_host_fails_fast():
    """A tiny timeout should fail quickly against a black hole, not hang."""
    import time
    # TEST-NET-2 — reserved, unroutable; connection attempts hang until timeout.
    client = KalamClient(
        "http://198.51.100.1:12345",
        Auth.basic("admin", "x"),
        options={"timeout_seconds": 2.0},
    )
    start = time.monotonic()
    with pytest.raises(KalamError):
        await client.query("SELECT 1")
    elapsed = time.monotonic() - start
    # The 2s timeout plus a bit of overhead — definitely under 10s.
    assert elapsed < 10, f"expected fast failure, took {elapsed:.1f}s"
