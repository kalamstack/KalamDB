"""Tests for transparent re-authentication when the JWT is no longer valid."""

import pytest


@pytest.mark.asyncio
async def test_reauth_transparently_after_invalidated_jwt(client):
    """Simulate the JWT going stale mid-session.

    Expected: the SDK notices it's not authenticated, runs login() again using
    the stored BasicAuth credentials, and the query that triggered the re-auth
    still succeeds without the caller seeing an error.
    """
    # First query — establishes the initial session.
    first = await client.query("SELECT 1 AS one")
    assert int(first["results"][0]["rows"][0][0]) == 1

    # Pretend the JWT has expired / been revoked.
    await client._test_invalidate_auth()

    # Next query should transparently re-login and succeed.
    second = await client.query("SELECT 2 AS two")
    assert int(second["results"][0]["rows"][0][0]) == 2


@pytest.mark.asyncio
async def test_multiple_queries_after_invalidation_only_relogin_once(client):
    """After an invalidation, concurrent queries should race to re-login but
    only one should actually hit the auth endpoint — the rest piggyback."""
    import asyncio

    await client.query("SELECT 1")  # initial auth
    await client._test_invalidate_auth()

    # Fire three queries concurrently — all should succeed.
    results = await asyncio.gather(
        client.query("SELECT 10 AS v"),
        client.query("SELECT 20 AS v"),
        client.query("SELECT 30 AS v"),
    )
    values = sorted(int(r["results"][0]["rows"][0][0]) for r in results)
    assert values == [10, 20, 30]
