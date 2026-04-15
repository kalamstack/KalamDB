"""Integration tests for authentication flows — JWT, explicit connect, bad creds."""

import pytest

from kalamdb import KalamClient, Auth, KalamAuthError, KalamError


@pytest.mark.asyncio
async def test_jwt_auth_works_after_manual_login():
    """User logs in via HTTP (outside the SDK) and uses the resulting JWT directly."""
    import json, urllib.request
    from conftest import KALAMDB_URL, KALAMDB_USER, require_integration_env

    password = require_integration_env()

    # Step 1: obtain a JWT by hitting the login endpoint directly.
    req = urllib.request.Request(
        f"{KALAMDB_URL}/v1/api/auth/login",
        data=json.dumps({"username": KALAMDB_USER, "password": password}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        token = json.loads(resp.read())["access_token"]

    # Step 2: pass the JWT to the SDK — it should authenticate without re-logging-in.
    async with KalamClient(KALAMDB_URL, Auth.jwt(token)) as c:
        result = await c.query("SELECT 1 AS one")
        assert int(result["results"][0]["rows"][0][0]) == 1


@pytest.mark.asyncio
async def test_wrong_password_raises_auth_error():
    from conftest import KALAMDB_URL, KALAMDB_USER

    client = KalamClient(
        KALAMDB_URL, Auth.basic(KALAMDB_USER, "definitely-wrong-password-xyz")
    )
    # Login is lazy, so the error should surface on the first query.
    with pytest.raises(KalamError):
        await client.query("SELECT 1")
    await client.disconnect()


@pytest.mark.asyncio
async def test_connect_explicit_triggers_login():
    """connect() should perform the login eagerly rather than waiting for first query."""
    from conftest import KALAMDB_URL, KALAMDB_USER, require_integration_env

    password = require_integration_env()

    async with KalamClient(KALAMDB_URL, Auth.basic(KALAMDB_USER, password)) as c:
        await c.connect()
        # Now a subsequent query should succeed without hitting login again.
        result = await c.query("SELECT 2 AS two")
        assert int(result["results"][0]["rows"][0][0]) == 2


@pytest.mark.asyncio
async def test_connect_with_bad_password_fails():
    """connect() with invalid credentials should raise immediately, not silently succeed."""
    from conftest import KALAMDB_URL, KALAMDB_USER

    client = KalamClient(KALAMDB_USER, Auth.basic(KALAMDB_USER, "xxxx"))
    with pytest.raises(KalamError):
        await client.connect()


@pytest.mark.asyncio
async def test_constructor_does_no_network_io():
    """__init__ should not make any network calls — it only stores config."""
    # Point at a clearly unreachable URL. If __init__ tried to connect, it
    # would hang or throw. We just want to prove construction returns.
    client = KalamClient(
        "http://127.0.0.1:1",  # port 1 should refuse quickly
        Auth.basic("admin", "x"),
    )
    # If we got here, construction was non-blocking.
    assert client is not None
    # The error should only surface when we actually try to query.
    with pytest.raises(KalamError):
        await client.query("SELECT 1")
