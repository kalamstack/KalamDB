"""Smoke tests for the __repr__ implementations."""

import pytest


@pytest.mark.asyncio
async def test_client_repr_shows_url(client):
    r = repr(client)
    assert "KalamClient" in r
    assert "http" in r  # Some form of URL appears


@pytest.mark.asyncio
async def test_subscription_repr_is_informative(client, temp_namespace):
    await client.query(f"""
        CREATE TABLE {temp_namespace}.t (id BIGINT PRIMARY KEY, v TEXT)
    """)
    sub = await client.subscribe(f"SELECT * FROM {temp_namespace}.t")
    try:
        r = repr(sub)
        assert "Subscription" in r
    finally:
        await sub.close()


@pytest.mark.asyncio
async def test_consumer_repr_is_informative(client):
    consumer = await client.consume(topic="any", group_id="g")
    try:
        r = repr(consumer)
        assert "Consumer" in r
    finally:
        await consumer.close()
