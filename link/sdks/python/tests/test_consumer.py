"""Integration tests for topic consumers."""

import pytest

from kalamdb import Consumer, KalamConfigError, KalamServerError


@pytest.mark.asyncio
async def test_consume_returns_consumer_instance(client):
    consumer = await client.consume(
        topic="does_not_matter",
        group_id="test_group",
        start="latest",
    )
    assert isinstance(consumer, Consumer)
    await consumer.close()


@pytest.mark.asyncio
async def test_consume_invalid_start_raises_config_error(client):
    with pytest.raises(KalamConfigError):
        await client.consume("topic", "group", start="middle_of_the_road")


@pytest.mark.asyncio
async def test_consume_defaults_to_latest(client):
    # Should not raise with no explicit start — default is "latest".
    c = await client.consume(topic="any", group_id="any")
    assert isinstance(c, Consumer)
    await c.close()


@pytest.mark.asyncio
async def test_poll_nonexistent_topic_raises(client):
    async with await client.consume(
        topic="truly_nonexistent_topic_qwerty",
        group_id="test_group",
    ) as consumer:
        with pytest.raises(KalamServerError):
            await consumer.poll()


@pytest.mark.asyncio
async def test_consumer_context_manager_closes_on_exit(client):
    async with await client.consume(topic="any", group_id="g") as c:
        assert isinstance(c, Consumer)
    # After the with block, the consumer should be closed — further poll fails.
    # Note: after __aexit__, the wrapper is fully gone, so attempting more
    # operations should raise a KalamError.
    with pytest.raises(Exception):
        await c.poll()
