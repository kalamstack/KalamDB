"""Full pub/sub loop tests — producer table feeds a topic, consumer reads it."""

import asyncio
import pytest


async def _setup_topic_with_source(client, namespace, table_name="src", topic_name="stream"):
    """Create a source table and a topic that mirrors its INSERT events."""
    await client.query(f"""
        CREATE TABLE {namespace}.{table_name} (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            msg TEXT
        )
    """)
    await client.query(f"CREATE TOPIC {namespace}.{topic_name}")
    await client.query(
        f"ALTER TOPIC {namespace}.{topic_name} ADD SOURCE {namespace}.{table_name} ON INSERT"
    )
    return f"{namespace}.{table_name}", f"{namespace}.{topic_name}"


@pytest.mark.asyncio
async def test_consumer_receives_record_from_topic(client, temp_namespace):
    src, topic = await _setup_topic_with_source(client, temp_namespace)

    # Start consuming from earliest so we pick up the row we're about to insert.
    async with await client.consume(
        topic=topic,
        group_id="test-group",
        start="earliest",
    ) as consumer:
        # Insert a row into the source table — should flow to the topic.
        await client.insert(src, {"msg": "hello subscribers"})

        # Poll until we see a record or give up after 5 seconds.
        seen = None
        deadline = asyncio.get_event_loop().time() + 5.0
        while asyncio.get_event_loop().time() < deadline:
            records = await consumer.poll()
            if records:
                seen = records[0]
                break
            await asyncio.sleep(0.2)

        assert seen is not None, "consumer never received the produced record"
        assert "offset" in seen
        # The message_id payload carries the row as JSON — verify our msg made it through.
        assert "hello subscribers" in seen.get("message_id", "")


@pytest.mark.asyncio
async def test_consumer_commit_advances_offset(client, temp_namespace):
    src, topic = await _setup_topic_with_source(client, temp_namespace)

    async with await client.consume(
        topic=topic, group_id="offset-group", start="earliest",
    ) as consumer:
        # Produce two rows.
        await client.insert(src, {"msg": "first"})
        await client.insert(src, {"msg": "second"})

        # Drain both rows and commit.
        got = []
        deadline = asyncio.get_event_loop().time() + 5.0
        while len(got) < 2 and asyncio.get_event_loop().time() < deadline:
            records = await consumer.poll()
            for r in records:
                got.append(r)
                await consumer.mark_processed(r)
            if records:
                await consumer.commit()
            else:
                await asyncio.sleep(0.2)
        assert len(got) >= 2

    # Open a new consumer for the same group — since we committed, it should
    # NOT receive the already-processed records.
    async with await client.consume(
        topic=topic, group_id="offset-group", start="earliest",
    ) as consumer:
        records = []
        deadline = asyncio.get_event_loop().time() + 2.0
        while asyncio.get_event_loop().time() < deadline:
            batch = await consumer.poll()
            records.extend(batch)
            if not batch:
                await asyncio.sleep(0.2)
        assert len(records) == 0, (
            f"expected 0 records after commit (offset should be advanced), "
            f"got {len(records)}"
        )


@pytest.mark.asyncio
async def test_run_agent_processes_records_and_commits(client, temp_namespace):
    from kalamdb import run_agent

    src, topic = await _setup_topic_with_source(client, temp_namespace)
    # Pre-produce a handful of rows.
    for i in range(3):
        await client.insert(src, {"msg": f"msg-{i}"})

    processed = []
    stop = asyncio.Event()

    async def handler(record):
        processed.append(record)
        if len(processed) >= 3:
            stop.set()

    async def run_with_timeout():
        async with asyncio.timeout(10):
            await run_agent(
                client=client,
                topic=topic,
                group_id="agent-test",
                on_record=handler,
                start="earliest",
                poll_idle_sleep_ms=200,
                stop_signal=stop,
            )

    await run_with_timeout()
    assert len(processed) >= 3
