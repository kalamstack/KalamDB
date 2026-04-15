"""Integration tests for live subscriptions — events, iteration, cleanup."""

import asyncio
import pytest


@pytest.mark.asyncio
async def test_subscription_ack_includes_schema(client, temp_namespace):
    await client.query(f"""
        CREATE TABLE {temp_namespace}.t (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            payload TEXT
        )
    """)
    async with await client.subscribe(f"SELECT * FROM {temp_namespace}.t") as sub:
        async with asyncio.timeout(3):
            event = await sub.next()
        assert event["type"] == "subscription_ack"
        # Schema should include the columns we SELECT *'d
        names = [f["name"] for f in event.get("schema", [])]
        assert "id" in names
        assert "payload" in names


@pytest.mark.asyncio
async def test_subscription_initial_data_batch_is_empty_on_fresh_table(client, temp_namespace):
    await client.query(f"""
        CREATE TABLE {temp_namespace}.t (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            payload TEXT
        )
    """)
    async with await client.subscribe(f"SELECT * FROM {temp_namespace}.t") as sub:
        await sub.next()  # ack
        async with asyncio.timeout(3):
            batch = await sub.next()
        assert batch["type"] == "initial_data_batch"
        assert batch.get("rows") == []


@pytest.mark.asyncio
async def test_subscription_initial_data_includes_existing_rows(client, temp_namespace):
    await client.query(f"""
        CREATE TABLE {temp_namespace}.t (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            payload TEXT
        )
    """)
    await client.insert(f"{temp_namespace}.t", {"payload": "preexisting"})

    async with await client.subscribe(f"SELECT * FROM {temp_namespace}.t") as sub:
        await sub.next()  # ack
        async with asyncio.timeout(3):
            batch = await sub.next()
        assert batch["type"] == "initial_data_batch"
        assert len(batch.get("rows", [])) == 1
        assert batch["rows"][0]["payload"] == "preexisting"


@pytest.mark.asyncio
async def test_subscribe_delivers_live_insert(client, temp_namespace):
    await client.query(f"""
        CREATE TABLE {temp_namespace}.t (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            payload TEXT
        )
    """)
    sub = await client.subscribe(f"SELECT * FROM {temp_namespace}.t")

    async def insert_later():
        await asyncio.sleep(0.3)
        await client.insert(f"{temp_namespace}.t", {"payload": "live"})

    task = asyncio.create_task(insert_later())
    try:
        saw_insert_payload = None
        async with asyncio.timeout(5):
            async for event in sub:
                if event.get("type") == "change" and event.get("change_type") == "insert":
                    saw_insert_payload = event["rows"][0]["payload"]
                    break
        assert saw_insert_payload == "live"
    finally:
        await task
        await sub.close()


@pytest.mark.asyncio
async def test_subscribe_delivers_delete_event(client, temp_namespace):
    await client.query(f"""
        CREATE TABLE {temp_namespace}.t (
            id BIGINT PRIMARY KEY,
            payload TEXT
        )
    """)
    await client.insert(f"{temp_namespace}.t", {"id": 42, "payload": "doomed"})

    sub = await client.subscribe(f"SELECT * FROM {temp_namespace}.t")

    async def delete_later():
        await asyncio.sleep(0.3)
        await client.delete(f"{temp_namespace}.t", 42)

    task = asyncio.create_task(delete_later())
    try:
        saw_delete = False
        async with asyncio.timeout(5):
            async for event in sub:
                if event.get("type") == "change" and event.get("change_type") == "delete":
                    saw_delete = True
                    break
        assert saw_delete, "expected a delete change event"
    finally:
        await task
        await sub.close()


@pytest.mark.asyncio
async def test_subscribe_raises_stop_async_iteration_on_close(client, temp_namespace):
    await client.query(f"""
        CREATE TABLE {temp_namespace}.t (id BIGINT PRIMARY KEY, v TEXT)
    """)
    sub = await client.subscribe(f"SELECT * FROM {temp_namespace}.t")
    await sub.close()
    with pytest.raises(StopAsyncIteration):
        await sub.next()


@pytest.mark.asyncio
async def test_multiple_subscriptions_on_same_client(client, temp_namespace):
    await client.query(f"""
        CREATE TABLE {temp_namespace}.t1 (id BIGINT PRIMARY KEY, v TEXT)
    """)
    await client.query(f"""
        CREATE TABLE {temp_namespace}.t2 (id BIGINT PRIMARY KEY, v TEXT)
    """)

    sub1 = await client.subscribe(f"SELECT * FROM {temp_namespace}.t1")
    sub2 = await client.subscribe(f"SELECT * FROM {temp_namespace}.t2")
    try:
        async with asyncio.timeout(3):
            ack1 = await sub1.next()
            ack2 = await sub2.next()
        assert ack1["type"] == "subscription_ack"
        assert ack2["type"] == "subscription_ack"
        assert ack1["subscription_id"] != ack2["subscription_id"]
    finally:
        await sub1.close()
        await sub2.close()
