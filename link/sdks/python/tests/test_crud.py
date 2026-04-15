"""Integration tests for insert() and delete() — round-trips, data types, pk_column."""

import pytest


@pytest.mark.asyncio
async def test_insert_round_trips_string(client, temp_namespace):
    await client.query(f"""
        CREATE TABLE {temp_namespace}.t (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            name TEXT
        )
    """)
    await client.insert(f"{temp_namespace}.t", {"name": "alice"})
    rows = await client.query_rows(f"SELECT name FROM {temp_namespace}.t")
    assert len(rows) == 1
    assert rows[0]["name"] == "alice"


@pytest.mark.asyncio
async def test_insert_round_trips_integer(client, temp_namespace):
    await client.query(f"""
        CREATE TABLE {temp_namespace}.t (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            age INT
        )
    """)
    await client.insert(f"{temp_namespace}.t", {"age": 30})
    rows = await client.query_rows(f"SELECT age FROM {temp_namespace}.t")
    assert int(rows[0]["age"]) == 30


@pytest.mark.asyncio
async def test_insert_round_trips_boolean(client, temp_namespace):
    await client.query(f"""
        CREATE TABLE {temp_namespace}.t (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            active BOOLEAN
        )
    """)
    await client.insert(f"{temp_namespace}.t", {"active": True})
    rows = await client.query_rows(f"SELECT active FROM {temp_namespace}.t")
    # The server may return this as a bool or a string — accept either.
    value = rows[0]["active"]
    assert value is True or value in ("true", "True", 1, "1")


@pytest.mark.asyncio
async def test_insert_round_trips_null(client, temp_namespace):
    await client.query(f"""
        CREATE TABLE {temp_namespace}.t (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            note TEXT
        )
    """)
    await client.insert(f"{temp_namespace}.t", {"note": None})
    rows = await client.query_rows(f"SELECT note FROM {temp_namespace}.t")
    assert rows[0]["note"] is None


@pytest.mark.asyncio
async def test_insert_escapes_sql_metacharacters_in_values(client, temp_namespace):
    # Parameterized insert should safely handle strings containing quotes,
    # semicolons, backslashes — things that would break string-concat SQL.
    await client.query(f"""
        CREATE TABLE {temp_namespace}.t (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            payload TEXT
        )
    """)
    nasty = "O'Brien; DROP TABLE x; --  \\ 'quoted'"
    await client.insert(f"{temp_namespace}.t", {"payload": nasty})
    rows = await client.query_rows(f"SELECT payload FROM {temp_namespace}.t")
    assert rows[0]["payload"] == nasty


@pytest.mark.asyncio
async def test_insert_multiple_rows_accumulates(client, temp_namespace):
    await client.query(f"""
        CREATE TABLE {temp_namespace}.t (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            name TEXT
        )
    """)
    for name in ["a", "b", "c"]:
        await client.insert(f"{temp_namespace}.t", {"name": name})

    rows = await client.query_rows(f"SELECT name FROM {temp_namespace}.t ORDER BY name")
    assert [r["name"] for r in rows] == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_delete_removes_row_by_default_id(client, temp_namespace):
    await client.query(f"""
        CREATE TABLE {temp_namespace}.t (
            id BIGINT PRIMARY KEY,
            name TEXT
        )
    """)
    await client.insert(f"{temp_namespace}.t", {"id": 1, "name": "x"})
    await client.insert(f"{temp_namespace}.t", {"id": 2, "name": "y"})

    await client.delete(f"{temp_namespace}.t", 1)

    rows = await client.query_rows(f"SELECT id FROM {temp_namespace}.t")
    ids = sorted(int(r["id"]) for r in rows)
    assert ids == [2]


@pytest.mark.asyncio
async def test_delete_supports_custom_pk_column(client, temp_namespace):
    await client.query(f"""
        CREATE TABLE {temp_namespace}.t (
            user_id TEXT PRIMARY KEY,
            email TEXT
        )
    """)
    await client.insert(f"{temp_namespace}.t", {"user_id": "u1", "email": "a@x"})
    await client.insert(f"{temp_namespace}.t", {"user_id": "u2", "email": "b@x"})

    await client.delete(f"{temp_namespace}.t", "u1", pk_column="user_id")

    rows = await client.query_rows(f"SELECT user_id FROM {temp_namespace}.t")
    assert [r["user_id"] for r in rows] == ["u2"]


@pytest.mark.asyncio
async def test_delete_nonexistent_row_does_not_raise(client, temp_namespace):
    await client.query(f"""
        CREATE TABLE {temp_namespace}.t (id BIGINT PRIMARY KEY, v TEXT)
    """)
    # Deleting a row that doesn't exist is a no-op (0 rows affected), not an error.
    await client.delete(f"{temp_namespace}.t", 999)
