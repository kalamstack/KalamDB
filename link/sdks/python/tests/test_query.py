"""Integration tests for KalamClient.query / query_rows — values, types, errors."""

import pytest

from kalamdb import KalamError, KalamServerError


@pytest.mark.asyncio
async def test_arithmetic_returns_correct_value(client):
    result = await client.query("SELECT 1 + 1 AS answer")
    row = result["results"][0]["rows"][0]
    assert int(row[0]) == 2


@pytest.mark.asyncio
async def test_query_returns_declared_schema(client):
    result = await client.query("SELECT 42 AS a, 'hi' AS b, TRUE AS c")
    schema = result["results"][0]["schema"]
    names = [f["name"] for f in schema]
    assert names == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_parameterized_int_bound_correctly(client):
    result = await client.query("SELECT $1 AS n", [42])
    row = result["results"][0]["rows"][0]
    assert int(row[0]) == 42


@pytest.mark.asyncio
async def test_parameterized_text_bound_correctly(client):
    result = await client.query("SELECT $1 AS s", ["hello world"])
    row = result["results"][0]["rows"][0]
    assert row[0] == "hello world"


@pytest.mark.asyncio
async def test_parameterized_bool_bound_correctly(client):
    result = await client.query("SELECT $1 AS b", [True])
    row = result["results"][0]["rows"][0]
    assert row[0] in (True, "true", 1, "1")


@pytest.mark.asyncio
async def test_parameterized_float_bound_correctly(client):
    result = await client.query("SELECT $1 AS f", [3.14])
    row = result["results"][0]["rows"][0]
    assert float(row[0]) == pytest.approx(3.14)


@pytest.mark.asyncio
async def test_parameterized_null_bound_correctly(client):
    result = await client.query("SELECT $1 AS x", [None])
    row = result["results"][0]["rows"][0]
    assert row[0] is None


@pytest.mark.asyncio
async def test_query_rows_maps_columns_to_dict(client):
    rows = await client.query_rows("SELECT 1 AS a, 'x' AS b")
    assert len(rows) == 1
    assert rows[0]["a"] == 1 or int(rows[0]["a"]) == 1
    assert rows[0]["b"] == "x"


@pytest.mark.asyncio
async def test_query_rows_empty_result(client, temp_namespace):
    await client.query(f"""
        CREATE TABLE {temp_namespace}.empty_t (id BIGINT PRIMARY KEY, v TEXT)
    """)
    rows = await client.query_rows(f"SELECT * FROM {temp_namespace}.empty_t")
    assert rows == []


@pytest.mark.asyncio
async def test_invalid_sql_raises_server_error(client):
    with pytest.raises(KalamServerError):
        await client.query("NOT VALID SQL AT ALL")


@pytest.mark.asyncio
async def test_server_error_is_a_kalam_error(client):
    # Every specific error type should inherit from KalamError so users can
    # catch broadly if they want.
    with pytest.raises(KalamError):
        await client.query("NOT VALID SQL")


@pytest.mark.asyncio
async def test_server_error_message_contains_detail(client):
    try:
        await client.query("SELECT * FROM nonexistent_ns.nonexistent_table")
    except KalamServerError as e:
        # Error surface should include the server's explanation, not just "oops".
        assert str(e)  # Non-empty message
    else:
        pytest.fail("Query against a missing table should have raised")
