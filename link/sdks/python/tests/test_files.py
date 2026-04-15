"""Integration tests for file uploads — query_with_files with FILE columns."""

import pytest

from kalamdb import KalamConfigError


@pytest.mark.asyncio
async def test_file_upload_round_trip_3_tuple_with_mime(client, temp_namespace):
    await client.query(f"""
        CREATE TABLE {temp_namespace}.uploads (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            name TEXT,
            attachment FILE
        )
    """)

    data = b"PNG\x89\x0D\x0A\x1A\x0Asome fake image bytes"
    result = await client.query_with_files(
        f"INSERT INTO {temp_namespace}.uploads (name, attachment) VALUES ($1, FILE('att'))",
        {"att": ("demo.png", data, "image/png")},
        ["demo"],
    )
    assert result["status"] == "success"

    # Confirm a row landed with the right name.
    rows = await client.query_rows(
        f"SELECT name FROM {temp_namespace}.uploads"
    )
    assert rows[0]["name"] == "demo"


@pytest.mark.asyncio
async def test_file_upload_2_tuple_without_mime(client, temp_namespace):
    await client.query(f"""
        CREATE TABLE {temp_namespace}.uploads (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            name TEXT,
            attachment FILE
        )
    """)

    # Omit mime — the 2-tuple form should work.
    result = await client.query_with_files(
        f"INSERT INTO {temp_namespace}.uploads (name, attachment) VALUES ($1, FILE('att'))",
        {"att": ("plain.txt", b"just some text")},
        ["no-mime"],
    )
    assert result["status"] == "success"


@pytest.mark.asyncio
async def test_file_upload_invalid_tuple_raises_config_error(client, temp_namespace):
    await client.query(f"""
        CREATE TABLE {temp_namespace}.uploads (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            attachment FILE
        )
    """)

    # Single-element "tuple" — too few items.
    with pytest.raises(KalamConfigError):
        await client.query_with_files(
            f"INSERT INTO {temp_namespace}.uploads (attachment) VALUES (FILE('att'))",
            {"att": ("only-filename",)},  # missing bytes
        )


@pytest.mark.asyncio
async def test_file_upload_non_tuple_value_raises_config_error(client, temp_namespace):
    await client.query(f"""
        CREATE TABLE {temp_namespace}.uploads (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            attachment FILE
        )
    """)

    # Pass a plain string instead of a tuple.
    with pytest.raises(KalamConfigError):
        await client.query_with_files(
            f"INSERT INTO {temp_namespace}.uploads (attachment) VALUES (FILE('att'))",
            {"att": "not a tuple"},
        )
