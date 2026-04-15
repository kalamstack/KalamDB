"""Tests for the run_agent helper — retry + backoff logic."""

import asyncio
import pytest

from kalamdb.agent import _process_one


@pytest.mark.asyncio
async def test_process_one_succeeds_on_first_try():
    calls = 0

    async def handler(record):
        nonlocal calls
        calls += 1

    ok = await _process_one(
        {"offset": 1},
        on_record=handler,
        on_failed=None,
        max_attempts=3,
        initial_backoff_ms=10,
        max_backoff_ms=50,
        multiplier=2.0,
    )
    assert ok is True
    assert calls == 1


@pytest.mark.asyncio
async def test_process_one_retries_on_failure():
    attempts = 0

    async def flaky(record):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("transient")

    ok = await _process_one(
        {"offset": 1},
        on_record=flaky,
        on_failed=None,
        max_attempts=5,
        initial_backoff_ms=1,
        max_backoff_ms=10,
        multiplier=2.0,
    )
    assert ok is True
    assert attempts == 3


@pytest.mark.asyncio
async def test_process_one_gives_up_and_calls_on_failed():
    failed_calls = []

    async def always_fail(record):
        raise RuntimeError("permanent")

    async def on_failed(record, error):
        failed_calls.append((record, error))

    ok = await _process_one(
        {"offset": 42},
        on_record=always_fail,
        on_failed=on_failed,
        max_attempts=2,
        initial_backoff_ms=1,
        max_backoff_ms=10,
        multiplier=2.0,
    )
    assert ok is False
    assert len(failed_calls) == 1
    assert failed_calls[0][0]["offset"] == 42
    assert isinstance(failed_calls[0][1], RuntimeError)
