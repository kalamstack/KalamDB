"""High-level agent runner for Kafka-style topic consumption.

Handles the polling loop, retries, backoff, and commits so your callback only
has to process each record.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Optional

from ._native import KalamClient, Consumer

_log = logging.getLogger("kalamdb.agent")


async def run_agent(
    *,
    client: KalamClient,
    topic: str,
    group_id: str,
    on_record: Callable[[dict[str, Any]], Awaitable[None]],
    on_failed: Optional[Callable[[dict[str, Any], Exception], Awaitable[None]]] = None,
    start: str = "latest",
    max_attempts: int = 3,
    initial_backoff_ms: int = 250,
    max_backoff_ms: int = 2000,
    multiplier: float = 2.0,
    poll_idle_sleep_ms: int = 500,
    ack_on_failed: bool = True,
    stop_signal: Optional[asyncio.Event] = None,
) -> None:
    """Run an agent loop that consumes a topic and processes each record.

    Args:
        client: Connected KalamClient.
        topic: Topic to consume from.
        group_id: Consumer group id (tracks per-group offsets).
        on_record: Async callback for each record. Raise to trigger retry.
        on_failed: Optional callback invoked when a record exhausts retries.
        start: Where to start reading: "earliest" or "latest" (default "latest").
        max_attempts: Max attempts per record before giving up (default 3).
        initial_backoff_ms: Initial retry delay in ms (default 250).
        max_backoff_ms: Maximum retry delay in ms (default 2000).
        multiplier: Backoff multiplier per retry (default 2.0).
        poll_idle_sleep_ms: Sleep when no records are available (default 500).
        ack_on_failed: If True, commit offset even on permanent failure.
            If False, the record stays in the topic for re-processing.
        stop_signal: Optional asyncio.Event — loop exits when set.

    Example:
        async def handle(record):
            summary = await llm.summarize(record["payload"])
            await save_summary(record["message_id"], summary)

        await run_agent(
            client=client,
            topic="blog.posts",
            group_id="summarizer-v1",
            on_record=handle,
            start="earliest",
        )
    """

    async with await client.consume(topic=topic, group_id=group_id, start=start) as consumer:
        while True:
            if stop_signal is not None and stop_signal.is_set():
                _log.info("run_agent stopping (signal)")
                return

            try:
                records = await consumer.poll()
            except Exception as e:
                _log.error("poll failed: %s", e)
                await asyncio.sleep(poll_idle_sleep_ms / 1000.0)
                continue

            if not records:
                await asyncio.sleep(poll_idle_sleep_ms / 1000.0)
                continue

            any_marked = False
            stop_committing = False
            for record in records:
                if stop_signal is not None and stop_signal.is_set():
                    break
                success = await _process_one(
                    record,
                    on_record=on_record,
                    on_failed=on_failed,
                    max_attempts=max_attempts,
                    initial_backoff_ms=initial_backoff_ms,
                    max_backoff_ms=max_backoff_ms,
                    multiplier=multiplier,
                )
                if success or ack_on_failed:
                    await consumer.mark_processed(record)
                    any_marked = True
                else:
                    # Record failed and ack_on_failed is False — stop advancing
                    # the offset so this record gets redelivered next poll.
                    stop_committing = True
                    break

            if any_marked and not stop_committing:
                try:
                    await consumer.commit()
                except Exception as e:
                    _log.error("commit failed: %s", e)


async def _process_one(
    record: dict[str, Any],
    *,
    on_record: Callable[[dict[str, Any]], Awaitable[None]],
    on_failed: Optional[Callable[[dict[str, Any], Exception], Awaitable[None]]],
    max_attempts: int,
    initial_backoff_ms: int,
    max_backoff_ms: int,
    multiplier: float,
) -> bool:
    """Process a single record with retry. Returns True if eventually ok."""
    backoff_ms = initial_backoff_ms
    last_error: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        try:
            await on_record(record)
            return True
        except Exception as e:
            last_error = e
            _log.warning(
                "record offset=%s attempt %d/%d failed: %s",
                record.get("offset"),
                attempt,
                max_attempts,
                e,
            )
            if attempt < max_attempts:
                await asyncio.sleep(backoff_ms / 1000.0)
                backoff_ms = min(int(backoff_ms * multiplier), max_backoff_ms)

    if on_failed is not None and last_error is not None:
        try:
            await on_failed(record, last_error)
        except Exception as e:
            _log.error("on_failed callback raised: %s", e)

    return False
