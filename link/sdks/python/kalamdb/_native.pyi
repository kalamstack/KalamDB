"""Type stubs for the kalamdb Python SDK."""

from typing import Any

class KalamError(Exception):
    """Base class for all KalamDB SDK errors."""

class KalamConnectionError(KalamError):
    """Network, WebSocket, or timeout error."""

class KalamAuthError(KalamError):
    """Authentication failure (bad credentials, expired token)."""

class KalamServerError(KalamError):
    """Server returned an error response."""

class KalamConfigError(KalamError):
    """Invalid client configuration."""

class Auth:
    """Authentication helper."""

    @staticmethod
    def basic(username: str, password: str) -> dict[str, str]:
        """Create basic auth credentials."""
        ...

    @staticmethod
    def jwt(token: str) -> dict[str, str]:
        """Create JWT auth credentials."""
        ...

class KalamClient:
    """KalamDB client for executing SQL queries and subscribing to changes.

    Example::

        from kalamdb import KalamClient, Auth

        client = KalamClient("http://localhost:8080", Auth.basic("admin", "pass"))
        rows = await client.query_rows("SELECT * FROM app.users")
        await client.disconnect()
    """

    def __init__(
        self,
        url: str,
        auth: dict[str, str],
        options: dict[str, Any] | None = None,
    ) -> None:
        """Create a new KalamDB client.

        Args:
            url: Server URL (e.g. "http://localhost:8080")
            auth: Auth credentials from Auth.basic() or Auth.jwt()
            options: Optional client settings:
                - timeout_seconds (float): per-request timeout (default 30)
                - max_retries (int): max HTTP retry attempts (default 3)
        """
        ...

    def __repr__(self) -> str: ...

    async def query(
        self, sql: str, params: list[Any] | None = None
    ) -> dict[str, Any]:
        """Execute SQL and return the full query response.

        Args:
            sql: SQL query, optionally with $1, $2, ... placeholders.
            params: Values to bind to the placeholders.
        """
        ...

    async def query_rows(
        self, sql: str, params: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        """Execute SQL and return rows from the first result set.

        Args:
            sql: SQL query, optionally with $1, $2, ... placeholders.
            params: Values to bind to the placeholders.
        """
        ...

    async def query_with_files(
        self,
        sql: str,
        files: dict[str, tuple],
        params: list[Any] | None = None,
    ) -> dict[str, Any]:
        """Execute SQL with file uploads.

        Args:
            sql: SQL query with FILE("name") placeholders.
            files: Dict mapping placeholder to (filename, bytes) or (filename, bytes, mime).
            params: Optional values for $1, $2, ... placeholders.
        """
        ...

    async def insert(self, table: str, data: dict[str, Any]) -> None:
        """Insert a row into a table."""
        ...

    async def delete(
        self,
        table: str,
        row_id: Any,
        pk_column: str = "id",
    ) -> None:
        """Delete a row by primary key value.

        Args:
            table: Fully qualified table name.
            row_id: The primary key value.
            pk_column: Name of the primary key column (default: "id").
        """
        ...

    async def disconnect(self) -> None:
        """Disconnect the client."""
        ...

    async def consume(
        self,
        topic: str,
        group_id: str,
        start: str = "latest",
    ) -> "Consumer":
        """Create a topic consumer (Kafka-style pub/sub).

        Args:
            topic: Topic to consume from.
            group_id: Consumer group identifier (tracks per-group offsets).
            start: Where to start: "earliest" or "latest" (default "latest").
        """
        ...

    async def __aenter__(self) -> "KalamClient": ...
    async def __aexit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None: ...


class Subscription:
    """Live subscription to a SQL query. Iterate with `async for event in sub`."""

    async def next(self) -> dict[str, Any]:
        """Get the next change event.

        Raises StopAsyncIteration when the stream ends.
        """
        ...

    async def close(self) -> None:
        """Close the subscription."""
        ...

    async def __aenter__(self) -> "Subscription": ...
    async def __aexit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None: ...
    def __aiter__(self) -> "Subscription": ...
    async def __anext__(self) -> dict[str, Any]: ...
    def __repr__(self) -> str: ...


class Consumer:
    """Topic consumer for Kafka-style pub/sub."""

    async def poll(self) -> list[dict[str, Any]]:
        """Poll for the next batch of records. Empty list if none available.

        Records are NOT automatically marked as processed — you must call
        `mark_processed(record)` for each one you've handled before calling
        `commit()`.
        """
        ...

    async def mark_processed(self, record: dict[str, Any]) -> None:
        """Mark a record as successfully processed, so `commit()` will ack it."""
        ...

    async def commit(self) -> None:
        """Commit offsets of all records that have been marked as processed."""
        ...

    async def close(self) -> None:
        """Close the consumer."""
        ...

    async def __aenter__(self) -> "Consumer": ...
    async def __aexit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None: ...
    def __repr__(self) -> str: ...
