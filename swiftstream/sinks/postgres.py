"""PostgreSQL sink: writes messages to a PostgreSQL table as JSONB rows."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import psycopg2
import psycopg2.extras

from swiftstream.config import PostgresConfig
from swiftstream.consumer import Message
from swiftstream.sinks.base import Sink

logger = logging.getLogger(__name__)


class PostgresSink(Sink):
    """Writes messages to a PostgreSQL table.

    Each message is stored as a JSONB row with metadata columns.
    The sink batches inserts for performance and creates the target
    table if it does not exist.

    Table schema:
        id          BIGSERIAL PRIMARY KEY
        topic       TEXT NOT NULL
        key         TEXT
        value       JSONB NOT NULL
        headers     JSONB
        created_at  TIMESTAMPTZ DEFAULT NOW()
    """

    def __init__(self, config: PostgresConfig, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._config = config
        self._conn: Any = None
        self._buffer: list[tuple[str, str | None, str, str]] = []
        self._batch_size = config.batch_size

    def _ensure_connection(self) -> Any:
        """Create the database connection if not already connected."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(
                host=self._config.host,
                port=self._config.port,
                dbname=self._config.database,
                user=self._config.user,
                password=self._config.password,
            )
            self._conn.autocommit = False
            self._ensure_table()
        return self._conn

    def _ensure_table(self) -> None:
        """Create the target table if it does not exist."""
        conn = self._conn
        table = self._config.table
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    id BIGSERIAL PRIMARY KEY,
                    topic TEXT NOT NULL,
                    key TEXT,
                    value JSONB NOT NULL,
                    headers JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table}_topic
                ON {table} (topic)
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table}_created_at
                ON {table} (created_at)
            """)
        conn.commit()
        logger.info("Ensured table %s exists", table)

    def write(self, message: Message) -> None:
        """Buffer a message for batch insertion."""
        row = (
            message.topic,
            message.key,
            json.dumps(message.value),
            json.dumps(message.headers) if message.headers else "{}",
        )
        self._buffer.append(row)

        if len(self._buffer) >= self._batch_size:
            self.flush()

    def write_batch(self, messages: list[Message]) -> None:
        """Write a batch of messages directly."""
        rows = [
            (
                m.topic,
                m.key,
                json.dumps(m.value),
                json.dumps(m.headers) if m.headers else "{}",
            )
            for m in messages
        ]
        self._buffer.extend(rows)
        self.flush()

    def flush(self) -> None:
        """Flush the buffer to PostgreSQL using execute_values for performance."""
        if not self._buffer:
            return

        conn = self._ensure_connection()
        table = self._config.table
        batch = list(self._buffer)
        self._buffer.clear()

        start = time.monotonic()
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    f"INSERT INTO {table} (topic, key, value, headers) VALUES %s",
                    batch,
                    template="(%s, %s, %s::jsonb, %s::jsonb)",
                    page_size=500,
                )
            conn.commit()
            elapsed = time.monotonic() - start
            logger.debug("Flushed %d rows to %s in %.3fs", len(batch), table, elapsed)
        except Exception:
            conn.rollback()
            raise

    def close(self) -> None:
        """Flush remaining data and close the connection."""
        try:
            self.flush()
        finally:
            if self._conn and not self._conn.closed:
                self._conn.close()
                self._conn = None
                logger.info("PostgreSQL connection closed")
