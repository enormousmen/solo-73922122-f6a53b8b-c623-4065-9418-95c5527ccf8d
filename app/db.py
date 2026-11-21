"""SQLite plumbing for the thin-volume API."""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

BLOCK_SIZE = 4096
DEFAULT_POOL_BLOCKS = 1024
DEFAULT_BUSY_TIMEOUT_MS = 30_000


class DatabaseBusy(Exception):
    """SQLite could not acquire the writer lock within the busy timeout."""


SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS volumes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    size_bytes  INTEGER NOT NULL CHECK (size_bytes > 0),
    block_size  INTEGER NOT NULL,
    block_count INTEGER NOT NULL CHECK (block_count >= 0),
    version     INTEGER NOT NULL DEFAULT 0,
    cloned_from INTEGER REFERENCES volumes(id) ON DELETE SET NULL,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

-- One row per *physical* 4 KiB block in the pool.  A physical block shared by
-- several volumes (through clone) still has exactly one row here.  refcount is
-- the number of logical block mappings pointing at it.
CREATE TABLE IF NOT EXISTS physical_blocks (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    data       BLOB NOT NULL CHECK (length(data) = 4096),
    refcount   INTEGER NOT NULL DEFAULT 1 CHECK (refcount >= 0),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS block_map (
    volume_id         INTEGER NOT NULL REFERENCES volumes(id) ON DELETE CASCADE,
    lba               INTEGER NOT NULL CHECK (lba >= 0),
    physical_block_id INTEGER NOT NULL REFERENCES physical_blocks(id) ON DELETE RESTRICT,
    PRIMARY KEY (volume_id, lba)
);

CREATE INDEX IF NOT EXISTS idx_block_map_physical
    ON block_map(physical_block_id);

-- Used for pool accounting (COUNT(*)) and for locating blocks whose last
-- reference was just dropped; keeps both off a full table scan.
CREATE INDEX IF NOT EXISTS idx_physical_refcount
    ON physical_blocks(refcount);
"""


def _busy_timeout_ms() -> int:
    raw = os.environ.get("SQLITE_BUSY_TIMEOUT_MS", str(DEFAULT_BUSY_TIMEOUT_MS))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError("SQLITE_BUSY_TIMEOUT_MS must be an integer") from exc
    if value < 0:
        raise ValueError("SQLITE_BUSY_TIMEOUT_MS must be >= 0")
    return value


def open_db(db_path: str) -> sqlite3.Connection:
    """Open a connection configured for explicit transaction control."""
    conn = sqlite3.connect(
        db_path,
        timeout=_busy_timeout_ms() / 1000,
        isolation_level=None,  # autocommit; transactions are explicit below
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(f"PRAGMA busy_timeout = {_busy_timeout_ms()}")
    conn.execute("PRAGMA synchronous = FULL")
    return conn


def init_db(db_path: str) -> None:
    if db_path == ":memory:":
        raise ValueError(
            "DB_PATH must be a file: each request uses its own SQLite connection"
        )
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
    try:
        # WAL lets readers keep a stable snapshot while a writer commits.
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES ('schema_version', '1')"
        )
    finally:
        conn.close()


@contextmanager
def transaction(db_path: str, *, write: bool) -> Iterator[sqlite3.Connection]:
    """Run a transaction.

    Writers take SQLite's RESERVED lock up front (BEGIN IMMEDIATE).  That makes
    capacity checks and mapping changes a single serialized critical section,
    so two concurrent writers can never both consume the last pool block.
    Readers use a deferred transaction, which in WAL mode sees a stable
    snapshot for the lifetime of the transaction.

    Lock-contention failures surface as ``DatabaseBusy`` (HTTP 503) rather than
    as an internal error, whether they happen at BEGIN or at COMMIT.
    """
    conn = open_db(db_path)
    try:
        _run(conn, "BEGIN IMMEDIATE" if write else "BEGIN")
        try:
            yield conn
            _run(conn, "COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
    finally:
        conn.close()


def _run(conn: sqlite3.Connection, sql: str) -> None:
    try:
        conn.execute(sql)
    except sqlite3.OperationalError as exc:
        message = str(exc).lower()
        if "locked" in message or "busy" in message:
            raise DatabaseBusy(str(exc)) from exc
        raise


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")
