"""Thin-volume semantics on top of SQLite.

Terminology
-----------
* Logical block (LBA) -- one 4 KiB slot inside a volume.
* Physical block      -- one row in ``physical_blocks``.  Volumes cloned from a
  common ancestor point at the same physical rows, so shared data occupies one
  pool slot no matter how many clones reference it.
* Write              -- copy-on-write: the LBA is repointed at a fresh physical
  block.  This keeps every other volume's view immutable.
* Free               -- drop the LBA mapping.  The physical block is released
  only when its reference count drops to zero (last reference).

Every write batch runs in one ``BEGIN IMMEDIATE`` transaction.  That gives us
two guarantees:

1. Capacity is checked against the *net* result of the whole batch, so a full
   pool can still absorb a batch that frees as many blocks as it allocates.
2. Writers are serialized while readers use WAL snapshots, so a clone reader
   observes either the state before the batch or the state after it.
"""

from __future__ import annotations

import base64
import binascii
import math
import os
import sqlite3
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from . import db as dbmod

BLOCK_SIZE = dbmod.BLOCK_SIZE
MAX_READ_BLOCKS = 1024
MAX_BATCH_OPERATIONS = 10_000
# Keep IN (...) lists well under SQLite's variable limit (999 on old builds).
SQL_CHUNK = 400


def _chunks(items: Sequence[Any], size: int = SQL_CHUNK):
    for start in range(0, len(items), size):
        yield items[start : start + size]


class ApiError(Exception):
    """An error that maps to a specific HTTP status code."""

    def __init__(self, status_code: int, detail: Any):
        super().__init__(str(detail))
        self.status_code = status_code
        self.detail = detail


@dataclass(frozen=True)
class Volume:
    id: int
    name: str
    size_bytes: int
    block_size: int
    block_count: int
    version: int
    cloned_from: int | None
    created_at: str
    updated_at: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Volume:
        return cls(
            id=row["id"],
            name=row["name"],
            size_bytes=row["size_bytes"],
            block_size=row["block_size"],
            block_count=row["block_count"],
            version=row["version"],
            cloned_from=row["cloned_from"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def as_dict(self, allocated_blocks: int | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "id": self.id,
            "name": self.name,
            "size_bytes": self.size_bytes,
            "block_size": self.block_size,
            "block_count": self.block_count,
            "version": self.version,
            "cloned_from": self.cloned_from,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
        if allocated_blocks is not None:
            payload["allocated_blocks"] = allocated_blocks
            payload["allocated_bytes"] = allocated_blocks * self.block_size
        return payload


# ---------------------------------------------------------------------------
# validation helpers
# ---------------------------------------------------------------------------


def _validate_name(name: str) -> str:
    if not isinstance(name, str):
        raise ApiError(422, "name must be a string")
    name = name.strip()
    if not name:
        raise ApiError(422, "name must not be empty")
    if len(name) > 128:
        raise ApiError(422, "name must be at most 128 characters")
    return name


def _validate_lba_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ApiError(422, f"{field} must be an integer")
    if value < 0:
        raise ApiError(422, f"{field} must be >= 0")
    return value


def decode_block(data_b64: Any, field: str) -> bytes:
    if not isinstance(data_b64, str):
        raise ApiError(422, f"{field} must be a base64 string")
    try:
        raw = base64.b64decode(data_b64, validate=True)
    except (binascii.Error, ValueError):
        raise ApiError(422, f"{field} is not valid base64") from None
    if len(raw) != BLOCK_SIZE:
        raise ApiError(
            422,
            f"{field} must decode to exactly {BLOCK_SIZE} bytes, got {len(raw)}",
        )
    return raw


def parse_pool_blocks(raw: str | int) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise ValueError("POOL_BLOCKS must be an integer") from None
    if value < 0:
        raise ValueError("POOL_BLOCKS must be >= 0")
    return value


# ---------------------------------------------------------------------------
# pool accounting
# ---------------------------------------------------------------------------


def get_capacity(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT value FROM meta WHERE key = 'pool_blocks'").fetchone()
    if row is None:
        raise ApiError(500, "pool capacity is not initialized")
    return int(row["value"])


def used_blocks(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) AS n FROM physical_blocks").fetchone()
    return int(row["n"])


def pool_stats(conn: sqlite3.Connection) -> dict[str, Any]:
    capacity = get_capacity(conn)
    used = used_blocks(conn)
    return {
        "capacity_blocks": capacity,
        "used_blocks": used,
        "free_blocks": capacity - used,
        "block_size": BLOCK_SIZE,
        "capacity_bytes": capacity * BLOCK_SIZE,
        "used_bytes": used * BLOCK_SIZE,
        "free_bytes": (capacity - used) * BLOCK_SIZE,
    }


# ---------------------------------------------------------------------------
# volume operations
# ---------------------------------------------------------------------------


def _get_volume_row(conn: sqlite3.Connection, volume_id: int) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM volumes WHERE id = ?", (volume_id,)).fetchone()
    if row is None:
        raise ApiError(404, f"volume {volume_id} not found")
    return row


def _allocated_blocks(conn: sqlite3.Connection, volume_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM block_map WHERE volume_id = ?", (volume_id,)
    ).fetchone()
    return int(row["n"])


def list_volumes(db_path: str) -> list[dict[str, Any]]:
    with dbmod.transaction(db_path, write=False) as conn:
        rows = conn.execute("SELECT * FROM volumes ORDER BY id").fetchall()
        return [
            Volume.from_row(row).as_dict(_allocated_blocks(conn, row["id"]))
            for row in rows
        ]


def get_volume(db_path: str, volume_id: int) -> dict[str, Any]:
    with dbmod.transaction(db_path, write=False) as conn:
        volume = Volume.from_row(_get_volume_row(conn, volume_id))
        return volume.as_dict(_allocated_blocks(conn, volume_id))


def create_volume(
    db_path: str, *, name: str, size_bytes: int, clone_from: int | None = None
) -> dict[str, Any]:
    name = _validate_name(name)
    if isinstance(size_bytes, bool) or not isinstance(size_bytes, int):
        raise ApiError(422, "size_bytes must be an integer")
    if size_bytes <= 0:
        raise ApiError(422, "size_bytes must be > 0")
    block_count = math.ceil(size_bytes / BLOCK_SIZE)
    if size_bytes % BLOCK_SIZE != 0:
        raise ApiError(
            422, f"size_bytes must be a multiple of {BLOCK_SIZE} (4 KiB blocks)"
        )

    with dbmod.transaction(db_path, write=True) as conn:
        if clone_from is None:
            source_row = None
        else:
            source_row = _get_volume_row(conn, clone_from)
            if source_row["size_bytes"] != size_bytes:
                raise ApiError(
                    409,
                    {
                        "error": "size_mismatch",
                        "detail": (
                            f"clone size_bytes ({size_bytes}) must equal source "
                            f"size_bytes ({source_row['size_bytes']})"
                        ),
                        "source_size_bytes": source_row["size_bytes"],
                    },
                )

        now = dbmod.utc_now()
        try:
            cur = conn.execute(
                """
                INSERT INTO volumes
                    (name, size_bytes, block_size, block_count, version,
                     cloned_from, created_at, updated_at)
                VALUES (?, ?, ?, ?, 0, ?, ?, ?)
                """,
                (
                    name,
                    size_bytes,
                    BLOCK_SIZE,
                    block_count,
                    source_row["id"] if source_row else None,
                    now,
                    now,
                ),
            )
        except sqlite3.IntegrityError:
            raise ApiError(409, f"volume name {name!r} already exists") from None
        volume_id = int(cur.lastrowid)

        if source_row is not None:
            # Copy mappings, not data: every shared physical block gains a
            # reference but the pool's used-block count does not change.
            conn.execute(
                """
                INSERT INTO block_map(volume_id, lba, physical_block_id)
                SELECT ?, lba, physical_block_id
                FROM block_map WHERE volume_id = ?
                """,
                (volume_id, source_row["id"]),
            )
            conn.execute(
                """
                UPDATE physical_blocks
                SET refcount = refcount + 1
                WHERE id IN (
                    SELECT physical_block_id FROM block_map WHERE volume_id = ?
                )
                """,
                (volume_id,),
            )

        volume = Volume.from_row(_get_volume_row(conn, volume_id))
        result = volume.as_dict(_allocated_blocks(conn, volume_id))
        result["pool"] = pool_stats(conn)
        return result


def delete_volume(db_path: str, volume_id: int) -> dict[str, Any]:
    with dbmod.transaction(db_path, write=True) as conn:
        row = _get_volume_row(conn, volume_id)
        # Collect the physical blocks that this volume references.  Shared
        # blocks stay alive as long as a clone still maps them.
        refs = conn.execute(
            "SELECT physical_block_id FROM block_map WHERE volume_id = ?",
            (volume_id,),
        ).fetchall()
        conn.execute("DELETE FROM volumes WHERE id = ?", (volume_id,))
        released = 0
        for ref in refs:
            conn.execute(
                "UPDATE physical_blocks SET refcount = refcount - 1 WHERE id = ?",
                (ref["physical_block_id"],),
            )
        rows = conn.execute(
            "SELECT id FROM physical_blocks WHERE refcount <= 0"
        ).fetchall()
        released = len(rows)
        for row_ in rows:
            conn.execute("DELETE FROM physical_blocks WHERE id = ?", (row_["id"],))
        stats = pool_stats(conn)
        return {
            "deleted_volume_id": row["id"],
            "released_blocks": released,
            "pool": stats,
        }


# ---------------------------------------------------------------------------
# block read
# ---------------------------------------------------------------------------


def _read_blocks(
    conn: sqlite3.Connection, volume: Volume, lbas: Sequence[int]
) -> list[bytes]:
    if not lbas:
        return []
    by_lba: dict[int, bytes] = {}
    for chunk in _chunks(list(lbas)):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT bm.lba, pb.data
            FROM block_map bm
            JOIN physical_blocks pb ON pb.id = bm.physical_block_id
            WHERE bm.volume_id = ? AND bm.lba IN ({placeholders})
            """,
            (volume.id, *chunk),
        ).fetchall()
        for row in rows:
            by_lba[row["lba"]] = bytes(row["data"])
    return [by_lba.get(lba, b"\x00" * BLOCK_SIZE) for lba in lbas]


def read_block(db_path: str, volume_id: int, lba: int) -> tuple[bytes, Volume]:
    lba = _validate_lba_int(lba, "lba")
    with dbmod.transaction(db_path, write=False) as conn:
        volume = Volume.from_row(_get_volume_row(conn, volume_id))
        if lba >= volume.block_count:
            raise ApiError(
                416,
                f"lba {lba} is outside volume range [0, {volume.block_count})",
            )
        data = _read_blocks(conn, volume, [lba])[0]
        return data, volume


def read_blocks(
    db_path: str, volume_id: int, *, start: int, count: int
) -> tuple[list[bytes], Volume]:
    start = _validate_lba_int(start, "start")
    count = _validate_lba_int(count, "count")
    if count < 1:
        raise ApiError(422, "count must be >= 1")
    if count > MAX_READ_BLOCKS:
        raise ApiError(422, f"count must be <= {MAX_READ_BLOCKS}")
    with dbmod.transaction(db_path, write=False) as conn:
        volume = Volume.from_row(_get_volume_row(conn, volume_id))
        end = start + count
        if end > volume.block_count:
            raise ApiError(
                416,
                (
                    f"range [{start}, {end}) exceeds volume block count "
                    f"{volume.block_count}"
                ),
            )
        return _read_blocks(conn, volume, list(range(start, end))), volume


# ---------------------------------------------------------------------------
# block batch update (write / free)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BatchOperation:
    writes: list[tuple[int, bytes]]
    frees: list[int]


def _apply_batch_locked(
    conn: sqlite3.Connection, volume: Volume, ops: BatchOperation
) -> dict[str, Any]:
    touched = [lba for lba, _ in ops.writes] + ops.frees
    capacity = get_capacity(conn)
    used_before = used_blocks(conn)

    old_mapping: dict[int, int] = {}
    for chunk in _chunks(touched):
        placeholders = ",".join("?" for _ in chunk)
        for row in conn.execute(
            f"""
            SELECT lba, physical_block_id FROM block_map
            WHERE volume_id = ? AND lba IN ({placeholders})
            """,
            (volume.id, *chunk),
        ):
            old_mapping[row["lba"]] = row["physical_block_id"]

    refcounts: dict[int, int] = {}
    for chunk in _chunks(list(old_mapping.values())):
        placeholders = ",".join("?" for _ in chunk)
        for row in conn.execute(
            f"SELECT id, refcount FROM physical_blocks "
            f"WHERE id IN ({placeholders})",
            tuple(chunk),
        ):
            refcounts[row["id"]] = int(row["refcount"])

    # Every touched mapping disappears; count how many references each
    # physical block loses.
    lost_refs: dict[int, int] = {}
    for physical_id in old_mapping.values():
        lost_refs[physical_id] = lost_refs.get(physical_id, 0) + 1

    for physical_id, lost in lost_refs.items():
        remaining = refcounts.get(physical_id, 0) - lost
        if remaining < 0:
            raise ApiError(500, "corrupt block map: refcount underflow")

    released = sum(
        1 for physical_id, lost in lost_refs.items()
        if refcounts.get(physical_id, 0) - lost == 0
    )
    allocated = len(ops.writes)
    used_after = used_before - released + allocated

    if used_after > capacity:
        raise ApiError(
            409,
            {
                "error": "pool_full",
                "detail": (
                    "batch would exceed pool capacity; the whole batch was "
                    "rejected and the volume is unchanged"
                ),
                "capacity_blocks": capacity,
                "used_blocks": used_before,
                "requested_new_blocks": allocated,
                "reclaimable_blocks": released,
                "required_blocks": used_after,
                "over_by_blocks": used_after - capacity,
            },
        )

    # Apply: drop old mappings first (the FK is RESTRICT), then decrement
    # reference counts and release blocks that reached zero, then insert the
    # fresh copy-on-write blocks.  Net capacity was validated above, and all of
    # this happens inside one transaction so no other writer can interleave.
    for chunk in _chunks(touched):
        conn.execute(
            f"DELETE FROM block_map WHERE volume_id = ? AND lba IN "
            f"({','.join('?' for _ in chunk)})",
            (volume.id, *chunk),
        )
    for physical_id, lost in lost_refs.items():
        conn.execute(
            "UPDATE physical_blocks SET refcount = refcount - ? WHERE id = ?",
            (lost, physical_id),
        )
    dead = conn.execute(
        "SELECT id FROM physical_blocks WHERE refcount <= 0"
    ).fetchall()
    for row in dead:
        conn.execute("DELETE FROM physical_blocks WHERE id = ?", (row["id"],))

    now = dbmod.utc_now()
    for lba, data in ops.writes:
        cur = conn.execute(
            "INSERT INTO physical_blocks(data, refcount, created_at) VALUES (?, 1, ?)",
            (sqlite3.Binary(data), now),
        )
        physical_id = int(cur.lastrowid)
        conn.execute(
            "INSERT INTO block_map(volume_id, lba, physical_block_id) "
            "VALUES (?, ?, ?)",
            (volume.id, lba, physical_id),
        )

    conn.execute(
        "UPDATE volumes SET version = version + 1, updated_at = ? WHERE id = ?",
        (now, volume.id),
    )
    new_version = int(
        conn.execute(
            "SELECT version FROM volumes WHERE id = ?", (volume.id,)
        ).fetchone()["version"]
    )
    stats = pool_stats(conn)
    return {
        "volume_id": volume.id,
        "version": new_version,
        "written_lbas": [lba for lba, _ in ops.writes],
        "freed_lbas": ops.frees,
        "allocated_blocks": allocated,
        "released_blocks": released,
        "pool": stats,
    }


def update_blocks(
    db_path: str,
    volume_id: int,
    *,
    writes: Iterable[tuple[int, bytes]],
    frees: Iterable[int],
) -> dict[str, Any]:
    writes = list(writes)
    frees = list(frees)
    if not writes and not frees:
        raise ApiError(422, "batch must contain at least one write or free")
    if len(writes) + len(frees) > MAX_BATCH_OPERATIONS:
        raise ApiError(
            413,
            f"batch has {len(writes) + len(frees)} operations; the limit is "
            f"{MAX_BATCH_OPERATIONS}",
        )

    seen: set[int] = set()
    for lba, _ in writes:
        lba = _validate_lba_int(lba, "lba")
        if lba in seen:
            raise ApiError(422, f"lba {lba} appears more than once in the batch")
        seen.add(lba)
    for lba in frees:
        lba = _validate_lba_int(lba, "lba")
        if lba in seen:
            raise ApiError(422, f"lba {lba} appears more than once in the batch")
        seen.add(lba)

    with dbmod.transaction(db_path, write=True) as conn:
        volume = Volume.from_row(_get_volume_row(conn, volume_id))
        for lba in seen:
            if lba >= volume.block_count:
                raise ApiError(
                    416,
                    f"lba {lba} is outside volume range [0, {volume.block_count})",
                )
        return _apply_batch_locked(
            conn, volume, BatchOperation(writes=writes, frees=frees)
        )


# ---------------------------------------------------------------------------
# startup configuration
# ---------------------------------------------------------------------------


def configure_pool(db_path: str, capacity: int) -> None:
    """Persist the configured physical pool size, refusing to shrink below use."""
    with dbmod.transaction(db_path, write=True) as conn:
        used = used_blocks(conn)
        current = conn.execute(
            "SELECT value FROM meta WHERE key = 'pool_blocks'"
        ).fetchone()
        if capacity < used:
            raise RuntimeError(
                f"POOL_BLOCKS={capacity} is smaller than the {used} physical "
                "blocks already in use; grow the pool or delete data first"
            )
        if current is None:
            conn.execute(
                "INSERT INTO meta(key, value) VALUES ('pool_blocks', ?)",
                (str(capacity),),
            )
        elif int(current["value"]) != capacity:
            conn.execute(
                "UPDATE meta SET value = ? WHERE key = 'pool_blocks'",
                (str(capacity),),
            )


def resolve_pool_blocks(explicit: int | None = None) -> int:
    if explicit is not None:
        return parse_pool_blocks(explicit)
    raw = os.environ.get("POOL_BLOCKS", str(dbmod.DEFAULT_POOL_BLOCKS))
    return parse_pool_blocks(raw)
