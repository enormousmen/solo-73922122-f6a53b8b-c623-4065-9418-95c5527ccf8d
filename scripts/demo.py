#!/usr/bin/env python3
"""End-to-end API demo against a running thin-volume service.

Usage:
    python scripts/demo.py [BASE_URL]

BASE_URL defaults to $THINVOL_URL or http://127.0.0.1:8000.

It requires an empty database (for example right after `docker compose up`):

1. creates a 16-block source volume and writes two 4 KiB blocks;
2. clones it and shows that the pool usage does not change (shared blocks);
3. overwrites a block in the clone and reads both volumes back to prove the
   copy-on-write split; frees a shared block to show the reference is dropped
   without returning capacity;
4. fills the remaining pool with a filler volume, then demonstrates a
   full-pool net-zero batch (free + write in one request) and a rejected
   over-capacity batch that leaves contents and usage untouched.

The script exits non-zero if any assertion fails and prints every request, so
its output doubles as an API example.
"""

from __future__ import annotations

import base64
import json
import os
import sys

import httpx

BASE = (
    sys.argv[1]
    if len(sys.argv) > 1
    else os.environ.get("THINVOL_URL", "http://127.0.0.1:8000")
).rstrip("/")
BLOCK = 4096

client = httpx.Client(base_url=BASE, timeout=30.0)


def show(label: str, response: httpx.Response, body: object | None = None) -> None:
    print(f"\n### {label}")
    print(f"{response.request.method} {response.request.url.path}")
    if response.request.content:
        try:
            print(json.dumps(json.loads(response.request.content), indent=2))
        except ValueError:
            print(response.request.content.decode(errors="replace")[:400])
    if body is None:
        print(f"-> {response.status_code}")
        try:
            print(json.dumps(response.json(), indent=2))
        except ValueError:
            print(f"<{len(response.content)} bytes>")
    else:
        print(f"-> {response.status_code} {body}")


def block_json(lba: int, fill: bytes) -> dict:
    return {"lba": lba, "data_b64": base64.b64encode(fill).decode()}


def read_block(vid: int, lba: int) -> bytes:
    r = client.get(f"/volumes/{vid}/blocks/{lba}")
    r.raise_for_status()
    return r.content


def write_blocks(vid: int, writes: list[tuple[int, bytes]], frees: list[int] = ()):
    return client.post(
        f"/volumes/{vid}/blocks",
        json={
            "writes": [block_json(lba, fill) for lba, fill in writes],
            "frees": list(frees),
        },
    )


def pool() -> dict:
    return client.get("/pool").json()


def expect(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def setup_clean_state() -> None:
    volumes = client.get("/volumes").json()["volumes"]
    if volumes:
        print(
            f"Found {len(volumes)} existing volume(s); this demo expects an "
            "empty database.  Run `docker compose down -v` first."
        )
        sys.exit(2)


def main() -> int:
    show("health", client.get("/health"))
    setup_clean_state()

    r = client.post("/volumes", json={"name": "base", "size_bytes": 16 * BLOCK})
    show("create a 16-block (64 KiB) empty volume", r)
    src = r.json()
    src_id = src["id"]
    expect(src["allocated_blocks"] == 0, "new volume must start fully thin")
    expect(pool()["used_blocks"] == 0, "empty volume must not consume the pool")

    r = client.get(f"/volumes/{src_id}/blocks/0")
    show("read an unallocated block -> 4 KiB of zeroes", r)
    expect(r.content == b"\x00" * BLOCK, "unallocated blocks must read zero")

    r = write_blocks(src_id, [(0, b"A" * BLOCK), (1, b"B" * BLOCK)])
    show("write blocks 0 and 1", r)
    expect(pool()["used_blocks"] == 2, "two written blocks -> two pool blocks")

    r = client.post(
        "/volumes", json={"name": "clone", "size_bytes": 16 * BLOCK, "clone_from": src_id}
    )
    show("clone the volume", r)
    clone = r.json()
    clone_id = clone["id"]
    expect(clone["cloned_from"] == src_id, "clone must record its parent")
    expect(clone["allocated_blocks"] == 2, "clone must map the parent's blocks")
    expect(pool()["used_blocks"] == 2, "clone must not allocate new pool blocks")
    print(
        f"\nPool after clone: used={pool()['used_blocks']} "
        f"(shared blocks cost one block regardless of reference count)"
    )

    r = write_blocks(clone_id, [(0, b"C" * BLOCK)])
    show("overwrite block 0 in the clone (copy-on-write)", r)
    expect(pool()["used_blocks"] == 3, "the clone's write allocates one block")
    expect(read_block(clone_id, 0) == b"C" * BLOCK, "clone sees its own write")
    expect(read_block(src_id, 0) == b"A" * BLOCK, "source must be unchanged")
    expect(read_block(src_id, 1) == b"B" * BLOCK, "shared block 1 still reads")

    r = client.post(f"/volumes/{clone_id}/blocks", json={"frees": [1]})
    show("free the clone's shared block 1 (last reference not yet gone)", r)
    expect(r.json()["released_blocks"] == 0, "source still references block 1")
    expect(pool()["used_blocks"] == 3, "usage unchanged after partial free")
    expect(read_block(src_id, 1) == b"B" * BLOCK, "source still reads block 1")

    # Fill whatever pool capacity remains with a filler volume, exactly as
    # many blocks as are free.  One batch, so it either fits exactly or fails.
    free = pool()["free_blocks"]
    if free > 0:
        r = client.post(
            "/volumes", json={"name": "filler", "size_bytes": free * BLOCK}
        )
        show(f"create a filler volume of exactly {free} block(s)", r)
        filler_id = r.json()["id"]
        r = write_blocks(
            filler_id, [(lba, bytes([0x10 + (lba % 200)]) * BLOCK) for lba in range(free)]
        )
        show("fill the pool in one batch", r)
        expect(r.status_code == 200, "filler writes should fit exactly")
    expect(pool()["free_blocks"] == 0, "pool should now be full")
    print(f"\nPool is full: {pool()}")

    # Full-pool net-zero swap: the clone's private block 0 is released and a
    # new block 10 allocated in the same request.  Writing block 10 alone
    # would not fit, but the batch's net usage is zero.
    r = write_blocks(clone_id, [(10, b"Z" * BLOCK)], frees=[0])
    show("full pool: free block 0 and write block 10 in one batch", r)
    expect(r.status_code == 200, "net-zero batch must succeed on a full pool")
    expect(pool()["free_blocks"] == 0, "usage stays at capacity")

    # Over-capacity batch: write two new blocks, free none.  Rejected whole.
    before_usage = pool()
    before_blocks = {lba: read_block(clone_id, lba) for lba in (13, 14)}
    # Blocks 0 and 2 are now holes in the clone, so these are fresh allocations.
    r = write_blocks(clone_id, [(13, b"X" * BLOCK), (14, b"Y" * BLOCK)])
    show("full pool: over-capacity batch is rejected atomically", r)
    expect(r.status_code == 409, "over-capacity batch must return 409")
    expect(r.json()["error"] == "pool_full", "expected pool_full error")
    expect(pool() == before_usage, "usage must be unchanged after rejection")
    expect(
        {lba: read_block(clone_id, lba) for lba in (13, 14)} == before_blocks,
        "contents must be unchanged after rejection",
    )

    r = client.get("/volumes")
    show("final volume inventory", r)
    print("\nAll demo assertions passed.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except httpx.HTTPError as exc:
        print(f"HTTP error: {exc}", file=sys.stderr)
        sys.exit(1)
    except AssertionError as exc:
        print(f"DEMO ASSERTION FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
