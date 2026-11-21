"""Functional tests for the thin volume API (in-process TestClient)."""

from __future__ import annotations

import base64
import os

import pytest
from fastapi.testclient import TestClient

BLOCK = 4096


def b64(fill: bytes | int, size: int = BLOCK) -> str:
    pattern = fill.to_bytes(1, "big") if isinstance(fill, int) else fill
    assert pattern
    raw = (pattern * (size // len(pattern) + 1))[:size]
    return base64.b64encode(raw).decode()


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "volumes.sqlite3"))
    monkeypatch.setenv("POOL_BLOCKS", "16")
    from app.main import app

    with TestClient(app) as c:
        yield c


def mkvol(client, name="v", size=8 * BLOCK, clone_from=None):
    body = {"name": name, "size_bytes": size}
    if clone_from is not None:
        body["clone_from"] = clone_from
    r = client.post("/volumes", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def write(client, vid, pairs):
    body = {"writes": [{"lba": lba, "data_b64": b64(fill)} for lba, fill in pairs]}
    return client.post(f"/volumes/{vid}/blocks", json=body)


def read(client, vid, lba):
    r = client.get(f"/volumes/{vid}/blocks/{lba}")
    assert r.status_code == 200, r.text
    return r.content


def pool(client):
    r = client.get("/pool")
    assert r.status_code == 200
    return r.json()


# ---------------------------------------------------------------------------
# creation and zero reads
# ---------------------------------------------------------------------------


def test_empty_volume_reads_zeros(client):
    vol = mkvol(client)
    assert vol["block_count"] == 8
    assert vol["allocated_blocks"] == 0
    assert pool(client)["used_blocks"] == 0
    for lba in (0, 3, 7):
        assert read(client, vol["id"], lba) == b"\x00" * BLOCK


def test_multi_block_read_includes_holes(client):
    vol = mkvol(client)
    assert write(client, vol["id"], [(2, b"\xaa")]).status_code == 200
    r = client.get(f"/volumes/{vol['id']}/blocks", params={"start": 1, "count": 3})
    assert r.status_code == 200
    assert r.headers["x-block-count"] == "3"
    data = r.content
    assert len(data) == 3 * BLOCK
    assert data[:BLOCK] == b"\x00" * BLOCK
    assert data[BLOCK : 2 * BLOCK] == b"\xaa" * BLOCK
    assert data[2 * BLOCK :] == b"\x00" * BLOCK


def test_rejects_bad_sizes_and_ranges(client):
    assert client.post(
        "/volumes", json={"name": "x", "size_bytes": 10}
    ).status_code == 422
    assert client.post(
        "/volumes", json={"name": "x", "size_bytes": 0}
    ).status_code == 422
    vol = mkvol(client)
    assert client.get(f"/volumes/{vol['id']}/blocks/8").status_code == 416
    assert write(client, vol["id"], [(8, b"\x01")]).status_code == 416
    short = base64.b64encode(b"short").decode()
    assert (
        client.post(
            f"/volumes/{vol['id']}/blocks",
            json={"writes": [{"lba": 0, "data_b64": short}]},
        ).status_code
        == 422
    )
    assert client.post(f"/volumes/{vol['id']}/blocks", json={}).status_code == 422
    assert (
        client.post(
            f"/volumes/{vol['id']}/blocks",
            json={"writes": [], "frees": [0]},
        ).status_code
        == 200
    )  # freeing a hole is a no-op


def test_duplicate_lba_in_batch_rejected(client):
    vol = mkvol(client)
    r = client.post(
        f"/volumes/{vol['id']}/blocks",
        json={
            "writes": [
                {"lba": 1, "data_b64": b64(b"\x01")},
                {"lba": 1, "data_b64": b64(b"\x02")},
            ]
        },
    )
    assert r.status_code == 422
    assert pool(client)["used_blocks"] == 0


def test_name_uniqueness(client):
    mkvol(client, name="dup")
    r = client.post("/volumes", json={"name": "dup", "size_bytes": BLOCK})
    assert r.status_code == 409


# ---------------------------------------------------------------------------
# clone sharing and copy-on-write
# ---------------------------------------------------------------------------


def test_clone_shares_blocks_and_writes_independently(client):
    src = mkvol(client, name="src")
    assert write(client, src["id"], [(0, b"\x11"), (1, b"\x12")]).status_code == 200
    assert pool(client)["used_blocks"] == 2

    clone = mkvol(client, name="clone", clone_from=src["id"])
    assert clone["cloned_from"] == src["id"]
    assert clone["allocated_blocks"] == 2
    # Sharing adds mappings, not physical blocks.
    assert pool(client)["used_blocks"] == 2
    assert read(client, clone["id"], 0) == b"\x11" * BLOCK
    assert read(client, clone["id"], 1) == b"\x12" * BLOCK

    # Overwrite one shared block in the clone: exactly one new physical block.
    assert write(client, clone["id"], [(0, b"\x21")]).status_code == 200
    assert pool(client)["used_blocks"] == 3
    assert read(client, clone["id"], 0) == b"\x21" * BLOCK
    assert read(client, src["id"], 0) == b"\x11" * BLOCK  # source untouched
    assert read(client, src["id"], 1) == b"\x12" * BLOCK
    # The shared block 1 is still shared.
    assert pool(client)["used_blocks"] == 3

    # Freeing the clone's shared block only drops its reference.
    r = client.post(f"/volumes/{clone['id']}/blocks", json={"frees": [1]})
    assert r.status_code == 200
    assert r.json()["released_blocks"] == 0
    assert pool(client)["used_blocks"] == 3
    assert read(client, clone["id"], 1) == b"\x00" * BLOCK
    assert read(client, src["id"], 1) == b"\x12" * BLOCK

    # Freeing the last reference releases the block.
    r = client.post(f"/volumes/{src['id']}/blocks", json={"frees": [1]})
    assert r.json()["released_blocks"] == 1
    assert pool(client)["used_blocks"] == 2


def test_delete_clone_keeps_source_and_releases_only_private_blocks(client):
    src = mkvol(client, name="src")
    write(client, src["id"], [(0, b"\x01"), (1, b"\x02")])
    clone = mkvol(client, name="clone", clone_from=src["id"])
    write(client, clone["id"], [(2, b"\x03")])
    assert pool(client)["used_blocks"] == 3

    r = client.delete(f"/volumes/{clone['id']}")
    assert r.status_code == 200
    assert r.json()["released_blocks"] == 1  # only its private block
    assert pool(client)["used_blocks"] == 2
    assert read(client, src["id"], 0) == b"\x01" * BLOCK


def test_clone_requires_matching_size(client):
    src = mkvol(client, name="src", size=4 * BLOCK)
    r = client.post(
        "/volumes", json={"name": "bad", "size_bytes": 8 * BLOCK, "clone_from": src["id"]}
    )
    assert r.status_code == 409


# ---------------------------------------------------------------------------
# batch semantics and capacity
# ---------------------------------------------------------------------------


def test_batch_writes_and_frees_multiple_blocks(client):
    vol = mkvol(client)
    write(client, vol["id"], [(0, b"\x01"), (1, b"\x02"), (2, b"\x03")])
    r = client.post(
        f"/volumes/{vol['id']}/blocks",
        json={
            "writes": [{"lba": 0, "data_b64": b64(b"\x09")}],
            "frees": [1, 2],
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert body["written_lbas"] == [0]
    assert body["freed_lbas"] == [1, 2]
    # Two freed mappings plus the replaced block at LBA 0.
    assert body["released_blocks"] == 3
    assert pool(client)["used_blocks"] == 1
    assert read(client, vol["id"], 0) == b"\x09" * BLOCK
    assert read(client, vol["id"], 1) == b"\x00" * BLOCK


def test_full_pool_batch_that_frees_enough_succeeds(client):
    # Shrink to a 2-block pool for the test.
    from app import storage

    db_path = os.environ["DB_PATH"]
    storage.configure_pool(db_path, 2)

    vol = mkvol(client)
    write(client, vol["id"], [(0, b"\x01"), (1, b"\x02")])
    assert pool(client)["used_blocks"] == 2

    # Pool is full; replacing two blocks with two others is a net-zero batch.
    r = client.post(
        f"/volumes/{vol['id']}/blocks",
        json={
            "writes": [
                {"lba": 2, "data_b64": b64(b"\x0a")},
                {"lba": 3, "data_b64": b64(b"\x0b")},
            ],
            "frees": [0, 1],
        },
    )
    assert r.status_code == 200, r.text
    assert pool(client)["used_blocks"] == 2
    assert read(client, vol["id"], 2) == b"\x0a" * BLOCK
    assert read(client, vol["id"], 3) == b"\x0b" * BLOCK
    assert read(client, vol["id"], 0) == b"\x00" * BLOCK


def test_over_capacity_batch_is_rejected_atomically(client):
    from app import storage

    storage.configure_pool(os.environ["DB_PATH"], 2)
    vol = mkvol(client)
    write(client, vol["id"], [(0, b"\x01")])
    assert pool(client)["used_blocks"] == 1

    r = client.post(
        f"/volumes/{vol['id']}/blocks",
        json={
            "writes": [
                {"lba": 1, "data_b64": b64(b"\x02")},
                {"lba": 2, "data_b64": b64(b"\x03")},
            ]
        },
    )
    assert r.status_code == 409, r.text
    body = r.json()
    assert body["error"] == "pool_full"
    assert body["capacity_blocks"] == 2
    assert body["used_blocks"] == 1
    assert body["over_by_blocks"] == 1

    # Contents and usage unchanged: neither write was applied.
    assert pool(client)["used_blocks"] == 1
    assert read(client, vol["id"], 1) == b"\x00" * BLOCK
    assert read(client, vol["id"], 2) == b"\x00" * BLOCK
    assert read(client, vol["id"], 0) == b"\x01" * BLOCK
    assert client.get(f"/volumes/{vol['id']}").json()["version"] == 1

    # A free-only batch still works when the pool is full.
    assert client.post(
        f"/volumes/{vol['id']}/blocks", json={"frees": [0]}
    ).status_code == 200
    assert pool(client)["used_blocks"] == 0


def test_zero_capacity_pool_rejects_any_clone_write(client):
    from app import storage

    storage.configure_pool(os.environ["DB_PATH"], 0)
    vol = mkvol(client)
    assert write(client, vol["id"], [(0, b"\x01")]).status_code == 409
    assert pool(client)["used_blocks"] == 0


def test_startup_refuses_to_shrink_pool_below_usage(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "v.sqlite3"))
    monkeypatch.setenv("POOL_BLOCKS", "4")
    from app.main import app

    with TestClient(app) as c:
        vol = mkvol(c)
        write(c, vol["id"], [(0, b"\x01"), (1, b"\x02")])

    monkeypatch.setenv("POOL_BLOCKS", "1")
    with pytest.raises(RuntimeError), TestClient(app):
        pass
