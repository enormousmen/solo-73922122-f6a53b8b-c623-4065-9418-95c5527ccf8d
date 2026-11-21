"""Concurrency tests against a real HTTP server.

The server runs in a background thread on a real socket.  FastAPI executes the
synchronous endpoints in a thread pool, so these tests produce genuinely
concurrent requests, each with its own SQLite connection.  That is what
exercises the ``BEGIN IMMEDIATE`` serialization and WAL snapshot isolation.

Each test gets a fresh database and a fresh server so pool capacity can be
tailored to the scenario.
"""

from __future__ import annotations

import base64
import os
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import httpx
import uvicorn

BLOCK = 4096


def b64(fill: bytes) -> str:
    return base64.b64encode((fill * (BLOCK // len(fill) + 1))[:BLOCK]).decode()


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@contextmanager
def serve(tmp_path, pool_blocks: int):
    from app.main import app

    previous = {k: os.environ.get(k) for k in ("DB_PATH", "POOL_BLOCKS")}
    os.environ["DB_PATH"] = str(tmp_path / "volumes.sqlite3")
    os.environ["POOL_BLOCKS"] = str(pool_blocks)

    config = uvicorn.Config(
        app, host="127.0.0.1", port=free_port(), log_level="warning"
    )
    srv = uvicorn.Server(config)
    thread = threading.Thread(target=srv.run, name="uvicorn-test", daemon=True)
    thread.start()

    deadline = time.time() + 30
    while time.time() < deadline:
        if srv.started:
            break
        if not thread.is_alive():
            raise RuntimeError("uvicorn thread exited before startup")
        time.sleep(0.05)
    else:
        raise RuntimeError("server did not start")

    try:
        with httpx.Client(
            base_url=f"http://127.0.0.1:{config.port}", timeout=30.0
        ) as client:
            yield client
    finally:
        srv.should_exit = True
        thread.join(timeout=15)
        assert not thread.is_alive(), "server did not shut down"
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def create(client, name, blocks, clone_from=None):
    body = {"name": name, "size_bytes": blocks * BLOCK}
    if clone_from is not None:
        body["clone_from"] = clone_from
    r = client.post("/volumes", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def write(client, vid, lba, fill, frees=()):
    return client.post(
        f"/volumes/{vid}/blocks",
        json={
            "writes": [{"lba": lba, "data_b64": b64(fill)}],
            "frees": list(frees),
        },
    )


def test_concurrent_writers_never_overcommit(tmp_path):
    with serve(tmp_path, 8) as client:
        vol = create(client, "overcommit", 64)

        def job(i):
            return write(client, vol["id"], i, b"\x5a")

        with ThreadPoolExecutor(max_workers=20) as pool:
            responses = list(pool.map(job, range(20)))

        statuses = [r.status_code for r in responses]
        assert statuses.count(200) == 8, statuses
        assert statuses.count(409) == 12, statuses
        for r in responses:
            if r.status_code == 409:
                assert r.json()["error"] == "pool_full"
        assert client.get("/pool").json()["used_blocks"] == 8

        allocated = [
            lba
            for lba in range(20)
            if client.get(f"/volumes/{vol['id']}/blocks/{lba}").content
            == b"\x5a" * BLOCK
        ]
        assert len(allocated) == 8


def test_full_pool_net_zero_batches_are_all_accepted(tmp_path):
    with serve(tmp_path, 8) as client:
        vol = create(client, "netzero", 32)
        for lba in range(8):
            assert write(client, vol["id"], lba, b"\x01").status_code == 200
        assert client.get("/pool").json()["used_blocks"] == 8

        def job(i):
            return write(client, vol["id"], 10 + i, b"\x02", frees=[i])

        with ThreadPoolExecutor(max_workers=8) as pool:
            responses = list(pool.map(job, range(8)))
        assert all(r.status_code == 200 for r in responses), [
            r.text for r in responses
        ]
        assert client.get("/pool").json()["used_blocks"] == 8

        # No free -> the pool is still full -> whole batch rejected.
        r = write(client, vol["id"], 9, b"\x03")
        assert r.status_code == 409
        assert client.get("/pool").json()["used_blocks"] == 8
        assert client.get(f"/volumes/{vol['id']}/blocks/9").content == b"\x00" * BLOCK


def test_clone_readers_never_see_a_torn_batch(tmp_path):
    # 8 shared blocks plus room for one full replacement generation of 8.
    with serve(tmp_path, 16) as client:
        src = create(client, "snap-src", 8)
        clone = create(client, "snap-clone", 8, clone_from=src["id"])
        for lba in range(8):
            assert write(client, src["id"], lba, b"\x00").status_code == 200
        assert client.get("/pool").json()["used_blocks"] == 8

        stop = threading.Event()
        violations: list = []

        def reader():
            while not stop.is_set():
                r = client.get(
                    f"/volumes/{clone['id']}/blocks",
                    params={"start": 0, "count": 8},
                )
                if r.status_code != 200:
                    violations.append(("status", r.status_code, r.text))
                    return
                data = r.content
                first = data[:BLOCK]
                if any(
                    data[i : i + BLOCK] != first
                    for i in range(0, len(data), BLOCK)
                ):
                    violations.append(("torn", data[:16]))

        with ThreadPoolExecutor(max_workers=3) as pool:
            readers = [pool.submit(reader) for _ in range(3)]
            try:
                for i in range(30):
                    fill = b"\xaa" if i % 2 == 0 else b"\xbb"
                    r = client.post(
                        f"/volumes/{src['id']}/blocks",
                        json={
                            "writes": [
                                {"lba": lba, "data_b64": b64(fill)}
                                for lba in range(8)
                            ]
                        },
                    )
                    assert r.status_code == 200, r.text
            finally:
                stop.set()
            for f in readers:
                f.result()

        assert not violations, violations[:3]
        # The clone still reads its original generation: all zeroes.
        clone_data = client.get(
            f"/volumes/{clone['id']}/blocks", params={"start": 0, "count": 8}
        ).content
        assert clone_data == b"\x00" * (8 * BLOCK)
        src_data = client.get(
            f"/volumes/{src['id']}/blocks", params={"start": 0, "count": 8}
        ).content
        assert {
            src_data[i : i + BLOCK] for i in range(0, len(src_data), BLOCK)
        } == {b"\xbb" * BLOCK}
