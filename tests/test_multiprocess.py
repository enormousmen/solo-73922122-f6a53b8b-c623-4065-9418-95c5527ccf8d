"""Cross-process capacity test.

Each process opens its own SQLite connection and races to allocate pool blocks.
This is the same locking path uvicorn workers use, without needing a manager
process that this sandbox may refuse to signal.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

BLOCK = 4096
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CHILD = r"""
import base64, json, os, sys
sys.path.insert(0, os.environ["PROJECT_ROOT"])
from app import storage

lba = int(sys.argv[1])
try:
    result = storage.update_blocks(
        os.environ["DB_PATH"],
        int(os.environ["VOLUME_ID"]),
        writes=[(lba, b"\x5a" * 4096)],
        frees=[],
    )
    print(json.dumps({"ok": True, "used": result["pool"]["used_blocks"]}))
except storage.ApiError as exc:
    print(json.dumps({"ok": False, "status": exc.status_code, "error": exc.detail}))
"""

SETUP = r"""
import sys
sys.path.insert(0, sys.argv[1])
from app import storage
from app.db import init_db

db = sys.argv[2]
init_db(db)
storage.configure_pool(db, 6)
volume = storage.create_volume(db, name="race", size_bytes=int(sys.argv[3]))
print(volume["id"])
"""

CHECK = r"""
import json, sys
sys.path.insert(0, sys.argv[1])
from app import db, storage

with db.transaction(sys.argv[2], write=False) as conn:
    print(json.dumps(storage.pool_stats(conn)))
"""


def run_script(script: str, *args: str, env: dict) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", script, *args],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_only_capacity_writers_succeed_across_processes(tmp_path):
    db = str(tmp_path / "v.sqlite3")
    env = dict(
        os.environ,
        DB_PATH=db,
        POOL_BLOCKS="6",
        PROJECT_ROOT=ROOT,
        PYTHONPATH=ROOT,
    )
    out = run_script(SETUP, ROOT, db, str(32 * BLOCK), env=env)
    assert out.returncode == 0, out.stderr
    volume_id = int(out.stdout.strip().splitlines()[-1])

    env["VOLUME_ID"] = str(volume_id)
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", CHILD, str(lba)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for lba in range(20)
    ]
    results = []
    for proc in procs:
        stdout, stderr = proc.communicate(timeout=120)
        assert proc.returncode == 0, stderr
        results.append(json.loads(stdout.strip().splitlines()[-1]))

    successes = [r for r in results if r["ok"]]
    failures = [r for r in results if not r["ok"]]
    assert len(successes) == 6, results
    assert len(failures) == 14, results
    assert all(r["status"] == 409 for r in failures), failures
    assert all(r["error"]["error"] == "pool_full" for r in failures)
    # Usage reported by each success is a snapshot at its own commit time.
    assert max(r["used"] for r in successes) == 6
    assert all(1 <= r["used"] <= 6 for r in successes)

    out = run_script(CHECK, ROOT, db, env=env)
    assert out.returncode == 0, out.stderr
    assert json.loads(out.stdout)["used_blocks"] == 6
