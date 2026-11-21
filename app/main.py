"""FastAPI application for a local thin-provisioned block volume service."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, ConfigDict, Field, field_validator

from . import db as dbmod
from . import storage
from .db import BLOCK_SIZE, init_db

log = logging.getLogger("thinvol")

DEFAULT_DB_PATH = "/data/volumes.sqlite3"


class BlockWrite(BaseModel):
    model_config = ConfigDict(extra="forbid")

    lba: int = Field(..., ge=0)
    data_b64: str

    @field_validator("data_b64")
    @classmethod
    def _check_base64(cls, value: str) -> str:
        storage.decode_block(value, "data_b64")
        return value


class BatchUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    writes: list[BlockWrite] = Field(default_factory=list)
    frees: list[int] = Field(default_factory=list)

    @field_validator("frees")
    @classmethod
    def _check_frees(cls, value: list[int]) -> list[int]:
        for lba in value:
            if lba < 0:
                raise ValueError("frees entries must be >= 0")
        return value


class CreateVolume(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    size_bytes: int = Field(..., gt=0)
    clone_from: int | None = None


def _db_path() -> str:
    return os.environ.get("DB_PATH", DEFAULT_DB_PATH)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db_path = _db_path()
    init_db(db_path)
    capacity = storage.resolve_pool_blocks()
    storage.configure_pool(db_path, capacity)
    log.info("thin-volume service ready: db=%s pool_blocks=%d", db_path, capacity)
    yield


app = FastAPI(
    title="Thin Volume Service",
    version="1.0.0",
    root_path=os.environ.get("ROOT_PATH", ""),
    docs_url="/docs",
    redoc_url=None,
    description=(
        "Local thin-provisioned 4 KiB block volumes backed by SQLite. "
        "Volumes can be created empty or cloned from an existing volume; "
        "clones share physical blocks copy-on-write and unallocated blocks "
        "read as zeroes. Batching is transactional: a batch that exceeds pool "
        "capacity is rejected entirely, leaving contents and usage unchanged."
    ),
    lifespan=lifespan,
)


@app.exception_handler(storage.ApiError)
async def _api_error_handler(request: Request, exc: storage.ApiError) -> JSONResponse:
    status = exc.status_code
    if isinstance(exc.detail, dict):
        content = {"status": status}
        content.update(exc.detail)
    else:
        content = {"error": "request_failed", "status": status, "detail": exc.detail}
    return JSONResponse(status_code=status, content=jsonable_encoder(content))


@app.exception_handler(dbmod.DatabaseBusy)
async def _busy_error_handler(
    request: Request, exc: dbmod.DatabaseBusy
) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={
            "error": "database_busy",
            "status": 503,
            "detail": (
                "another writer held the SQLite lock longer than the busy "
                "timeout; retry the request"
            ),
        },
        headers={"Retry-After": "1"},
    )


@app.exception_handler(RequestValidationError)
async def _validation_error_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={
            "error": "validation_error",
            "status": 422,
            "detail": jsonable_encoder(exc.errors()),
        },
    )


@app.exception_handler(Exception)
async def _unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
    log.exception("unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={"error": "internal_error", "status": 500, "detail": str(exc)},
    )


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "block_size": BLOCK_SIZE}


@app.get("/pool")
def get_pool() -> dict[str, Any]:
    with dbmod.transaction(_db_path(), write=False) as conn:
        return storage.pool_stats(conn)


@app.get("/volumes")
def list_volumes() -> dict[str, Any]:
    return {"volumes": storage.list_volumes(_db_path())}


@app.post("/volumes", status_code=201)
def create_volume(body: CreateVolume) -> dict[str, Any]:
    return storage.create_volume(
        _db_path(),
        name=body.name,
        size_bytes=body.size_bytes,
        clone_from=body.clone_from,
    )


@app.get("/volumes/{volume_id}")
def get_volume(volume_id: int) -> dict[str, Any]:
    return storage.get_volume(_db_path(), volume_id)


@app.delete("/volumes/{volume_id}")
def delete_volume(volume_id: int) -> dict[str, Any]:
    return storage.delete_volume(_db_path(), volume_id)


@app.post("/volumes/{volume_id}/blocks")
def update_blocks(volume_id: int, body: BatchUpdate) -> dict[str, Any]:
    writes = [
        (item.lba, storage.decode_block(item.data_b64, "data_b64"))
        for item in body.writes
    ]
    return storage.update_blocks(
        _db_path(), volume_id, writes=writes, frees=body.frees
    )


@app.get("/volumes/{volume_id}/blocks/{lba}", response_class=Response)
def read_block(volume_id: int, lba: int) -> Response:
    data, volume = storage.read_block(_db_path(), volume_id, lba)
    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={
            "X-Volume-Id": str(volume.id),
            "X-Block-Size": str(volume.block_size),
            "X-Volume-Version": str(volume.version),
            "Content-Length": str(len(data)),
        },
    )


@app.get("/volumes/{volume_id}/blocks", response_class=Response)
def read_blocks(
    volume_id: int,
    start: int = Query(0, ge=0),
    count: int = Query(1, ge=1, le=storage.MAX_READ_BLOCKS),
) -> Response:
    blocks, volume = storage.read_blocks(
        _db_path(), volume_id, start=start, count=count
    )
    return Response(
        content=b"".join(blocks),
        media_type="application/octet-stream",
        headers={
            "X-Volume-Id": str(volume.id),
            "X-Block-Size": str(volume.block_size),
            "X-Volume-Version": str(volume.version),
            "X-Block-Count": str(len(blocks)),
            "X-Start-Lba": str(start),
            "Content-Length": str(len(blocks) * volume.block_size),
        },
    )
