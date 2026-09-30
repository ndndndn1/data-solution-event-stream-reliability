"""Optional FastAPI boundary. Run with uvicorn and TELEMETRY_DB."""
from __future__ import annotations

import json
import os
import sqlite3
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request
from starlette.concurrency import run_in_threadpool

from .telemetry import EventConflict, TelemetryStore

MAX_BODY_BYTES = 128 * 1024


def create_app(db_path: str | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.store = await run_in_threadpool(TelemetryStore, db_path or os.environ.get("TELEMETRY_DB", "telemetry.sqlite3"))
        yield

    app = FastAPI(title="Durable factory telemetry demo", version="0.2.0", lifespan=lifespan)

    async def database_call(function, *args, **kwargs):
        try:
            return await run_in_threadpool(function, *args, **kwargs)
        except EventConflict as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        except sqlite3.Error as exc:
            raise HTTPException(503, "Storage unavailable; retry the unchanged batch", headers={"Retry-After": "1"}) from exc

    @app.get("/readyz")
    async def ready(request: Request):
        await database_call(request.app.state.store.ready)
        return {"status": "ready"}

    @app.post("/v1/telemetry")
    async def ingest(request: Request):
        if request.headers.get("content-type", "").split(";")[0].strip().lower() != "application/json":
            raise HTTPException(415, "Content-Type must be application/json")
        body = bytearray()
        async for chunk in request.stream():
            if len(body) + len(chunk) > MAX_BODY_BYTES:
                raise HTTPException(413, "Request exceeds 128 KiB")
            body.extend(chunk)
        try:
            def reject_constant(value):
                raise ValueError(f"non-finite JSON constant: {value}")
            def unique_keys(pairs):
                result = {}
                for key, value in pairs:
                    if key in result:
                        raise ValueError("duplicate JSON object key")
                    result[key] = value
                return result
            records = json.loads(body, parse_constant=reject_constant, object_pairs_hook=unique_keys)
        except (ValueError, UnicodeDecodeError, RecursionError) as exc:
            raise HTTPException(422, "Invalid JSON") from exc
        return await database_call(request.app.state.store.ingest, records)

    @app.get("/v1/factories/{factory_id}/equipment/{equipment_id}/telemetry")
    async def retrieve(request: Request, factory_id: str, equipment_id: str,
                       metric: str = Query(...), limit: int = Query(100, ge=1, le=1000)):
        return await database_call(request.app.state.store.query, factory_id, equipment_id, metric, limit=limit)

    return app


app = create_app()
