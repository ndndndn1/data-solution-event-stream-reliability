"""Single-host, transactional telemetry storage; independent of the simulator."""
from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

MAX_BATCH = 100
IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
TIMESTAMP = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])\Z")
UNITS = {"temperature": "degC", "vibration": "mm/s", "pressure": "kPa"}


def identity(value: object) -> str:
    if not isinstance(value, str) or not IDENTITY.fullmatch(value):
        raise ValueError("identity must be 1-64 ASCII letters, digits, dots, underscores or hyphens; start with a letter or digit")
    return value


def timestamp(value: object) -> str:
    if not isinstance(value, str) or not TIMESTAMP.fullmatch(value):
        raise ValueError("observed_at requires YYYY-MM-DDTHH:MM:SS[.ffffff]Z or +/-HH:MM")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("timezone required")
        return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds")
    except (ValueError, OverflowError) as exc:
        raise ValueError("observed_at must be an ISO 8601 string with timezone") from exc


@dataclass(frozen=True)
class TelemetryEvent:
    factory_id: str
    event_id: str
    equipment_id: str
    observed_at: str
    metric: str
    value: float
    unit: str

    @classmethod
    def parse(cls, raw: object) -> TelemetryEvent:
        fields = set(cls.__dataclass_fields__)
        if not isinstance(raw, dict) or set(raw) != fields:
            raise ValueError("event must contain exactly: " + ", ".join(sorted(fields)))
        for field in ("factory_id", "event_id", "equipment_id"):
            identity(raw[field])
        metric, unit = raw["metric"], raw["unit"]
        if not isinstance(metric, str) or metric not in UNITS or unit != UNITS[metric]:
            raise ValueError("metric/unit must be temperature/degC, vibration/mm/s or pressure/kPa")
        value = raw["value"]
        if type(value) not in (int, float):
            raise ValueError("value must be a finite JSON number, not a boolean or string")
        try:
            value = float(value)
        except OverflowError as exc:
            raise ValueError("value must be finite and within +/-1e100") from exc
        if not math.isfinite(value) or abs(value) > 1e100:
            raise ValueError("value must be finite and within +/-1e100")
        if type(raw["value"]) is int and int(value) != raw["value"]:
            raise ValueError("integer value must be exactly representable as binary64")
        # Normalize equivalent numeric and timezone representations before hashing.
        value = 0.0 if value == 0 else value
        return cls(raw["factory_id"], raw["event_id"], raw["equipment_id"],
                   timestamp(raw["observed_at"]), metric, value, unit)

    def payload(self) -> dict:
        return dict(self.__dict__)


class EventConflict(ValueError):
    """An existing factory/event key was reused with a different payload."""


class TelemetryStore:
    def __init__(self, path: str | Path):
        self.path = str(path)
        if not self.path or self.path == ":memory:":
            raise ValueError("use a filesystem database for durable storage")
        with closing(self._connect()) as db:
            db.execute("PRAGMA journal_mode=WAL")
            with db:
                db.execute("""CREATE TABLE IF NOT EXISTS telemetry (
                    factory_id TEXT NOT NULL, event_id TEXT NOT NULL,
                    equipment_id TEXT NOT NULL, observed_at TEXT NOT NULL,
                    metric TEXT NOT NULL, value REAL NOT NULL, unit TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    PRIMARY KEY (factory_id, event_id))""")
                db.execute("""CREATE INDEX IF NOT EXISTS telemetry_lookup
                    ON telemetry(factory_id, equipment_id, metric, observed_at, event_id)""")

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA synchronous=FULL")
        return db

    def ready(self) -> bool:
        with closing(self._connect()) as db:
            db.execute("SELECT factory_id FROM telemetry LIMIT 1").fetchone()
        return True

    def ingest(self, records: object, *, before_commit: Callable[[], None] | None = None) -> dict:
        if not isinstance(records, list) or not 1 <= len(records) <= MAX_BATCH:
            raise ValueError(f"batch must contain 1-{MAX_BATCH} events")
        events = [TelemetryEvent.parse(record) for record in records]
        inserted = duplicates = 0
        # Fresh connections are never shared between request threads. BEGIN IMMEDIATE
        # serializes competing writers before checking the unique key.
        with closing(self._connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            for event in events:
                payload = event.payload()
                digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
                old = db.execute("SELECT payload_hash FROM telemetry WHERE factory_id=? AND event_id=?",
                                 (event.factory_id, event.event_id)).fetchone()
                if old:
                    if old["payload_hash"] != digest:
                        raise EventConflict(f"payload conflict for {event.factory_id}/{event.event_id}")
                    duplicates += 1
                else:
                    db.execute("""INSERT INTO telemetry
                        (factory_id, event_id, equipment_id, observed_at, metric, value, unit, payload_hash)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                               (*payload.values(), digest))
                    inserted += 1
            if before_commit:
                before_commit()
        return {"inserted": inserted, "duplicates": duplicates}

    def query(self, factory_id: str, equipment_id: str, metric: str, *, limit: int = 100) -> dict:
        identity(factory_id)
        identity(equipment_id)
        if metric not in UNITS or type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("valid metric and limit between 1 and 1000 required")
        with closing(self._connect()) as db:
            rows = db.execute("""SELECT factory_id, event_id, equipment_id, observed_at,
                metric, value, unit FROM telemetry
                WHERE factory_id=? AND equipment_id=? AND metric=?
                ORDER BY observed_at, event_id LIMIT ?""",
                (factory_id, equipment_id, metric, limit + 1)).fetchall()
        selected = [dict(row) for row in rows[:limit]]
        values = [row["value"] for row in selected]
        return {"events": selected, "has_more": len(rows) > limit,
                "aggregate": {"scope": "returned_events", "count": len(values),
                              "unit": UNITS[metric],
                              "min": min(values) if values else None,
                              "max": max(values) if values else None,
                              "mean": math.fsum(values) / len(values) if values else None}}
