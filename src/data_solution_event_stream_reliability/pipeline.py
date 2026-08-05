from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True, slots=True)
class PipelineResult:
    accepted: tuple[dict[str, Any], ...]
    dead_letter: tuple[dict[str, Any], ...]
    duplicates: int


class ReliablePipeline:
    """Small deterministic ingestion, validation, deduplication, and DLQ slice."""

    def process(self, records: Iterable[dict[str, Any]]) -> PipelineResult:
        accepted: list[dict[str, Any]] = []
        dead_letter: list[dict[str, Any]] = []
        seen: set[str] = set()
        duplicates = 0
        for record in records:
            record_id = record.get("id")
            payload = record.get("payload")
            if not isinstance(record_id, str) or not record_id or payload is None:
                dead_letter.append({"record": dict(record), "reason": "SCHEMA_VALIDATION_FAILED"})
                continue
            if record_id in seen:
                duplicates += 1
                continue
            seen.add(record_id)
            accepted.append(dict(record))
        return PipelineResult(tuple(accepted), tuple(dead_letter), duplicates)
