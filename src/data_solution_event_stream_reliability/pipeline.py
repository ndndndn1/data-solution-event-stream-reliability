from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


Record = Mapping[str, Any]
Processor = Callable[[dict[str, Any]], None]


@dataclass(frozen=True, slots=True)
class PipelineMetrics:
    """Per-run or cumulative counters exposed by the pipeline."""

    received: int = 0
    accepted: int = 0
    duplicates: int = 0
    validation_failures: int = 0
    retry_attempts: int = 0
    processing_failures: int = 0
    late_records: int = 0
    replayed: int = 0
    recovered: int = 0

    def __add__(self, other: PipelineMetrics) -> PipelineMetrics:
        if not isinstance(other, PipelineMetrics):
            return NotImplemented
        return PipelineMetrics(
            received=self.received + other.received,
            accepted=self.accepted + other.accepted,
            duplicates=self.duplicates + other.duplicates,
            validation_failures=(
                self.validation_failures + other.validation_failures
            ),
            retry_attempts=self.retry_attempts + other.retry_attempts,
            processing_failures=(
                self.processing_failures + other.processing_failures
            ),
            late_records=self.late_records + other.late_records,
            replayed=self.replayed + other.replayed,
            recovered=self.recovered + other.recovered,
        )


@dataclass(frozen=True, slots=True)
class PipelineResult:
    accepted: tuple[dict[str, Any], ...]
    dead_letter: tuple[dict[str, Any], ...]
    duplicates: int
    metrics: PipelineMetrics = PipelineMetrics()


class ReliablePipeline:
    """Deterministic in-memory slice of a reliable event consumer.

    A processor represents the downstream write. It may raise an exception to
    request a retry. An event ID is remembered only after that write succeeds,
    which makes a later delivery safe to retry.
    """

    def __init__(
        self,
        processor: Processor | None = None,
        *,
        max_retries: int = 2,
        max_lateness: timedelta | None = timedelta(minutes=5),
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be zero or greater")
        if max_lateness is not None and max_lateness < timedelta(0):
            raise ValueError("max_lateness must be zero or greater")

        self._processor = processor if processor is not None else (lambda _record: None)
        self._max_retries = max_retries
        self._max_lateness = max_lateness
        self._clock = clock if clock is not None else (lambda: datetime.now(timezone.utc))
        self._processed_ids: set[str] = set()
        self._metrics = PipelineMetrics()

    @property
    def metrics(self) -> PipelineMetrics:
        """Return cumulative metrics for this pipeline instance."""

        return self._metrics

    def process(self, records: Iterable[Record]) -> PipelineResult:
        """Validate and deliver records, retrying transient processor errors.

        Records must support deepcopy (JSON-like payloads are recommended).
        Non-copyable objects raise rather than silently sharing mutable state.
        Each processor attempt receives an independent snapshot.
        """

        return self._process(records, replayed=False)

    def replay(self, dead_letter: Iterable[Mapping[str, Any]]) -> PipelineResult:
        """Reprocess records emitted by a previous result's dead-letter queue."""

        records: list[Any] = []
        for entry in dead_letter:
            if not isinstance(entry, Mapping) or "record" not in entry:
                raise ValueError("each dead-letter entry must contain 'record'")
            records.append(entry["record"])
        return self._process(records, replayed=True)

    def _process(
        self, records: Iterable[Record], *, replayed: bool
    ) -> PipelineResult:
        accepted: list[dict[str, Any]] = []
        dead_letter: list[dict[str, Any]] = []
        batch_ids: set[str] = set()
        counters = {
            "received": 0,
            "duplicates": 0,
            "validation_failures": 0,
            "retry_attempts": 0,
            "processing_failures": 0,
            "late_records": 0,
        }

        for raw_record in records:
            counters["received"] += 1
            record, validation_error = self._validate(raw_record)
            if validation_error is not None:
                counters["validation_failures"] += 1
                dead_letter.append(
                    {
                        "record": self._snapshot(raw_record),
                        "reason": "SCHEMA_VALIDATION_FAILED",
                        "error": validation_error,
                        "attempts": 0,
                    }
                )
                continue

            assert record is not None
            record_id = record["id"]
            if record_id in batch_ids or record_id in self._processed_ids:
                counters["duplicates"] += 1
                continue
            batch_ids.add(record_id)

            if self._is_late(record):
                counters["late_records"] += 1

            attempts = 0
            while True:
                attempts += 1
                try:
                    self._processor(deepcopy(record))
                except Exception as error:
                    if attempts <= self._max_retries:
                        counters["retry_attempts"] += 1
                        continue
                    counters["processing_failures"] += 1
                    dead_letter.append(
                        {
                            "record": deepcopy(record),
                            "reason": "PROCESSING_FAILED",
                            "error": f"{type(error).__name__}: {error}",
                            "attempts": attempts,
                        }
                    )
                    break
                else:
                    self._processed_ids.add(record_id)
                    accepted.append(deepcopy(record))
                    break

        run_metrics = PipelineMetrics(
            received=counters["received"],
            accepted=len(accepted),
            duplicates=counters["duplicates"],
            validation_failures=counters["validation_failures"],
            retry_attempts=counters["retry_attempts"],
            processing_failures=counters["processing_failures"],
            late_records=counters["late_records"],
            replayed=counters["received"] if replayed else 0,
            recovered=len(accepted) if replayed else 0,
        )
        self._metrics = self._metrics + run_metrics
        return PipelineResult(
            accepted=tuple(accepted),
            dead_letter=tuple(dead_letter),
            duplicates=run_metrics.duplicates,
            metrics=run_metrics,
        )

    def _validate(
        self, raw_record: Any
    ) -> tuple[dict[str, Any] | None, str | None]:
        if not isinstance(raw_record, Mapping):
            return None, "record must be a mapping"

        record = deepcopy(dict(raw_record))
        record_id = record.get("id")
        if not isinstance(record_id, str) or not record_id.strip():
            return None, "id must be a non-empty string"
        if "payload" not in record or record["payload"] is None:
            return None, "payload is required"

        if "event_time" in record:
            try:
                self._parse_event_time(record["event_time"])
            except (TypeError, ValueError) as error:
                return None, str(error)
        return record, None

    def _is_late(self, record: Record) -> bool:
        if self._max_lateness is None or "event_time" not in record:
            return False
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("clock must return a timezone-aware datetime")
        event_time = self._parse_event_time(record["event_time"])
        return event_time < now - self._max_lateness

    @staticmethod
    def _parse_event_time(value: Any) -> datetime:
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError as error:
                raise ValueError("event_time must be ISO 8601") from error
        else:
            raise TypeError("event_time must be a datetime or ISO 8601 string")
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("event_time must include a timezone")
        return parsed

    @staticmethod
    def _snapshot(record: Any) -> Any:
        return deepcopy(dict(record) if isinstance(record, Mapping) else record)
