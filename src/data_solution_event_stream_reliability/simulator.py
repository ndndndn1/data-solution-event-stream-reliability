from __future__ import annotations

import random
from collections import Counter
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass(frozen=True, slots=True)
class EventGenerationConfig:
    """Configuration for a deterministic synthetic event stream."""

    event_count: int = 100_000
    seed: int = 20_260_806
    duplicate_rate: float = 0.05
    invalid_rate: float = 0.01
    late_rate: float = 0.10
    transient_failure_rate: float = 0.02
    outage_failure_rate: float = 0.005
    reference_time: datetime = datetime(2026, 8, 6, tzinfo=timezone.utc)
    on_time_age: timedelta = timedelta(minutes=1)
    late_age: timedelta = timedelta(minutes=30)

    def __post_init__(self) -> None:
        if self.event_count <= 0:
            raise ValueError("event_count must be greater than zero")

        category_rates = (
            self.invalid_rate,
            self.late_rate,
            self.transient_failure_rate,
            self.outage_failure_rate,
        )
        rates = (self.duplicate_rate, *category_rates)
        if any(rate < 0 or rate > 1 for rate in rates):
            raise ValueError("all rates must be between zero and one")
        if sum(category_rates) > 1:
            raise ValueError("exclusive category rates must sum to one or less")
        if self.duplicate_count > self.event_count - self.invalid_count:
            raise ValueError("duplicate count cannot exceed valid event count")
        if (
            self.reference_time.tzinfo is None
            or self.reference_time.utcoffset() is None
        ):
            raise ValueError("reference_time must include a timezone")
        if self.on_time_age < timedelta(0):
            raise ValueError("on_time_age must be zero or greater")
        if self.late_age <= self.on_time_age:
            raise ValueError("late_age must be greater than on_time_age")

    @property
    def duplicate_count(self) -> int:
        return int(self.event_count * self.duplicate_rate)

    @property
    def invalid_count(self) -> int:
        return int(self.event_count * self.invalid_rate)

    @property
    def late_count(self) -> int:
        return int(self.event_count * self.late_rate)

    @property
    def transient_failure_count(self) -> int:
        return int(self.event_count * self.transient_failure_rate)

    @property
    def outage_failure_count(self) -> int:
        return int(self.event_count * self.outage_failure_rate)


@dataclass(frozen=True, slots=True)
class GenerationSummary:
    original_records: int
    duplicate_records: int
    delivered_records: int
    invalid_records: int
    valid_unique_records: int
    late_records: int
    transient_failure_records: int
    outage_failure_records: int


@dataclass(frozen=True, slots=True)
class FailurePlan:
    """IDs whose processor calls should fail in a controlled way."""

    transient_ids: frozenset[str]
    outage_ids: frozenset[str]


@dataclass(frozen=True, slots=True)
class GeneratedEventStream:
    """Re-iterable event stream with its deterministic fault metadata."""

    config: EventGenerationConfig
    delivery_order: tuple[int, ...]
    invalid_indexes: frozenset[int]
    late_indexes: frozenset[int]
    failure_plan: FailurePlan

    def __iter__(self) -> Iterator[dict[str, Any]]:
        for index in self.delivery_order:
            yield self._build_record(index)

    def __len__(self) -> int:
        return len(self.delivery_order)

    @property
    def summary(self) -> GenerationSummary:
        return GenerationSummary(
            original_records=self.config.event_count,
            duplicate_records=self.config.duplicate_count,
            delivered_records=len(self.delivery_order),
            invalid_records=self.config.invalid_count,
            valid_unique_records=self.config.event_count - self.config.invalid_count,
            late_records=self.config.late_count,
            transient_failure_records=self.config.transient_failure_count,
            outage_failure_records=self.config.outage_failure_count,
        )

    def _build_record(self, index: int) -> dict[str, Any]:
        event_time = self.config.reference_time - (
            self.config.late_age
            if index in self.late_indexes
            else self.config.on_time_age
        )
        record: dict[str, Any] = {
            "id": event_id(index),
            "event_time": event_time.isoformat(),
        }
        if index not in self.invalid_indexes:
            record["payload"] = {
                "campaign_id": index % 1_000,
                "event_type": "ad_impression",
                "value": index,
            }
        return record


class EventStreamGenerator:
    """Create synthetic streams without coupling generation to a consumer."""

    def __init__(self, config: EventGenerationConfig | None = None) -> None:
        self.config = config or EventGenerationConfig()

    def generate(self) -> GeneratedEventStream:
        rng = random.Random(self.config.seed)
        indexes = list(range(self.config.event_count))
        rng.shuffle(indexes)

        cursor = 0
        invalid = frozenset(indexes[cursor : cursor + self.config.invalid_count])
        cursor += self.config.invalid_count
        late = frozenset(indexes[cursor : cursor + self.config.late_count])
        cursor += self.config.late_count
        transient = frozenset(
            indexes[cursor : cursor + self.config.transient_failure_count]
        )
        cursor += self.config.transient_failure_count
        outage = frozenset(
            indexes[cursor : cursor + self.config.outage_failure_count]
        )

        valid_indexes = [
            index
            for index in range(self.config.event_count)
            if index not in invalid
        ]
        duplicate_indexes = rng.sample(valid_indexes, self.config.duplicate_count)
        delivery_order = list(range(self.config.event_count)) + duplicate_indexes
        rng.shuffle(delivery_order)

        return GeneratedEventStream(
            config=self.config,
            delivery_order=tuple(delivery_order),
            invalid_indexes=invalid,
            late_indexes=late,
            failure_plan=FailurePlan(
                transient_ids=frozenset(event_id(index) for index in transient),
                outage_ids=frozenset(event_id(index) for index in outage),
            ),
        )


class FaultInjectingProcessor:
    """Reusable callable that injects transient and outage failures."""

    def __init__(
        self,
        failure_plan: FailurePlan,
        downstream: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.failure_plan = failure_plan
        self._downstream = downstream or (lambda _record: None)
        self._outage_active = True
        self._attempts: Counter[str] = Counter()
        self._successful_deliveries: Counter[str] = Counter()

    def __call__(self, record: dict[str, Any]) -> None:
        record_id = record["id"]
        self._attempts[record_id] += 1
        if (
            record_id in self.failure_plan.transient_ids
            and self._attempts[record_id] == 1
        ):
            raise TimeoutError("simulated transient downstream timeout")
        if record_id in self.failure_plan.outage_ids and self._outage_active:
            raise ConnectionError("simulated downstream outage")

        self._downstream(record)
        self._successful_deliveries[record_id] += 1

    @property
    def attempts(self) -> Mapping[str, int]:
        return dict(self._attempts)

    @property
    def successful_deliveries(self) -> Mapping[str, int]:
        return dict(self._successful_deliveries)

    def recover_outage(self) -> None:
        self._outage_active = False


def event_id(index: int) -> str:
    return f"event-{index:08d}"
