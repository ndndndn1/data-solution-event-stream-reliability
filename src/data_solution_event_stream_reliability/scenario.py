from __future__ import annotations

import argparse
import json
from collections import Counter
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import timedelta
from typing import Any

from .pipeline import PipelineMetrics, ReliablePipeline
from .simulator import (
    EventGenerationConfig,
    EventStreamGenerator,
    FaultInjectingProcessor,
    GenerationSummary,
)


@dataclass(frozen=True, slots=True)
class ScenarioConfig:
    generation: EventGenerationConfig = field(default_factory=EventGenerationConfig)
    max_retries: int = 2
    max_lateness: timedelta = timedelta(minutes=5)

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError("max_retries must be zero or greater")
        if self.max_lateness < timedelta(0):
            raise ValueError("max_lateness must be zero or greater")


@dataclass(frozen=True, slots=True)
class ScenarioReport:
    generation: GenerationSummary
    initial_metrics: PipelineMetrics
    replay_metrics: PipelineMetrics
    initial_dlq_by_reason: dict[str, int]
    remaining_dlq_by_reason: dict[str, int]
    invariants: dict[str, bool]

    @property
    def passed(self) -> bool:
        return all(self.invariants.values())

    def to_dict(self) -> dict[str, Any]:
        return {
            "generation": asdict(self.generation),
            "initial_metrics": asdict(self.initial_metrics),
            "replay_metrics": asdict(self.replay_metrics),
            "initial_dlq_by_reason": dict(self.initial_dlq_by_reason),
            "remaining_dlq_by_reason": dict(self.remaining_dlq_by_reason),
            "invariants": dict(self.invariants),
            "passed": self.passed,
        }


def run_scenario(config: ScenarioConfig | None = None) -> ScenarioReport:
    """Run initial delivery, outage recovery, and selective DLQ replay."""

    scenario_config = config or ScenarioConfig()
    stream = EventStreamGenerator(scenario_config.generation).generate()
    processor = FaultInjectingProcessor(stream.failure_plan)
    pipeline = ReliablePipeline(
        processor,
        max_retries=scenario_config.max_retries,
        max_lateness=scenario_config.max_lateness,
        clock=lambda: scenario_config.generation.reference_time,
    )

    initial = pipeline.process(stream)
    initial_dlq = _count_reasons(initial.dead_letter)
    processing_dlq = [
        entry
        for entry in initial.dead_letter
        if entry["reason"] == "PROCESSING_FAILED"
    ]
    quarantined = [
        entry
        for entry in initial.dead_letter
        if entry["reason"] != "PROCESSING_FAILED"
    ]

    processor.recover_outage()
    replay = pipeline.replay(processing_dlq)
    remaining_dlq = [*quarantined, *replay.dead_letter]
    summary = stream.summary
    deliveries = processor.successful_deliveries
    transient_dlq = (
        summary.transient_failure_records
        if scenario_config.max_retries == 0
        else 0
    )
    expected_processing_dlq = summary.outage_failure_records + transient_dlq
    expected_retry_attempts = (
        summary.outage_failure_records * scenario_config.max_retries
        + (
            summary.transient_failure_records
            * min(scenario_config.max_retries, 1)
        )
    )
    expected_late_records = 0
    if scenario_config.generation.late_age > scenario_config.max_lateness:
        expected_late_records += summary.late_records
    if scenario_config.generation.on_time_age > scenario_config.max_lateness:
        expected_late_records += (
            summary.valid_unique_records - summary.late_records
        )

    invariants = {
        "all_valid_events_recovered": (
            initial.metrics.accepted + replay.metrics.recovered
            == summary.valid_unique_records
        ),
        "each_valid_event_delivered_once": (
            len(deliveries) == summary.valid_unique_records
            and all(count == 1 for count in deliveries.values())
        ),
        "all_duplicates_suppressed": (
            initial.metrics.duplicates == summary.duplicate_records
        ),
        "invalid_events_quarantined": (
            _count_reasons(remaining_dlq).get("SCHEMA_VALIDATION_FAILED", 0)
            == summary.invalid_records
        ),
        "late_events_classified": (
            initial.metrics.late_records == expected_late_records
        ),
        "retry_and_dlq_counts_match": (
            initial.metrics.retry_attempts == expected_retry_attempts
            and initial.metrics.processing_failures == expected_processing_dlq
        ),
        "failed_events_replayed": (
            replay.metrics.replayed == expected_processing_dlq
            and replay.metrics.recovered == expected_processing_dlq
            and replay.metrics.processing_failures == 0
        ),
    }

    return ScenarioReport(
        generation=summary,
        initial_metrics=initial.metrics,
        replay_metrics=replay.metrics,
        initial_dlq_by_reason=initial_dlq,
        remaining_dlq_by_reason=_count_reasons(remaining_dlq),
        invariants=invariants,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run a deterministic event-stream reliability scenario."
    )
    parser.add_argument(
        "--events",
        type=int,
        default=100_000,
        help="number of unique source events (default: 100000)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=20_260_806,
        help="random seed used for category selection and ordering",
    )
    parser.add_argument(
        "--json", action="store_true", help="emit a machine-readable JSON report"
    )
    args = parser.parse_args(argv)

    try:
        report = run_scenario(
            ScenarioConfig(
                generation=EventGenerationConfig(
                    event_count=args.events,
                    seed=args.seed,
                )
            )
        )
    except ValueError as error:
        parser.error(str(error))

    if args.json:
        print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    else:
        print(_format_report(report))
    return 0 if report.passed else 1


def _count_reasons(dead_letter: Sequence[dict[str, Any]]) -> dict[str, int]:
    return dict(Counter(entry["reason"] for entry in dead_letter))


def _format_report(report: ScenarioReport) -> str:
    generation = report.generation
    initial = report.initial_metrics
    replay = report.replay_metrics
    status = "PASS" if report.passed else "FAIL"
    return "\n".join(
        [
            "Event Stream Reliability Scenario",
            f"- source events: {generation.original_records:,}",
            f"- delivered records: {generation.delivered_records:,}",
            f"- accepted initially: {initial.accepted:,}",
            f"- duplicates suppressed: {initial.duplicates:,}",
            f"- late records observed: {initial.late_records:,}",
            f"- retry attempts: {initial.retry_attempts:,}",
            f"- processing failures sent to DLQ: {initial.processing_failures:,}",
            f"- recovered by replay: {replay.recovered:,}",
            f"- remaining DLQ: {sum(report.remaining_dlq_by_reason.values()):,}",
            f"- integrity checks: {status}",
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())
