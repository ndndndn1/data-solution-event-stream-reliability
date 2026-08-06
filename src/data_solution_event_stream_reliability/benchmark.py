from __future__ import annotations

import argparse
import json
import platform
import statistics
import sys
import time
import tracemalloc
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .scenario import ScenarioConfig, run_scenario
from .simulator import EventGenerationConfig


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def run_benchmark(
    *, events: int = 100_000, warmup_runs: int = 1, measured_runs: int = 10
) -> dict[str, Any]:
    if events <= 0 or warmup_runs < 0 or measured_runs <= 0:
        raise ValueError("events and measured_runs must be positive; warmup_runs cannot be negative")
    for index in range(warmup_runs):
        report = run_scenario(
            ScenarioConfig(generation=EventGenerationConfig(event_count=events, seed=index))
        )
        if not report.passed:
            raise RuntimeError("warmup correctness check failed")

    wall_seconds: list[float] = []
    cpu_seconds: list[float] = []
    peak_memory_bytes: list[int] = []
    correctness: list[bool] = []
    recovery: list[bool] = []
    delivered_records = 0
    for index in range(measured_runs):
        tracemalloc.start()
        cpu_started = time.process_time()
        wall_started = time.perf_counter()
        report = run_scenario(
            ScenarioConfig(
                generation=EventGenerationConfig(
                    event_count=events,
                    seed=20_260_806 + index,
                )
            )
        )
        wall = time.perf_counter() - wall_started
        cpu = time.process_time() - cpu_started
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        wall_seconds.append(wall)
        cpu_seconds.append(cpu)
        peak_memory_bytes.append(peak)
        correctness.append(report.passed)
        recovery.append(report.invariants["failed_events_replayed"])
        delivered_records = report.generation.delivered_records

    latencies_ms = [value * 1000 for value in wall_seconds]
    throughputs = [delivered_records / value for value in wall_seconds]
    failed_runs = sum(not passed for passed in correctness)
    return {
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "representative_workload": events >= 100_000,
        "events_per_run": events,
        "delivered_records_per_run": delivered_records,
        "warmup_runs": warmup_runs,
        "measured_runs": measured_runs,
        "throughput_records_per_second": {
            "mean": round(statistics.fmean(throughputs), 2),
            "min": round(min(throughputs), 2),
        },
        "run_latency_ms": {
            "p50": round(_percentile(latencies_ms, 0.50), 3),
            "p95": round(_percentile(latencies_ms, 0.95), 3),
            "p99": round(_percentile(latencies_ms, 0.99), 3),
        },
        "error_rate": failed_runs / measured_runs,
        "cpu_seconds": {
            "mean": round(statistics.fmean(cpu_seconds), 6),
            "max": round(max(cpu_seconds), 6),
        },
        "peak_memory_bytes": {
            "mean": round(statistics.fmean(peak_memory_bytes)),
            "max": max(peak_memory_bytes),
        },
        "storage_bytes": 0,
        "correctness_passed": all(correctness),
        "failure_recovery_passed": all(recovery),
        "environment": {
            "python": sys.version.split()[0],
            "system": platform.system(),
            "machine": platform.machine(),
        },
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Benchmark the composite reliability scenario.")
    parser.add_argument("--events", type=int, default=100_000)
    parser.add_argument("--warmup-runs", type=int, default=1)
    parser.add_argument("--measured-runs", type=int, default=10)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        result = run_benchmark(
            events=args.events,
            warmup_runs=args.warmup_runs,
            measured_runs=args.measured_runs,
        )
    except (RuntimeError, ValueError) as error:
        parser.error(str(error))
    rendered = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")
    return 0 if result["correctness_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
