import contextlib
import io
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from data_solution_event_stream_reliability.scenario import (
    ScenarioConfig,
    main,
    run_scenario,
)
from data_solution_event_stream_reliability.simulator import EventGenerationConfig


class ScenarioTests(unittest.TestCase):
    def test_combined_reliability_scenario_has_exact_conservation(self):
        report = run_scenario(
            ScenarioConfig(generation=EventGenerationConfig(event_count=1_000))
        )

        self.assertTrue(report.passed)
        self.assertEqual(report.generation.delivered_records, 1_050)
        self.assertEqual(report.initial_metrics.received, 1_050)
        self.assertEqual(report.initial_metrics.accepted, 985)
        self.assertEqual(report.initial_metrics.duplicates, 50)
        self.assertEqual(report.initial_metrics.validation_failures, 10)
        self.assertEqual(report.initial_metrics.late_records, 100)
        self.assertEqual(report.initial_metrics.retry_attempts, 30)
        self.assertEqual(report.initial_metrics.processing_failures, 5)
        self.assertEqual(report.replay_metrics.replayed, 5)
        self.assertEqual(report.replay_metrics.recovered, 5)
        self.assertEqual(
            report.remaining_dlq_by_reason, {"SCHEMA_VALIDATION_FAILED": 10}
        )

    def test_json_cli_output_is_machine_readable(self):
        output = io.StringIO()

        with contextlib.redirect_stdout(output):
            exit_code = main(["--events", "1000", "--seed", "7", "--json"])

        payload = json.loads(output.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertTrue(payload["passed"])
        self.assertEqual(payload["generation"]["original_records"], 1_000)
        self.assertEqual(payload["replay_metrics"]["recovered"], 5)

    def test_zero_retries_moves_transient_failures_to_replay(self):
        report = run_scenario(
            ScenarioConfig(
                generation=EventGenerationConfig(event_count=1_000),
                max_retries=0,
            )
        )

        self.assertTrue(report.passed)
        self.assertEqual(report.initial_metrics.retry_attempts, 0)
        self.assertEqual(report.initial_metrics.processing_failures, 25)
        self.assertEqual(report.replay_metrics.recovered, 25)


if __name__ == "__main__":
    unittest.main()
