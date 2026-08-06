import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from data_solution_event_stream_reliability.benchmark import run_benchmark


class BenchmarkTests(unittest.TestCase):
    def test_small_benchmark_reports_required_metrics(self):
        result = run_benchmark(events=100, warmup_runs=0, measured_runs=2)
        self.assertEqual(result["measured_runs"], 2)
        self.assertFalse(result["representative_workload"])
        self.assertTrue(result["correctness_passed"])
        self.assertTrue(result["failure_recovery_passed"])
        self.assertEqual(result["error_rate"], 0)
        self.assertLessEqual(
            result["run_latency_ms"]["p50"], result["run_latency_ms"]["p99"]
        )


if __name__ == "__main__":
    unittest.main()
