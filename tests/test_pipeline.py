import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from data_solution_event_stream_reliability import ReliablePipeline


class PipelineTests(unittest.TestCase):
    def test_validation_deduplication_and_dlq(self):
        result = ReliablePipeline().process([
            {"id": "a", "payload": {"value": 1}},
            {"id": "a", "payload": {"value": 1}},
            {"payload": {"value": 2}},
        ])
        self.assertEqual(len(result.accepted), 1)
        self.assertEqual(result.duplicates, 1)
        self.assertEqual(result.dead_letter[0]["reason"], "SCHEMA_VALIDATION_FAILED")


if __name__ == "__main__":
    unittest.main()
