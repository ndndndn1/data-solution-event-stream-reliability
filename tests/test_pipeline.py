import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from data_solution_event_stream_reliability import ReliablePipeline


class PipelineTests(unittest.TestCase):
    def test_validation_deduplication_and_dlq(self):
        result = ReliablePipeline().process(
            [
                {"id": "a", "payload": {"value": 1}},
                {"id": "a", "payload": {"value": 1}},
                {"payload": {"value": 2}},
            ]
        )

        self.assertEqual(len(result.accepted), 1)
        self.assertEqual(result.duplicates, 1)
        self.assertEqual(result.dead_letter[0]["reason"], "SCHEMA_VALIDATION_FAILED")
        self.assertEqual(result.metrics.received, 3)
        self.assertEqual(result.metrics.validation_failures, 1)

    def test_retries_then_accepts_without_counting_failed_attempt_as_duplicate(self):
        attempts = 0

        def flaky_processor(_record):
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise TimeoutError("temporary outage")

        pipeline = ReliablePipeline(flaky_processor, max_retries=2)
        result = pipeline.process([{"id": "retry-me", "payload": {}}])

        self.assertEqual(attempts, 3)
        self.assertEqual(len(result.accepted), 1)
        self.assertEqual(result.metrics.retry_attempts, 2)
        self.assertEqual(result.metrics.processing_failures, 0)

    def test_exhausted_retry_goes_to_dlq_and_can_be_replayed(self):
        unavailable = True

        def processor(_record):
            if unavailable:
                raise ConnectionError("sink unavailable")

        pipeline = ReliablePipeline(processor, max_retries=1)
        failed = pipeline.process([{"id": "recover-me", "payload": {"value": 1}}])

        self.assertEqual(len(failed.accepted), 0)
        self.assertEqual(failed.dead_letter[0]["reason"], "PROCESSING_FAILED")
        self.assertEqual(failed.dead_letter[0]["attempts"], 2)

        unavailable = False
        replayed = pipeline.replay(failed.dead_letter)

        self.assertEqual(len(replayed.accepted), 1)
        self.assertEqual(replayed.metrics.replayed, 1)
        self.assertEqual(replayed.metrics.recovered, 1)
        self.assertEqual(pipeline.metrics.processing_failures, 1)
        self.assertEqual(pipeline.metrics.recovered, 1)

    def test_successful_ids_are_deduplicated_across_calls(self):
        pipeline = ReliablePipeline()
        record = {"id": "once", "payload": {"value": 1}}

        pipeline.process([record])
        duplicate = pipeline.process([record])

        self.assertEqual(len(duplicate.accepted), 0)
        self.assertEqual(duplicate.duplicates, 1)

    def test_late_record_is_accepted_and_observable(self):
        now = datetime(2026, 8, 6, 0, 0, tzinfo=timezone.utc)
        pipeline = ReliablePipeline(
            max_lateness=timedelta(minutes=5), clock=lambda: now
        )

        result = pipeline.process(
            [
                {
                    "id": "late",
                    "payload": {},
                    "event_time": "2026-08-05T23:54:59Z",
                }
            ]
        )

        self.assertEqual(len(result.accepted), 1)
        self.assertEqual(result.metrics.late_records, 1)

    def test_invalid_optional_event_time_goes_to_dlq(self):
        result = ReliablePipeline().process(
            [{"id": "bad-time", "payload": {}, "event_time": "yesterday"}]
        )

        self.assertEqual(result.metrics.validation_failures, 1)
        self.assertIn("ISO 8601", result.dead_letter[0]["error"])

    def test_configuration_must_be_non_negative(self):
        with self.assertRaises(ValueError):
            ReliablePipeline(max_retries=-1)
        with self.assertRaises(ValueError):
            ReliablePipeline(max_lateness=timedelta(seconds=-1))


if __name__ == "__main__":
    unittest.main()
