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

    def test_nested_payload_isolated_for_each_retry_and_dlq(self):
        original = {"id": "nested", "payload": {"values": [1, 2, 3]}}
        seen = []

        def mutating_failure(record):
            seen.append(list(record["payload"]["values"]))
            record["payload"]["values"].pop()
            raise TimeoutError("failed after modifying working copy")

        result = ReliablePipeline(mutating_failure, max_retries=2).process([original])
        self.assertEqual(seen, [[1, 2, 3]] * 3)
        self.assertEqual(original["payload"]["values"], [1, 2, 3])
        original["payload"]["values"].append(99)
        self.assertEqual(result.dead_letter[0]["record"]["payload"]["values"], [1, 2, 3])
        replayed = ReliablePipeline().replay(result.dead_letter)
        self.assertEqual(replayed.accepted[0]["payload"]["values"], [1, 2, 3])

    def test_invalid_input_dlq_is_a_nested_snapshot(self):
        original = {"payload": {"value": 1}}
        result = ReliablePipeline().process([original])
        original["payload"]["value"] = 999
        self.assertEqual(result.dead_letter[0]["record"]["payload"]["value"], 1)

    def test_success_result_and_caller_isolated_from_processor(self):
        original = {"id": "ok", "payload": {"values": [1]}}
        captured = []

        def mutate(record):
            record["payload"]["values"].append(2)
            captured.append(record)

        result = ReliablePipeline(mutate).process([original])
        captured[0]["payload"]["values"].append(3)
        self.assertEqual(original["payload"]["values"], [1])
        self.assertEqual(result.accepted[0]["payload"]["values"], [1])
        original["payload"]["values"].append(4)
        self.assertEqual(result.accepted[0]["payload"]["values"], [1])

    def test_false_valued_callable_is_not_replaced_by_noop(self):
        class Sink(list):
            def __call__(self, record):
                self.append(record)

        sink = Sink()
        result = ReliablePipeline(sink).process([{"id": "a", "payload": {}}])
        self.assertEqual(result.metrics.accepted, 1)
        self.assertEqual(len(sink), 1)

    def test_false_valued_clock_is_used(self):
        class Clock:
            def __bool__(self):
                return False

            def __call__(self):
                return datetime(2026, 1, 1, tzinfo=timezone.utc)

        result = ReliablePipeline(clock=Clock()).process([
            {"id": "a", "payload": {}, "event_time": "2026-01-01T00:00:00Z"}
        ])
        self.assertEqual(result.metrics.late_records, 0)

    def test_configuration_must_be_non_negative(self):
        with self.assertRaises(ValueError):
            ReliablePipeline(max_retries=-1)
        with self.assertRaises(ValueError):
            ReliablePipeline(max_lateness=timedelta(seconds=-1))


if __name__ == "__main__":
    unittest.main()
