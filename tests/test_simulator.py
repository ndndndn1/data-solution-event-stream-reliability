import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from data_solution_event_stream_reliability import (
    EventGenerationConfig,
    EventStreamGenerator,
    FailurePlan,
    FaultInjectingProcessor,
)


class EventStreamGeneratorTests(unittest.TestCase):
    def test_false_valued_downstream_is_called(self):
        class Sink(list):
            def __call__(self, record):
                self.append(record)

        sink = Sink()
        processor = FaultInjectingProcessor(FailurePlan(frozenset(), frozenset()), downstream=sink)
        processor({"id": "a", "payload": {}})
        self.assertEqual(len(sink), 1)
        self.assertEqual(processor.successful_deliveries, {"a": 1})


    def test_generates_expected_reiterable_volume_and_categories(self):
        stream = EventStreamGenerator(EventGenerationConfig(event_count=1_000)).generate()

        first_pass = list(stream)
        second_pass = list(stream)

        self.assertEqual(first_pass, second_pass)
        self.assertEqual(stream.summary.original_records, 1_000)
        self.assertEqual(stream.summary.duplicate_records, 50)
        self.assertEqual(stream.summary.delivered_records, 1_050)
        self.assertEqual(stream.summary.invalid_records, 10)
        self.assertEqual(stream.summary.late_records, 100)
        self.assertEqual(stream.summary.transient_failure_records, 20)
        self.assertEqual(stream.summary.outage_failure_records, 5)
        self.assertEqual(len(first_pass), 1_050)
        self.assertEqual(len(stream), 1_050)

    def test_seed_controls_the_delivery_order(self):
        first = EventStreamGenerator(
            EventGenerationConfig(event_count=1_000, seed=1)
        ).generate()
        same = EventStreamGenerator(
            EventGenerationConfig(event_count=1_000, seed=1)
        ).generate()
        different = EventStreamGenerator(
            EventGenerationConfig(event_count=1_000, seed=2)
        ).generate()

        self.assertEqual(first.delivery_order, same.delivery_order)
        self.assertNotEqual(first.delivery_order, different.delivery_order)

    def test_fault_injector_can_wrap_a_reusable_downstream(self):
        delivered = []
        processor = FaultInjectingProcessor(
            FailurePlan(
                transient_ids=frozenset({"transient"}),
                outage_ids=frozenset({"outage"}),
            ),
            downstream=lambda record: delivered.append(record["id"]),
        )

        with self.assertRaises(TimeoutError):
            processor({"id": "transient", "payload": {}})
        processor({"id": "transient", "payload": {}})
        with self.assertRaises(ConnectionError):
            processor({"id": "outage", "payload": {}})
        processor.recover_outage()
        processor({"id": "outage", "payload": {}})

        self.assertEqual(delivered, ["transient", "outage"])
        self.assertEqual(processor.attempts["transient"], 2)
        self.assertEqual(processor.successful_deliveries["outage"], 1)

    def test_rejects_invalid_generator_configuration(self):
        invalid_configs = [
            {"event_count": 0},
            {"duplicate_rate": -0.1},
            {"invalid_rate": 0.5, "late_rate": 0.6},
            {
                "invalid_rate": 0.99,
                "late_rate": 0,
                "transient_failure_rate": 0,
                "outage_failure_rate": 0,
                "duplicate_rate": 0.5,
            },
            {"reference_time": datetime(2026, 8, 6)},
            {
                "on_time_age": timedelta(minutes=10),
                "late_age": timedelta(minutes=5),
            },
        ]

        for values in invalid_configs:
            with self.subTest(values=values), self.assertRaises(ValueError):
                EventGenerationConfig(**values)


if __name__ == "__main__":
    unittest.main()
