import concurrent.futures
import copy
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
from data_solution_event_stream_reliability.telemetry import EventConflict, TelemetryStore
from data_solution_event_stream_reliability.api import create_app
from fastapi.testclient import TestClient


def event(**changes):
    return {"factory_id": "factory-demo", "equipment_id": "pump-01", "event_id": "sample-1",
            "observed_at": "2026-09-30T10:00:00+09:00", "metric": "temperature",
            "value": 23.5, "unit": "degC", **changes}


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "telemetry.sqlite3"
        self.store = TelemetryStore(self.path)

    def rows(self):
        return self.store.query("factory-demo", "pump-01", "temperature")

    def test_ephemeral_database_paths_rejected(self):
        for path in ("", ":memory:"):
            with self.subTest(path=path), self.assertRaises(ValueError):
                TelemetryStore(path)

    def test_reopen_replay(self):
        self.assertEqual(self.store.ingest([event()]), {"inserted": 1, "duplicates": 0})
        self.store = TelemetryStore(self.path)
        self.assertEqual(self.store.ingest([event()]), {"inserted": 0, "duplicates": 1})
        self.assertEqual(self.rows()["aggregate"]["count"], 1)

    def test_concurrent_duplicates_across_store_instances(self):
        stores = [TelemetryStore(self.path) for _ in range(8)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda store: store.ingest([event()]), stores))
        self.assertEqual(sum(x["inserted"] for x in results), 1)
        self.assertEqual(sum(x["duplicates"] for x in results), 7)
        self.assertEqual(self.rows()["aggregate"]["count"], 1)

    def test_conflict_rolls_back_entire_batch(self):
        self.store.ingest([event()])
        with self.assertRaises(EventConflict):
            self.store.ingest([event(event_id="new"), event(value=99)])
        self.assertEqual(self.rows()["aggregate"]["count"], 1)
        self.assertEqual(self.store.ingest([event(event_id="new")])["inserted"], 1)

    def test_abrupt_process_exit_before_commit(self):
        import subprocess
        code = """
import os, sys
from data_solution_event_stream_reliability.telemetry import TelemetryStore
store = TelemetryStore(sys.argv[1])
store.ingest([__import__("json").loads(sys.argv[2])], before_commit=lambda: os._exit(17))
"""
        import json
        import os
        result = subprocess.run([sys.executable, "-c", code, str(self.path), json.dumps(event())],
                                env={**os.environ, "PYTHONPATH": str(Path(__file__).parents[1] / "src")})
        self.assertEqual(result.returncode, 17)
        self.assertEqual(self.rows()["events"], [])
        self.assertEqual(self.store.ingest([event()])["inserted"], 1)

    def test_same_event_id_separate_factories(self):
        self.assertEqual(self.store.ingest([event(), event(factory_id="other")])["inserted"], 2)
        self.assertEqual(self.rows()["aggregate"]["count"], 1)

    def test_failure_before_commit_then_retry(self):
        def fail():
            raise RuntimeError("injected crash boundary")
        with self.assertRaises(RuntimeError):
            self.store.ingest([event(), event(event_id="second")], before_commit=fail)
        self.assertEqual(TelemetryStore(self.path).query("factory-demo", "pump-01", "temperature")["events"], [])
        self.assertEqual(self.store.ingest([event(), event(event_id="second")])["inserted"], 2)

    def test_late_events_order_and_aggregate(self):
        self.store.ingest([event(event_id="new", value=30),
                           event(event_id="old", observed_at="2020-01-01T00:00:00Z", value=10),
                           event(event_id="also-new", value=20)])
        result = self.rows()
        self.assertEqual([x["event_id"] for x in result["events"]], ["old", "also-new", "new"])
        self.assertEqual(result["aggregate"], {"scope": "returned_events", "count": 3, "unit": "degC", "min": 10, "max": 30, "mean": 20})

    def test_bounded_query_aggregate_and_empty(self):
        self.store.ingest([event(), event(event_id="sample-2", value=99)])
        result = self.store.query("factory-demo", "pump-01", "temperature", limit=1)
        self.assertTrue(result["has_more"])
        self.assertEqual(result["aggregate"]["mean"], 23.5)
        self.assertIsNone(self.store.query("missing", "pump-01", "temperature")["aggregate"]["mean"])

    def test_equivalent_timezone_and_number_replay(self):
        self.store.ingest([event(value=23)])
        self.assertEqual(self.store.ingest([event(value=23.0, observed_at="2026-09-30T01:00:00Z")])["duplicates"], 1)

    def test_invalid_records_leave_no_writes(self):
        changes = [{"observed_at": "2026-09-30T00:00:00"}, {"observed_at": "yesterday"},
                   {"observed_at": "2026-01-01T00:00:00+00:99"},
                   {"observed_at": "2026-09-30T00:00:00.1234567Z"},
                   {"observed_at": "0001-01-01T00:00:00+09:00"},
                   {"value": float("nan")}, {"value": float("inf")}, {"value": -float("inf")},
                   {"value": True}, {"value": "12"}, {"value": 10**400},
                   {"factory_id": "../bad"}, {"equipment_id": " "}, {"event_id": "x"*65},
                   {"metric": "voltage"}, {"unit": "F"}, {"extra": "unknown"}]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.store.ingest([event(event_id="valid"), event(**change)])
        self.assertEqual(self.rows()["events"], [])

    def test_batch_bounds(self):
        for records in ([], [event()]*101, {}, None):
            with self.subTest(records=type(records)), self.assertRaises(ValueError):
                self.store.ingest(records)
        self.assertEqual(self.store.ingest([event()]*100), {"inserted": 1, "duplicates": 99})

    def test_conflicting_duplicates_in_single_batch_rollback(self):
        with self.assertRaises(EventConflict):
            self.store.ingest([event(), event(value=5)])
        self.assertEqual(self.rows()["events"], [])

    def test_lossy_integer_rejected_instead_of_silent_duplicate(self):
        self.store.ingest([event(value=2**53)])
        with self.assertRaisesRegex(ValueError, "exactly representable"):
            self.store.ingest([event(value=2**53 + 1)])
        self.assertEqual(self.rows()["events"][0]["value"], 2**53)

    def test_no_mutation_of_input(self):
        records = [event()]
        before = copy.deepcopy(records)
        self.store.ingest(records)
        self.assertEqual(records, before)


class APITests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.app = create_app(str(Path(self.temp.name) / "api.sqlite3"))
        self.client = self.enterContext(TestClient(self.app))

    def test_ready_ingest_replay_conflict_query(self):
        self.assertEqual(self.client.get("/readyz").json(), {"status": "ready"})
        self.assertEqual(self.client.post("/v1/telemetry", json=[event()]).json()["inserted"], 1)
        self.assertEqual(self.client.post("/v1/telemetry", json=[event()]).json()["duplicates"], 1)
        self.assertEqual(self.client.post("/v1/telemetry", json=[event(value=10)]).status_code, 409)
        response = self.client.get("/v1/factories/factory-demo/equipment/pump-01/telemetry?metric=temperature")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["aggregate"]["count"], 1)

    def test_validation(self):
        for body in ([event(observed_at="2026-01-01")], [event(factory_id=" ")], [event()]*101, []):
            self.assertEqual(self.client.post("/v1/telemetry", json=body).status_code, 422)
        for raw in ('[NaN]', '[Infinity]', '[1e999]', '{"a":1,"a":2}', 'not-json'):
            self.assertEqual(self.client.post("/v1/telemetry", content=raw, headers={"Content-Type": "application/json"}).status_code, 422)
        self.assertEqual(self.client.post("/v1/telemetry", content="[]").status_code, 415)

    def test_body_bound_including_streamed_body(self):
        response = self.client.post("/v1/telemetry", content=iter([b" " * 65536, b" " * 65537]), headers={"Content-Type": "application/json"})
        self.assertEqual(response.status_code, 413)

    def test_query_validation(self):
        path = "/v1/factories/factory-demo/equipment/pump-01/telemetry"
        for params in ({}, {"metric": "temperature", "limit": 1001}, {"metric": "bad"}):
            self.assertEqual(self.client.get(path, params=params).status_code, 422)

    def test_storage_failure_is_retryable_and_private(self):
        with patch.object(self.app.state.store, "ingest", side_effect=sqlite3.OperationalError("secret/path")):
            response = self.client.post("/v1/telemetry", json=[event()])
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.headers["retry-after"], "1")
        self.assertNotIn("secret", response.text)
        with patch.object(self.app.state.store, "ready", side_effect=sqlite3.OperationalError("missing")):
            self.assertEqual(self.client.get("/readyz").status_code, 503)

    def test_real_write_lock_failure_then_retry(self):
        with sqlite3.connect(self.app.state.store.path) as lock:
            lock.execute("BEGIN IMMEDIATE")
            response = self.client.post("/v1/telemetry", json=[event()])
            self.assertEqual(response.status_code, 503)
            lock.rollback()
        self.assertEqual(self.client.post("/v1/telemetry", json=[event()]).json(),
                         {"inserted": 1, "duplicates": 0})

    def test_database_runs_off_event_loop_thread(self):
        import threading
        called_threads = []
        original = self.app.state.store.ingest
        def checked(records):
            import asyncio
            with self.assertRaises(RuntimeError):
                asyncio.get_running_loop()
            called_threads.append(threading.get_ident())
            return original(records)
        with patch.object(self.app.state.store, "ingest", side_effect=checked):
            self.assertEqual(self.client.post("/v1/telemetry", json=[event()]).status_code, 200)
        self.assertEqual(len(called_threads), 1)


if __name__ == "__main__":
    unittest.main()
