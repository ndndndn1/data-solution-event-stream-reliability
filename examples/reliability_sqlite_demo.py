"""Original pipeline acceptance using a real idempotent SQLite sink and disk DLQ.

Faults remain deliberately injected. Persistence and row/payload checks are real.
The sink's PRIMARY KEY, not the in-memory pipeline alone, closes lost-ack retries.
"""
import json
from pathlib import Path
import sqlite3
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from data_solution_event_stream_reliability import ReliablePipeline
from data_solution_event_stream_reliability.simulator import (
    EventGenerationConfig, EventStreamGenerator, FaultInjectingProcessor,
)


def run_demo():
    config = EventGenerationConfig(event_count=2_000, seed=29)
    stream = EventStreamGenerator(config).generate()
    # Oracle built from actual source records, independently of pipeline metrics.
    expected = {record["id"]: json.dumps(record, sort_keys=True)
                for record in stream if "payload" in record}
    with tempfile.TemporaryDirectory() as directory:
        database = Path(directory) / "sink.sqlite3"
        dlq_path = Path(directory) / "dlq.json"
        db = sqlite3.connect(database)
        try:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("CREATE TABLE events(id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
            acknowledgment_lost = False
            sink_replays = 0

            def sink(record):
                nonlocal acknowledgment_lost, sink_replays
                encoded = json.dumps(record, sort_keys=True)
                with db:
                    existing = db.execute("SELECT payload FROM events WHERE id=?", (record["id"],)).fetchone()
                    if existing:
                        if existing[0] != encoded:
                            raise ValueError("same event ID with conflicting payload")
                        sink_replays += 1
                    else:
                        db.execute("INSERT INTO events VALUES (?, ?)", (record["id"], encoded))
                # Inject the ambiguous boundary AFTER an actual durable commit.
                if not acknowledgment_lost:
                    acknowledgment_lost = True
                    raise TimeoutError("committed but acknowledgment was lost")

            processor = FaultInjectingProcessor(stream.failure_plan, downstream=sink)
            pipeline = ReliablePipeline(processor, clock=lambda: config.reference_time)
            initial = pipeline.process(stream)
            dlq_path.write_text(json.dumps(initial.dead_letter), encoding="utf-8")
            saved_dlq = json.loads(dlq_path.read_text(encoding="utf-8"))
            processing = [entry for entry in saved_dlq if entry["reason"] == "PROCESSING_FAILED"]
            invalid = [entry for entry in saved_dlq if entry["reason"] == "SCHEMA_VALIDATION_FAILED"]
            processor.recover_outage()
            recovered = pipeline.replay(processing)
            observed = dict(db.execute("SELECT id, payload FROM events"))
            assert observed == expected
            assert initial.metrics.accepted == 1970
            assert recovered.metrics.recovered == 10
            assert len(invalid) == 20
            assert initial.metrics.late_records == 200
            # A fresh pipeline has forgotten all IDs. Sink idempotency still protects
            # already committed writes when the entire source is replayed.
            restarted = ReliablePipeline(sink, clock=lambda: config.reference_time)
            replay = restarted.process(stream)
            assert dict(db.execute("SELECT id, payload FROM events")) == expected
            assert replay.metrics.accepted == 1980
            assert replay.metrics.duplicates == 100
            assert replay.metrics.validation_failures == 20
        finally:
            db.close()
        # Reopen the real file to distinguish durable rows from object counters.
        db = sqlite3.connect(database)
        try:
            assert dict(db.execute("SELECT id, payload FROM events")) == expected
        finally:
            db.close()

        # Negative control: a non-idempotent sink cannot resolve ambiguous commits.
        db = sqlite3.connect(Path(directory) / "non_idempotent.sqlite3")
        try:
            db.execute("CREATE TABLE effects(id TEXT)")
            attempts = 0

            def unsafe_sink(record):
                nonlocal attempts
                with db:
                    db.execute("INSERT INTO effects VALUES (?)", (record["id"],))
                attempts += 1
                if attempts == 1:
                    raise TimeoutError("committed but acknowledgment was lost")

            negative = ReliablePipeline(unsafe_sink, max_retries=1).process([{"id": "one", "payload": {}}])
            unsafe_rows = db.execute("SELECT COUNT(*) FROM effects").fetchone()[0]
            assert negative.metrics.accepted == 1 and unsafe_rows == 2
        finally:
            db.close()
        print(json.dumps({"result": "PASS", "source_records": 2000,
            "initial_accepted": 1970, "recovered_from_disk_dlq": 10,
            "invalid_quarantined": 20, "late_observed": 200,
            "durable_rows": len(expected), "all_payloads_match_source": True,
            "new_pipeline_full_replay_added_rows": 0,
            "lost_ack_injected_after_commit": acknowledgment_lost,
            "idempotent_sink_replays": sink_replays,
            "negative_control_non_idempotent_effect_rows": unsafe_rows,
            "scope": "real local SQLite + synthetic source and controlled faults"}, indent=2))


if __name__ == "__main__":
    run_demo()
