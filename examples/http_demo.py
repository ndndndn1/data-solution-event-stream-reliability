"""Real loopback HTTP + process restart acceptance demo. Uses synthetic data only."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = json.loads((ROOT / "examples/telemetry-batch.json").read_text())


def run_demo():
    with tempfile.TemporaryDirectory() as directory:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        url = f"http://127.0.0.1:{port}"
        env = dict(os.environ, TELEMETRY_DB=str(Path(directory) / "demo.sqlite3"))
        env["PYTHONPATH"] = str(ROOT / "src") + os.pathsep + env.get("PYTHONPATH", "")

        def request(path, payload=None):
            data = None if payload is None else json.dumps(payload).encode()
            req = Request(url + path, data=data, headers={"Content-Type": "application/json"})
            try:
                with urlopen(req, timeout=3) as response:
                    return response.status, json.load(response)
            except HTTPError as response:
                return response.code, json.load(response)

        def start():
            process = subprocess.Popen([sys.executable, "-m", "uvicorn",
                "data_solution_event_stream_reliability.api:app", "--host", "127.0.0.1",
                "--port", str(port), "--log-level", "warning"], env=env)
            try:
                for _ in range(100):
                    if process.poll() is not None:
                        raise RuntimeError("HTTP server exited during startup")
                    try:
                        if request("/readyz")[0] == 200:
                            return process
                    except (URLError, TimeoutError):
                        pass
                    time.sleep(0.05)
                raise RuntimeError("HTTP server not ready in five seconds")
            except BaseException:
                stop(process)
                raise

        def stop(process):
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

        process = start()
        try:
            assert request("/v1/telemetry", SAMPLE) == (200, {"inserted": 2, "duplicates": 0})
        finally:
            stop(process)
        process = start()
        try:
            assert request("/v1/telemetry", SAMPLE) == (200, {"inserted": 0, "duplicates": 2})
            assert request("/v1/telemetry", [{**SAMPLE[0], "value": 999}])[0] == 409
            status, result = request("/v1/factories/factory-demo/equipment/pump-01/telemetry?metric=temperature")
            assert status == 200
            assert [row["event_id"] for row in result["events"]] == ["sample-late", "sample-new"]
            assert result["aggregate"]["mean"] == 25
            assert result["aggregate"]["count"] == 2
            print(json.dumps({"result": "PASS", "transport": "real loopback HTTP", "process_restart": True,
                              "inserted": 2, "replay_duplicates": 2, "conflict_status": 409,
                              "ordered_ids": [row["event_id"] for row in result["events"]],
                              "aggregate": result["aggregate"]}, indent=2))
        finally:
            stop(process)


if __name__ == "__main__":
    run_demo()
