"""Process-level checks for the Linux/WSL synthetic job CLI."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
from tempfile import TemporaryDirectory
import time
import unittest


HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "jobs.py"
JOB = "rj-0123456789abcdef0123456789abcdef"


class JobsTest(unittest.TestCase):
    def setUp(self):
        temp = TemporaryDirectory(prefix=".test-state-", dir=HERE)
        self.addCleanup(temp.cleanup)
        # SQLite URI quoting and subprocess argv must handle this literal name.
        self.state = Path(temp.name) / "jobs #1?"

    def cli(self, op, job_id=JOB, *flags):
        return subprocess.run(
            [sys.executable, "-I", "-B", str(SCRIPT), "--state-dir", str(self.state),
             op, "--job-id", job_id, *flags],
            capture_output=True, text=True, timeout=5,
        )

    def job(self, op, job_id=JOB, *flags):
        proc = self.cli(op, job_id, *flags)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stderr, "")
        self.assertEqual(len(proc.stdout.splitlines()), 1, proc.stdout)
        return json.loads(proc.stdout)

    def error(self, proc, code, label=None):
        self.assertEqual(proc.returncode, code, (proc.stdout, proc.stderr))
        self.assertEqual(proc.stdout, "")
        if label:
            self.assertEqual(json.loads(proc.stderr)["error"], label)

    def until(self, state, job_id=JOB, timeout=5):
        deadline = time.monotonic() + timeout
        while True:
            row = self.job("status", job_id)
            if row["status"] == state:
                return row
            self.assertLess(time.monotonic(), deadline, row)
            time.sleep(0.05)

    def test_detached_completion_and_replay(self):
        # A queued reply and closed pipes are observed before completion. Then
        # a separate process observes the job after the submitter has exited.
        spec = ("--duration-ms", "1200")
        queued = self.job("submit", JOB, *spec)
        self.assertEqual(queued["status"], "queued")
        self.assertEqual(queued["jobId"], JOB)
        self.assertEqual(queued["schemaVersion"], 1)
        self.assertEqual(queued["kind"], "synthetic")
        self.assertIsNone(queued["result"])
        self.assertIsNone(queued["finishedAt"])
        self.assertIn(self.job("status")["status"], {"queued", "running"})
        done = self.until("completed")
        self.assertEqual(done["durationMs"], 1200)
        self.assertEqual(done["submittedAt"], queued["submittedAt"])
        self.assertLessEqual(done["submittedAt"], done["startedAt"])
        self.assertLess(done["startedAt"], done["finishedAt"])
        expected = hashlib.sha256(f"synthetic-v1:{JOB}:1200".encode("ascii")).hexdigest()
        self.assertEqual(done["result"], {"sha256": expected})
        self.assertIsNone(done["failure"])
        self.assertEqual(self.job("submit", JOB, *spec), done)
        self.assertEqual(self.job("cancel"), done)

    def test_concurrent_identical_submits_and_conflict(self):
        spec = ("--duration-ms", "3000")
        with ThreadPoolExecutor(max_workers=8) as pool:
            replies = list(pool.map(lambda _: self.job("submit", JOB, *spec), range(8)))
        self.assertTrue(any(row["status"] == "queued" for row in replies))
        self.assertTrue(all(row["status"] in {"queued", "running"} for row in replies))
        self.assertEqual(len({row["submittedAt"] for row in replies}), 1)
        with closing(sqlite3.connect(self.state / "synthetic-jobs.sqlite3")) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 1)
        self.error(self.cli("submit", JOB, "--duration-ms", "3001"), 4, "id_conflict")
        self.assertEqual(self.job("cancel")["status"], "cancelled")

    def test_cancel_is_durable_after_original_finish_time(self):
        spec = ("--duration-ms", "1800")
        self.job("submit", JOB, *spec)
        running = self.until("running")
        cancelled = self.job("cancel")
        self.assertEqual(cancelled["status"], "cancelled")
        self.assertEqual(cancelled["startedAt"], running["startedAt"])
        self.assertIsNotNone(cancelled["finishedAt"])
        self.assertIsNone(cancelled["result"])
        time.sleep(1.9)
        self.assertEqual(self.job("status"), cancelled)
        self.assertEqual(self.job("cancel"), cancelled)
        self.assertEqual(self.job("submit", JOB, *spec), cancelled)

    def test_capacity_and_recovery(self):
        spec = ("--duration-ms", "30000")
        active = [f"capacity-{i}" for i in range(4)]
        for job_id in active:
            self.job("submit", job_id, *spec)
        self.error(self.cli("submit", "extra", *spec), 5, "capacity")
        self.error(self.cli("status", "extra"), 3, "not_found")
        self.job("submit", active[0], *spec)
        self.assertEqual(self.job("cancel", active.pop())["status"], "cancelled")
        self.job("submit", "extra", *spec)
        for job_id in [*active, "extra"]:
            self.assertEqual(self.job("cancel", job_id)["status"], "cancelled")

    def test_expired_worker_does_not_resurrect(self):
        self.job("submit", "seed", "--duration-ms", "1")
        self.until("completed", "seed")
        # Inject short, deterministic stand-ins for a dead worker without
        # requiring kill-by-PID or a five-second real-time timeout.
        with closing(sqlite3.connect(self.state / "synthetic-jobs.sqlite3")) as db, db:
            db.executemany("""INSERT INTO jobs (id, duration_ms, status, submitted_ms, deadline_ms)
                VALUES (?, 200, ?, 0, 1)""", [("missing-worker", "queued"), ("dead-worker", "running")])
        for job_id, error in [("missing-worker", "worker_start_timeout"), ("dead-worker", "worker_timeout")]:
            failed = self.job("status", job_id)
            self.assertEqual(failed["status"], "failed")
            self.assertEqual(failed["failure"], error)
            self.assertIsNone(failed["result"])
            self.assertEqual(self.job("cancel", job_id), failed)
            self.assertEqual(self.job("submit", job_id, "--duration-ms", "200"), failed)

    def test_missing_and_malformed_requests_do_not_create_state(self):
        self.error(self.cli("status"), 3, "not_found")
        self.error(self.cli("cancel"), 3, "not_found")
        for bad_id in ("../outside", "a/b", "a b", "a" * 129):
            self.error(self.cli("submit", bad_id), 2)
        for bad_duration in ("0", "30001", "-1", "1.5", "1_000"):
            self.error(self.cli("submit", JOB, "--duration-ms", bad_duration), 2)
        self.error(self.cli("submit", JOB, "--command", "unaccepted"), 2)
        self.assertFalse(self.state.exists())


if __name__ == "__main__":
    unittest.main()
