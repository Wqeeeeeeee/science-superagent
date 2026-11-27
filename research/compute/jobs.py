#!/usr/bin/env python3
"""Local, bounded synthetic job lifecycle for Linux/WSL."""

import argparse
from contextlib import closing, contextmanager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import time

MAX_DURATION_MS = 30_000
MAX_ACTIVE = 4
GRACE_MS = 5_000
DB_NAME = "synthetic-jobs.sqlite3"
ACTIVE = ("queued", "running")


class JobError(Exception):
    def __init__(self, code, message, exit_code):
        super().__init__(message)
        self.code = code
        self.exit_code = exit_code


def now_ms():
    return time.time_ns() // 1_000_000


def timestamp(ms):
    if ms is None:
        return None
    dt = datetime.fromtimestamp(ms / 1000, timezone.utc)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def snapshot(row):
    return {
        "schemaVersion": 1,
        "jobId": row["id"],
        "kind": "synthetic",
        "status": row["status"],
        "durationMs": row["duration_ms"],
        "submittedAt": timestamp(row["submitted_ms"]),
        "startedAt": timestamp(row["started_ms"]),
        "finishedAt": timestamp(row["finished_ms"]),
        "result": {"sha256": row["digest"]} if row["digest"] is not None else None,
        "failure": row["failure"],
    }


@contextmanager
def connect(root, create):
    if create:
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = root / DB_NAME
    if not create and not path.exists():
        raise JobError("not_found", "job does not exist", 3)
    uri = path.as_uri() + ("?mode=rwc" if create else "?mode=rw")
    with closing(sqlite3.connect(uri, uri=True, isolation_level=None, timeout=2)) as db:
        db.row_factory = sqlite3.Row
        if create:
            db.execute("""CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY NOT NULL, duration_ms INTEGER NOT NULL,
                status TEXT NOT NULL, submitted_ms INTEGER NOT NULL,
                deadline_ms INTEGER NOT NULL, started_ms INTEGER,
                finished_ms INTEGER, digest TEXT, failure TEXT
            )""")
            db.execute("""CREATE INDEX IF NOT EXISTS active_jobs ON jobs (deadline_ms)
                WHERE status IN ('queued','running')""")
        yield db


@contextmanager
def transaction(db):
    db.execute("BEGIN IMMEDIATE")
    try:
        yield
        db.commit()
    except BaseException:
        db.rollback()
        raise


def lookup(db, job_id):
    return db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()


def expire(db, now, job_id=None):
    db.execute("""UPDATE jobs SET status = 'failed', finished_ms = ?,
        failure = CASE status WHEN 'queued' THEN 'worker_start_timeout' ELSE 'worker_timeout' END
        WHERE deadline_ms <= ? AND status IN ('queued','running')
        AND (? IS NULL OR id = ?)""", (now, now, job_id, job_id))


def launch(root, job_id):
    # No shell, caller-provided executable, inherited terminal, pipes or environment.
    subprocess.Popen(
        [sys.executable, "-I", "-B", str(Path(__file__).resolve()),
         "--state-dir", str(root), "_worker", "--job-id", job_id],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        close_fds=True, start_new_session=True, cwd=root, env={},
    )


def submit(db, root, job_id, duration):
    with transaction(db):
        now = now_ms()
        expire(db, now)
        row = lookup(db, job_id)
        if row is not None:
            if row["duration_ms"] != duration:
                raise JobError("id_conflict", "job ID already exists with a different duration", 4)
            return row
        active = db.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('queued','running')").fetchone()[0]
        if active >= MAX_ACTIVE:
            raise JobError("capacity", "active job limit reached; retry later", 5)
        db.execute("""INSERT INTO jobs (id, duration_ms, status, submitted_ms, deadline_ms)
            VALUES (?, ?, 'queued', ?, ?)""", (job_id, duration, now, now + GRACE_MS))
        # The child acquires the same write lock before claiming. A failed spawn
        # rolls back submission; a child cannot claim an uncommitted job.
        try:
            launch(root, job_id)
        except OSError as exc:
            raise JobError("launch_failed", "could not launch local Python worker", 1) from exc
        return lookup(db, job_id)


def observe(db, job_id, cancel):
    row = lookup(db, job_id)
    if row is None:
        raise JobError("not_found", "job does not exist", 3)
    if row["status"] not in ACTIVE or (not cancel and row["deadline_ms"] > now_ms()):
        return row
    with transaction(db):
        now = now_ms()
        expire(db, now, job_id)
        if cancel:
            db.execute("""UPDATE jobs SET status = 'cancelled', finished_ms = ?
                WHERE id = ? AND status IN ('queued','running')""", (now, job_id))
        return lookup(db, job_id)


def worker(db, job_id):
    with transaction(db):
        now = now_ms()
        expire(db, now, job_id)
        row = lookup(db, job_id)
        if row is None or row["status"] != "queued":
            return
        duration = row["duration_ms"]
        db.execute("""UPDATE jobs SET status = 'running', started_ms = ?, deadline_ms = ?
            WHERE id = ?""", (now, now + duration + GRACE_MS, job_id))
    end = time.monotonic() + duration / 1000
    while (remaining := end - time.monotonic()) > 0:
        time.sleep(min(remaining, 0.1))
        row = lookup(db, job_id)
        if row is None or row["status"] != "running":
            return
    digest = hashlib.sha256(f"synthetic-v1:{job_id}:{duration}".encode("ascii")).hexdigest()
    with transaction(db):
        now = now_ms()
        expire(db, now, job_id)
        db.execute("""UPDATE jobs SET status = 'completed', finished_ms = ?, digest = ?
            WHERE id = ? AND status = 'running'""", (now, digest, job_id))


def read_id(value):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", value):
        raise argparse.ArgumentTypeError("expected [A-Za-z0-9][A-Za-z0-9_.:-]{0,127}")
    return value


def read_duration(value):
    if re.fullmatch(r"[0-9]{1,5}", value) and 1 <= int(value) <= MAX_DURATION_MS:
        return int(value)
    raise argparse.ArgumentTypeError(f"expected an integer in [1, {MAX_DURATION_MS}]")


def main():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--state-dir", type=Path, required=True)
    operations = parser.add_subparsers(dest="op", metavar="{submit,status,cancel}", required=True)
    for op in ("submit", "status", "cancel", "_worker"):
        command = operations.add_parser(op, allow_abbrev=False)
        command.add_argument("--job-id", type=read_id, required=True)
        if op == "submit":
            command.add_argument("--duration-ms", type=read_duration, default=1000)
    args = parser.parse_args()
    try:
        root = args.state_dir.resolve()
        with connect(root, create=args.op == "submit") as db:
            if args.op == "_worker":
                worker(db, args.job_id)
                return 0
            if args.op == "submit":
                row = submit(db, root, args.job_id, args.duration_ms)
            else:
                row = observe(db, args.job_id, cancel=args.op == "cancel")
    except JobError as exc:
        print(json.dumps({"error": exc.code, "message": str(exc)}), file=sys.stderr)
        return exc.exit_code
    except (OSError, RuntimeError, sqlite3.Error) as exc:
        print(json.dumps({"error": "state_error", "message": str(exc)}), file=sys.stderr)
        return 1
    print(json.dumps(snapshot(row), separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
