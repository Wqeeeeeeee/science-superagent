#!/usr/bin/env python3
"""Local research ledger and bounded, offline synthetic workflow."""
import argparse
import contextlib
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import subprocess
import sys
import time
import uuid

BASE = Path(__file__).resolve().parent
LEASE = 3
DURATION_MS = 2000
DATA = ('02', '08', '10', '20')
TASK_SCHEMA = json.loads((BASE / 'task.schema.json').read_text())
RESULT_SCHEMA = json.loads((BASE / 'result.schema.json').read_text())
ENV = {'PATH': os.defpath, 'LANG': 'C.UTF-8'}


class Invalid(ValueError):
    pass


def dumps(data):
    return json.dumps(data, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(text):
    return hashlib.sha256(text.encode()).hexdigest()


def validate(rule, value):
    """Validate the deliberately small, dependency-free subset used by our two schemas."""
    if 'anyOf' in rule:
        for part in rule['anyOf']:
            try:
                validate(part, value)
                return
            except Invalid:
                pass
        raise Invalid('no matching evidence schema')
    kind = rule.get('type')
    if kind and type(value) != {'object': dict, 'array': list, 'integer': int, 'string': str}[kind]:
        raise Invalid('wrong JSON type')
    if 'const' in rule and (type(value) != type(rule['const']) or value != rule['const']):
        raise Invalid('wrong schema version')
    if 'enum' in rule and value not in rule['enum']:
        raise Invalid('invalid enum member')
    if kind == 'object':
        if set(value) != set(rule['required']):
            raise Invalid('wrong object keys')
        for key, spec in rule['properties'].items():
            validate(spec, value[key])
    elif kind == 'array':
        if len(value) > rule['maxItems']:
            raise Invalid('too many array items')
        for item in value:
            validate(rule['items'], item)
        if rule['uniqueItems'] and len(set(value)) != len(value):
            raise Invalid('duplicate array member')
    elif kind == 'string':
        if 'pattern' in rule and not re.fullmatch(rule['pattern'], value):
            raise Invalid('invalid string pattern')
        if not rule.get('minLength', 0) <= len(value) <= rule.get('maxLength', 65536):
            raise Invalid('invalid string length')
    elif kind == 'integer' and not rule['minimum'] <= value <= rule['maximum']:
        raise Invalid('integer out of bounds')


def read_result(path):
    def unique(pairs):
        result = dict(pairs)
        if len(result) != len(pairs):
            raise Invalid('duplicate result keys')
        return result

    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > 16384:
            raise Invalid('invalid result artifact')
        return json.loads(stream.read(16385), object_pairs_hook=unique)


def evidence(task, job_id):
    if task['kind'] == 'recovery':
        return {'normalizer': 'decimal-v1'}
    if task['kind'] == 'compute':
        return {'sha256': digest(f'synthetic-v1:{job_id}:{DURATION_MS}')}
    values = [int(x, 10) for x in DATA]
    return {'count': len(values), 'total': sum(values), 'mean_milli': 1000 * sum(values) // len(values)}


def validate_result(path, task, attempt, job_id=None):
    result = read_result(path)
    validate(RESULT_SCHEMA, result)
    identity = (result['task_id'], result['attempt'], result['kind'])
    if identity != (task['id'], attempt, task['kind']) or result['evidence'] != evidence(task, job_id):
        raise Invalid('independent scientific acceptance failed')
    return result


def select_executor(task, available=('fixture',)):
    if task['kind'] == 'compute':
        if task['executor'] not in ('auto', 'fixture'):
            raise Invalid('compute belongs to the independent manager')
        return 'compute'
    preference = ('dsh', 'codex', 'fixture') if task['executor'] == 'auto' else (task['executor'],)
    return next((name for name in preference if name in available), None)


class Controller:
    def __init__(self, root, *, readonly=False):
        self.root = Path(root).resolve()
        db = self.root / 'controller.sqlite3'
        if readonly:
            if not db.is_file():
                raise Invalid('research ledger does not exist')
            target = db.as_uri() + '?mode=ro'
        else:
            self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
            target = str(db)
        self.db = sqlite3.connect(target, uri=readonly, isolation_level=None, timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA foreign_keys=ON')
        if readonly:
            return
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS run (id INTEGER PRIMARY KEY CHECK(id=1),
                identity TEXT NOT NULL, config_hash TEXT NOT NULL, budget INTEGER NOT NULL, spent INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, contract TEXT NOT NULL,
                resource TEXT NOT NULL, state TEXT NOT NULL, tries INTEGER NOT NULL DEFAULT 0,
                lease TEXT, expires REAL, checkpoint TEXT, job_id TEXT, ticket_id TEXT UNIQUE, session_id TEXT UNIQUE,
                original_id TEXT REFERENCES tasks(id), reason TEXT);
            CREATE UNIQUE INDEX IF NOT EXISTS resource_guard ON tasks(resource) WHERE state='running';
            CREATE TABLE IF NOT EXISTS attempts (task_id TEXT REFERENCES tasks(id), number INTEGER,
                worker TEXT NOT NULL, executor TEXT NOT NULL, directory TEXT NOT NULL UNIQUE,
                outcome TEXT NOT NULL, code TEXT, PRIMARY KEY(task_id,number));
            CREATE TABLE IF NOT EXISTS issues (id TEXT PRIMARY KEY, state TEXT NOT NULL, fingerprint TEXT UNIQUE,
                task_id TEXT REFERENCES tasks(id), recovery_id TEXT UNIQUE REFERENCES tasks(id),
                attempts TEXT NOT NULL, log_path TEXT NOT NULL, acceptance TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, kind TEXT NOT NULL,
                task_id TEXT REFERENCES tasks(id), recovery_id TEXT REFERENCES tasks(id),
                issue_id TEXT REFERENCES issues(id), created_at TEXT NOT NULL);
        ''')

    @contextlib.contextmanager
    def transaction(self):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
        except BaseException:
            self.db.rollback()
            raise
        else:
            self.db.commit()

    def rows(self, sql, args=()):
        return [dict(x) for x in self.db.execute(sql, args)]

    def initialize(self, config):
        if type(config) != dict or set(config) != {'budget', 'tasks'}:
            raise Invalid('invalid workflow keys')
        budget, tasks = config['budget'], config['tasks']
        if type(budget) != int or not 1 <= budget <= 100 or type(tasks) != list or not 1 <= len(tasks) <= 64:
            raise Invalid('invalid workflow limits')
        graph = {}
        for task in tasks:
            validate(TASK_SCHEMA, task)
            name = task['id']
            if name in graph or name.startswith('recover-') or task['kind'] == 'recovery':
                raise Invalid('duplicate or reserved task')
            graph[name] = set(task['depends_on'])
        remain = set(graph)
        while remain:
            ready = {k for k in remain if graph[k] <= graph.keys() - remain}
            if not ready:
                raise Invalid('cyclic or missing dependency')
            remain -= ready
        with self.transaction():
            key = digest(dumps(config))
            old = self.db.execute('SELECT config_hash FROM run').fetchone()
            if old:
                if old[0] != key:
                    raise Invalid('workflow drift; use a new state directory')
                return
            self.db.execute('INSERT INTO run VALUES (1,?,?,?,0)', (uuid.uuid4().hex, key, budget))
            for task in tasks:
                self.insert(task, 'queued')

    def insert(self, task, state, original=None):
        self.db.execute('INSERT INTO tasks(id,contract,resource,state,original_id) VALUES (?,?,?,?,?)',
                        (task['id'], dumps(task), task['resource'], state, original))

    def events(self):
        return self.rows('SELECT * FROM events ORDER BY id')

    def issues(self):
        rows = self.rows('SELECT * FROM issues ORDER BY id')
        for row in rows:
            row['attempts'] = json.loads(row['attempts'])
        return rows

    def status(self):
        return {'run': self.rows('SELECT budget,spent FROM run'), 'tasks': self.rows(
            'SELECT id,state,tries,checkpoint,job_id,ticket_id,session_id,original_id,reason FROM tasks ORDER BY id'),
            'attempts': self.rows('SELECT * FROM attempts ORDER BY task_id,number'),
            'issues': self.issues(), 'events': self.events()}

    def route(self, event_id, ticket_id, session_id):
        if not all(re.fullmatch(r'[A-Za-z0-9._:/-]{1,128}', x) for x in (ticket_id, session_id)):
            raise Invalid('invalid native linkage')
        with self.transaction():
            row = self.db.execute('SELECT t.* FROM events e JOIN tasks t ON t.id=e.recovery_id WHERE e.id=?',
                                  (event_id,)).fetchone()
            if not row:
                raise Invalid('unknown controller event')
            if row['ticket_id']:
                if (row['ticket_id'], row['session_id']) != (ticket_id, session_id):
                    raise Invalid('conflicting event route')
                return 'duplicate'
            if row['state'] != 'awaiting_route':
                raise Invalid('recovery not awaiting a route')
            self.db.execute('UPDATE tasks SET state=\'queued\',ticket_id=?,session_id=? WHERE id=?',
                            (ticket_id, session_id, row['id']))
            return 'stored'

    def stop_task(self, row, reason):
        self.db.execute("UPDATE tasks SET state='stopped',reason=?,lease=NULL,expires=NULL WHERE id=?",
                        (reason, row['id']))
        if row['original_id']:
            self.db.execute("UPDATE tasks SET state='stopped', reason='recovery_stopped' WHERE id=? AND state='waiting_recovery'",
                            (row['original_id'],))

    def claim(self):
        with self.transaction():
            now = time.time()
            for row in self.rows("SELECT * FROM tasks WHERE state='running' AND expires<=?", (now,)):
                self.db.execute("UPDATE attempts SET outcome='interrupted',code='lease_expired' WHERE task_id=? AND number=?",
                                (row['id'], row['tries']))
                self.db.execute("UPDATE tasks SET state='queued',lease=NULL,expires=NULL WHERE id=?", (row['id'],))
            run = self.db.execute('SELECT * FROM run').fetchone()
            if not run:
                return None
            rows = self.rows('SELECT * FROM tasks ORDER BY id')
            states = {row['id']: row['state'] for row in rows}
            locked = {row['resource'] for row in rows if row['state'] == 'running'}
            for row in rows:
                if row['state'] != 'queued':
                    continue
                task = json.loads(row['contract'])
                deps = [states[d] for d in task['depends_on']]
                if 'stopped' in deps:
                    self.stop_task(row, 'dependency_stopped')
                    continue
                if any(state != 'succeeded' for state in deps) or row['resource'] in locked:
                    continue
                if row['tries'] >= task['attempts'] or run['spent'] >= run['budget']:
                    self.stop_task(row, 'attempt_limit' if row['tries'] >= task['attempts'] else 'budget_limit')
                    continue
                backend = select_executor(task)
                if backend is None:
                    self.stop_task(row, 'executor_not_enabled')
                    continue
                lease, number = uuid.uuid4().hex, row['tries'] + 1
                worker = 'recovery-worker' if row['original_id'] else 'primary-worker'
                directory = f'work/{worker}/{row["id"]}/{number:02d}-{lease}'
                job_id = (row['job_id'] or 'rj-' + digest(run['identity'] + ':' + row['id'])[:32]) if backend == 'compute' else None
                self.db.execute('UPDATE run SET spent=spent+1 WHERE id=1')
                self.db.execute("UPDATE tasks SET state='running',tries=?,lease=?,expires=?,job_id=? WHERE id=?",
                                (number, lease, now + LEASE, job_id, row['id']))
                self.db.execute('INSERT INTO attempts VALUES (?,?,?,?,?,?,NULL)',
                                (row['id'], number, worker, backend, directory, 'running'))
                return dict(row, task=task, lease=lease, number=number, directory=directory, job_id=job_id,
                            executor=backend)
            return None

    def heartbeat(self, work):
        now = time.time()
        cursor = self.db.execute("UPDATE tasks SET expires=? WHERE id=? AND lease=? AND state='running' AND expires>?",
                                 (now + LEASE, work['id'], work['lease'], now))
        if cursor.rowcount != 1:
            raise Invalid('lease expired')

    def checkpoint(self, work, value):
        with self.transaction():
            self.heartbeat(work)
            self.db.execute('UPDATE tasks SET checkpoint=? WHERE id=?', (value, work['id']))

    def escalate(self, work, code):
        key = self.db.execute('SELECT identity FROM run').fetchone()[0]
        fingerprint = digest('statistics-v1:' + work['id'] + ':' + code)
        prior = self.db.execute('SELECT id FROM issues WHERE fingerprint=? OR task_id=?',
                                (fingerprint, work['id'])).fetchone()
        if prior:
            self.stop_task(work, 'repeat_after_recovery')
            return
        unique = digest(key + ':' + fingerprint)[:32]
        recovery_id, issue_id = 'recover-' + unique, 'ri-' + unique
        contract = dict(version=1, id=recovery_id, kind='recovery', depends_on=[],
                        resource='synthetic-recovery', executor='auto', attempts=1, timeout_s=4,
                        description='Propose a decimal normalization fix for the synthetic dataset.')
        validate(TASK_SCHEMA, contract)
        self.insert(contract, 'awaiting_route', work['id'])
        attempts = self.rows('SELECT number,executor,directory,outcome,code FROM attempts WHERE task_id=? ORDER BY number',
                             (work['id'],))
        self.db.execute('INSERT INTO issues VALUES (?,?,?,?,?,?,?,?)', (issue_id, 'open', fingerprint, work['id'],
                         recovery_id, dumps(attempts), work['directory'] + '/worker.log',
                         'Allowlisted decimal-v1 repair and exact synthetic metrics on original replay.'))
        self.db.execute('INSERT INTO events VALUES (?,?,?,?,?,?)', ('re-' + unique, 'recovery.requested', work['id'],
                         recovery_id, issue_id, datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')))
        self.db.execute("UPDATE tasks SET state='waiting_recovery' WHERE id=?", (work['id'],))

    def finish(self, work, failure=None):
        if failure is None:
            try:
                validate_result(self.root / work['directory'] / 'result.json', work['task'], work['number'], work['job_id'])
            except (ValueError, OSError):
                failure = 'validation_rejected'
        with self.transaction():
            self.heartbeat(work)
            self.db.execute('UPDATE tasks SET lease=NULL,expires=NULL WHERE id=?', (work['id'],))
            self.db.execute('UPDATE attempts SET outcome=?,code=? WHERE task_id=? AND number=?',
                            ('failed' if failure else 'validated', failure, work['id'], work['number']))
            if not failure:
                self.db.execute("UPDATE tasks SET state='succeeded',reason=NULL WHERE id=?", (work['id'],))
                if work['original_id']:
                    self.db.execute("UPDATE issues SET state='closed' WHERE recovery_id=?", (work['id'],))
                    self.db.execute("UPDATE tasks SET state='queued',checkpoint='decimal-v1' WHERE id=? AND state='waiting_recovery'",
                                    (work['original_id'],))
            elif (work['task']['kind'] == 'statistics' and failure == 'fixture_decimal_parse' and
                  work['number'] >= 2 and work['number'] < work['task']['attempts']):
                self.escalate(work, failure)
            elif work['number'] >= work['task']['attempts']:
                self.stop_task(work, failure)
            else:
                self.db.execute("UPDATE tasks SET state='queued',reason=? WHERE id=?", (failure, work['id']))
        return failure

    def compute(self, work, *, detach=False):
        cmd = [sys.executable, '-I', '-B', str(BASE / 'compute/jobs.py'), '--state-dir', str(self.root / 'compute')]
        args = ['--job-id', work['job_id']]
        timeout = time.monotonic() + work['task']['timeout_s']

        def call(verb, extra=()):
            reply = subprocess.run(cmd + [verb] + args + list(extra), capture_output=True, env=ENV, timeout=2)
            if reply.returncode != 0:
                raise Invalid('compute ' + verb + ' failed')
            return json.loads(reply.stdout)

        self.heartbeat(work)
        job = call('submit', ('--duration-ms', str(DURATION_MS)))
        self.checkpoint(work, 'job-submitted')
        if detach:
            print(dumps({'phase': 'detached', 'job': job}), flush=True)
            os._exit(75)  # Simulate loss of the controller/worker invoker, not a graceful release.
        while job['status'] in ('queued', 'running'):
            if time.monotonic() >= timeout:
                self.heartbeat(work)
                call('cancel')
                return 'compute_timeout'
            time.sleep(0.1)
            self.heartbeat(work)
            job = call('status')
        if job['status'] != 'completed' or job['jobId'] != work['job_id']:
            return 'compute_not_completed'
        self.checkpoint(work, 'job-completed')
        path = self.root / work['directory'] / 'result.json'
        path.write_text(dumps(dict(version=1, task_id=work['id'], attempt=work['number'],
                                  kind='compute', evidence=job['result'])) + '\n')
        return None

    def execute(self, work, *, detach=False):
        directory = self.root / work['directory']
        directory.mkdir(mode=0o700, parents=True)
        if work['executor'] == 'compute':
            try:
                failure = self.compute(work, detach=detach)
            except (Invalid, subprocess.TimeoutExpired):
                # Do not commit an outcome on a lost/stale lease; finish fences the writer.
                failure = 'compute_adapter_error'
        else:
            mode = ('decimal-v1' if work['checkpoint'] == 'decimal-v1' or work['id'] != 'process'
                    else ('local-trim' if work['number'] > 1 else 'base-auto'))
            request = dict(task_id=work['id'], kind=work['task']['kind'], attempt=work['number'], mode=mode, data=DATA)
            (directory / 'input.json').write_text(dumps(request) + '\n')
            failure = self.fixture(work, directory)
        (directory / 'worker.log').write_text(dumps({'executor': work['executor'], 'task_id': work['id'],
                                                   'attempt': work['number'], 'outcome': failure or 'submitted',
                                                   'strategy': mode if work['executor'] != 'compute' else 'detached'}) + '\n')
        return self.finish(work, failure)

    def fixture(self, work, directory):
        cmd = [sys.executable, '-I', '-B', str(BASE / 'fixture_worker.py')]
        deadline = time.monotonic() + work['task']['timeout_s']
        proc = subprocess.Popen(cmd, cwd=directory, env=ENV, stdin=subprocess.DEVNULL,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            while True:
                try:
                    code = proc.wait(timeout=0.2)
                    return {0: None, 12: 'fixture_decimal_parse'}.get(code, 'fixture_error')
                except subprocess.TimeoutExpired:
                    if time.monotonic() >= deadline:
                        return 'worker_timeout'
                    self.heartbeat(work)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()


def main():
    os.umask(0o077)
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--state-dir', required=True, type=Path)
    sub = cli.add_subparsers(dest='command', required=True)
    for name in ('status', 'events', 'issues'):
        sub.add_parser(name)
    route = sub.add_parser('route')
    for name in ('event_id', 'ticket_id', 'session_id'):
        route.add_argument(name)
    demo = sub.add_parser('demo')
    demo.add_argument('--config', type=Path, default=BASE / 'demo.json')
    demo.add_argument('--stop-after-escalation', action='store_true')
    demo.add_argument('--require-native', action='store_true')
    demo.add_argument('--detach-on-submit', action='store_true')
    args = cli.parse_args()
    ctl = Controller(args.state_dir, readonly=args.command in ('status', 'events', 'issues'))
    if args.command in ('status', 'events', 'issues'):
        print(dumps(getattr(ctl, args.command)()))
        return 0
    if args.command == 'route':
        print(dumps({'disposition': ctl.route(args.event_id, args.ticket_id, args.session_id)}))
        return 0
    ctl.initialize(json.loads(args.config.read_text()))
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        state = ctl.status()
        if state['events'] and args.stop_after_escalation:
            break
        pending_routes = {t['id'] for t in state['tasks'] if t['state'] == 'awaiting_route'}
        if pending_routes and not args.require_native:
            for event in state['events']:
                if event['recovery_id'] in pending_routes:
                    ctl.route(event['id'], 'fixture-ticket-' + event['id'], 'fixture-session-' + event['id'])
        work = ctl.claim()
        if work:
            ctl.execute(work, detach=args.detach_on_submit)
        elif any(t['state'] == 'running' for t in state['tasks']):
            time.sleep(0.1)
        elif any(t['state'] == 'queued' for t in state['tasks']) and not pending_routes:
            continue  # Propagate staged dependency stops.
        else:
            break
    final = ctl.status()
    print(dumps(final))
    if all(t['state'] == 'succeeded' for t in final['tasks']):
        return 0
    if args.stop_after_escalation and final['events']:
        return 0
    return 2


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (Invalid, sqlite3.Error, OSError) as exc:
        sys.exit(str(exc))
