import copy
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

import controller as c

CONFIG = json.loads((c.BASE / 'demo.json').read_text())


class ControllerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = Path(self.tmp.name) / 'state'
        self.ctl = c.Controller(self.state)
        self.addCleanup(self.ctl.db.close)

    def config(self, *ids, budget=10):
        tasks = [copy.deepcopy(t) for t in CONFIG['tasks'] if t['id'] in ids]
        for task in tasks:
            task['depends_on'] = [d for d in task['depends_on'] if d in ids]
        return dict(budget=budget, tasks=tasks)

    def cli(self, *args, code=0):
        p = subprocess.run([sys.executable, '-B', str(c.BASE / 'controller.py'), '--state-dir', str(self.state), *args],
                           text=True, capture_output=True, check=False, timeout=15)
        self.assertEqual(p.returncode, code, p.stderr or p.stdout)
        return json.loads(p.stdout)

    def test_schema_dag_cycle_and_contract_drift(self):
        config = self.config('process', 'verify')
        config['tasks'][0]['depends_on'] = ['verify']
        with self.assertRaisesRegex(c.Invalid, 'cyclic'):
            self.ctl.initialize(config)
        config['tasks'][0]['depends_on'] = []
        self.ctl.initialize(config)
        self.ctl.initialize(config)
        config['tasks'][0]['description'] = 'Different'
        with self.assertRaisesRegex(c.Invalid, 'drift'):
            self.ctl.initialize(config)

    def test_lease_fencing_and_resource_lock(self):
        cfg = self.config('process', 'verify')
        cfg['tasks'][1]['depends_on'] = []
        self.ctl.initialize(cfg)
        first = self.ctl.claim()
        competing = c.Controller(self.state)
        self.addCleanup(competing.db.close)
        self.assertIsNone(competing.claim())
        self.ctl.heartbeat(first)
        self.ctl.db.execute('UPDATE tasks SET expires=0 WHERE id=?', (first['id'],))
        second = competing.claim()
        self.assertNotEqual(first['directory'], second['directory'])
        with self.assertRaisesRegex(c.Invalid, 'lease expired'):
            self.ctl.finish(first, 'stale')
        self.assertEqual(competing.status()['attempts'][0]['outcome'], 'interrupted')

    def test_result_gate_rejects_threshold_override_and_wrong_values(self):
        self.ctl.initialize(self.config('verify'))
        work = self.ctl.claim()
        directory = self.state / work['directory']
        directory.mkdir(parents=True)
        path = directory / 'result.json'
        expected = dict(version=1, task_id='verify', attempt=1, kind='statistics',
                        evidence=dict(count=4, total=40, mean_milli=10000))
        for record in (dict(expected, acceptance='accept-all'), dict(expected, evidence=dict(count=4, total=0, mean_milli=0))):
            path.write_text(json.dumps(record))
            with self.assertRaises(c.Invalid):
                c.validate_result(path, work['task'], 1)
        self.assertEqual(self.ctl.finish(work), 'validation_rejected')
        self.assertEqual(self.ctl.status()['tasks'][0]['state'], 'stopped')

    def test_duplicate_route_repeat_error_and_worker_isolation(self):
        self.ctl.initialize(self.config('process', 'verify'))
        for _ in range(2):
            self.assertEqual(self.ctl.execute(self.ctl.claim()), 'fixture_decimal_parse')
        event, = self.ctl.events()
        self.assertEqual(self.ctl.route(event['id'], 'ticket-native', 'session-native'), 'stored')
        self.assertEqual(self.ctl.route(event['id'], 'ticket-native', 'session-native'), 'duplicate')
        with self.assertRaisesRegex(c.Invalid, 'conflicting'):
            self.ctl.route(event['id'], 'ticket-other', 'session-other')
        recovery = self.ctl.claim()
        self.assertEqual(recovery['id'], event['recovery_id'])
        self.assertIsNone(self.ctl.execute(recovery))
        self.assertEqual(self.ctl.issues()[0]['state'], 'closed')
        work = self.ctl.claim()
        self.assertEqual(work['checkpoint'], 'decimal-v1')
        self.assertEqual(self.ctl.finish(work, 'fixture_decimal_parse'), 'fixture_decimal_parse')
        self.ctl.claim()  # Stops descendants; no second recovery event.
        state = self.ctl.status()
        self.assertEqual((len(state['events']), len(state['issues'])), (1, 1))
        self.assertEqual(next(t['reason'] for t in state['tasks'] if t['id'] == 'process'), 'repeat_after_recovery')
        self.assertEqual(next(t['reason'] for t in state['tasks'] if t['id'] == 'verify'), 'dependency_stopped')
        self.assertEqual(len({a['directory'] for a in state['attempts']}), 4)
        self.assertIn('/recovery-worker/', recovery['directory'])
        self.assertIn('/primary-worker/', work['directory'])

    def test_budget_and_executor_stop(self):
        self.ctl.initialize(self.config('process', 'verify', budget=1))
        self.ctl.execute(self.ctl.claim())
        self.assertIsNone(self.ctl.claim())
        self.ctl.claim()
        state = self.ctl.status()
        self.assertEqual(state['run'], [{'budget': 1, 'spent': 1}])
        self.assertEqual(next(t['reason'] for t in state['tasks'] if t['id'] == 'process'), 'budget_limit')
        task = CONFIG['tasks'][1]
        self.assertEqual(c.select_executor(task, ('codex', 'dsh')), 'dsh')
        self.assertEqual(c.select_executor(task, ('codex',)), 'codex')
        self.assertIsNone(c.select_executor(dict(task, executor='dsh')))
        self.assertFalse(state['issues'])

    def test_invoker_exit_keeps_compute_job_and_run_resumes(self):
        detached = self.cli('demo', '--detach-on-submit', code=75)['job']
        self.assertIn(detached['status'], ('queued', 'running'))
        self.assertIsNone(detached['result'])
        intermediate = self.cli('demo', '--stop-after-escalation')
        task = next(t for t in intermediate['tasks'] if t['id'] == 'detached')
        self.assertEqual((task['state'], task['job_id'], task['tries']), ('succeeded', detached['jobId'], 2))
        self.assertEqual(intermediate['attempts'][0]['outcome'], 'interrupted')
        self.assertEqual(len(intermediate['events']), 1)
        self.cli('demo', '--require-native', code=2)
        final = self.cli('demo')
        self.assertTrue(all(t['state'] == 'succeeded' for t in final['tasks']))
        self.assertEqual(final['run'][0]['spent'], 7)
        self.assertEqual(final['issues'][0]['state'], 'closed')
        results = [a for a in final['attempts'] if a['task_id'] in ('process', 'verify') and a['outcome'] == 'validated']
        self.assertEqual(len(results), 2)
        self.assertEqual([json.loads((self.state / a['directory'] / 'result.json').read_text())['evidence'] for a in results],
                         [{'count': 4, 'total': 40, 'mean_milli': 10000}] * 2)


if __name__ == '__main__':
    unittest.main()
