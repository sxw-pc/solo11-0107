import json
import hashlib
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from cdcverdict.dispatch import (complete_dispatch, dispatch_report, dispatch_tick,
                                 queue_dispatch, register_dispatch_worker, renew_dispatch)
from cdcverdict.supply import (observe, register_lineage, register_slot, register_worker,
                               request_recovery, supply_report)


class DispatchExecutionTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / 'dispatch.db'
        self.fixtures = Path(__file__).parent / 'fixtures'
        register_slot(self.db, self.fixtures / 'feed_slot.json')
        register_worker(self.db, self.fixtures / 'recovery_worker.json')
        register_lineage(self.db, self.fixtures / 'feed_lineage_a.json')
        self._write('observation.json', {'event_id': 'obs-1', 'slot': 'north-day',
                    'producer': 'producer-a', 'digest': 'digest-A', 'row_count': 7,
                    'observed_at': '2026-11-17T09:00:00Z'})
        observe(self.db, self.root / 'observation.json')
        register_dispatch_worker(self.db, self.fixtures / 'dispatch_worker_a.json')

    def _write(self, name, value):
        path = self.root / name
        path.write_text(json.dumps(value, sort_keys=True), encoding='utf-8')
        return path

    def _queue(self, job, tenant):
        request_recovery(self.db, self._write(job + '.json', {
            'job_id': job, 'slot': 'north-day', 'worker': 'north-worker',
            'priority': 1, 'units': 1, 'reason': 'missing independent source'}))
        return queue_dispatch(self.db, self._write(job + '-dispatch.json', {
            'task_id': 'task-' + job, 'job_id': job, 'tenant': tenant, 'lease_ticks': 3}))

    def _artifact(self, name):
        path = self.root / name
        path.write_bytes(('acquired evidence for ' + name).encode())
        return path

    def test_expiry_reassigns_to_other_worker_and_fences_old_completion(self):
        print(__import__('cdcverdict').__file__)
        self._queue('j1', 'tenant-a')
        first = dispatch_tick(self.db, 10)
        self.assertEqual([('exec-a', 1)], [(x['worker'], x['generation']) for x in first])
        register_dispatch_worker(self.db, self.fixtures / 'dispatch_worker_b.json')
        second = dispatch_tick(self.db, 13)
        self.assertEqual([('exec-b', 2)], [(x['worker'], x['generation']) for x in second])
        with self.assertRaisesRegex(ValueError, 'stale or expired'):
            complete_dispatch(self.db, 'task-j1', 'exec-a', 1, 13, self._artifact('old-result'))
        self.assertEqual('active', supply_report(self.db)['jobs'][0]['status'])
        evidence = self._artifact('current-result')
        result = complete_dispatch(self.db, 'task-j1', 'exec-b', 2, 14, evidence)
        self.assertEqual('completed', result['status'])
        self.assertEqual('completed', supply_report(self.db)['jobs'][0]['status'])
        report = dispatch_report(self.db)
        self.assertEqual(hashlib.sha256(evidence.read_bytes()).hexdigest(), report['tasks'][0]['artifact_sha'])
        self.assertEqual(len(evidence.read_bytes()), report['artifacts'][0]['bytes'])
        self.assertEqual(['queued', 'leased', 'expired', 'leased', 'completed'],
                         [event['action'] for event in report['events']])

    def test_tenants_take_turns_after_capacity_released(self):
        self._queue('a1', 'tenant-a')
        self._queue('a2', 'tenant-a')
        self._queue('b1', 'tenant-b')
        first = dispatch_tick(self.db, 1)
        self.assertEqual('task-a1', first[0]['task_id'])
        complete_dispatch(self.db, 'task-a1', 'exec-a', 1, 2, self._artifact('artifact-a1'))
        second = dispatch_tick(self.db, 2)
        self.assertEqual('task-b1', second[0]['task_id'])
        complete_dispatch(self.db, 'task-b1', 'exec-a', 1, 3, self._artifact('artifact-b1'))
        third = dispatch_tick(self.db, 3)
        self.assertEqual('task-a2', third[0]['task_id'])
        self.assertEqual(['task-a1', 'task-b1', 'task-a2'],
                         [event['task_id'] for event in dispatch_report(self.db)['events'] if event['action'] == 'leased'])

    def test_source_revision_cancels_lease_and_rejects_commit(self):
        self._queue('j1', 'tenant-a')
        dispatch_tick(self.db, 1)
        observe(self.db, self._write('late.json', {'event_id': 'obs-2', 'slot': 'north-day',
            'producer': 'producer-a', 'digest': 'digest-B', 'row_count': 7,
            'observed_at': '2026-11-17T10:00:00Z'}))
        with self.assertRaisesRegex(ValueError, 'recovery job changed'):
            complete_dispatch(self.db, 'task-j1', 'exec-a', 1, 2, self._artifact('artifact-stale'))
        self.assertEqual([], dispatch_tick(self.db, 2))
        self.assertEqual('canceled', dispatch_report(self.db)['tasks'][0]['status'])

    def test_live_worker_renews_lease_and_blocks_premature_takeover(self):
        self._queue('j1', 'tenant-a')
        dispatch_tick(self.db, 10)
        register_dispatch_worker(self.db, self.fixtures / 'dispatch_worker_b.json')
        renewed = renew_dispatch(self.db, 'task-j1', 'exec-a', 1, 12)
        self.assertEqual(15, renewed['expires_at'])
        self.assertEqual([], dispatch_tick(self.db, 13))
        with self.assertRaisesRegex(ValueError, 'stale or expired'):
            renew_dispatch(self.db, 'task-j1', 'exec-b', 1, 14)
        self.assertEqual('exec-a', dispatch_report(self.db)['tasks'][0]['worker_id'])
        self.assertEqual('completed', complete_dispatch(
            self.db, 'task-j1', 'exec-a', 1, 14, self._artifact('renewed-result'))['status'])

    def test_cli_replays_fixture_and_persists_evidence(self):
        request_recovery(self.db, self.fixtures / 'recovery_low.json')
        project = Path(__file__).parents[1]
        env = dict(os.environ, PYTHONPATH=str(project / 'src'))

        def run(*args):
            result = subprocess.run([sys.executable, '-m', 'cdcverdict', *map(str, args)],
                                    cwd=project, env=env, text=True, capture_output=True, check=True)
            return json.loads(result.stdout)

        self.assertEqual('pending', run('dispatch-queue', self.fixtures / 'dispatch_task_low.json',
                                        '--db', self.db)['status'])
        self.assertEqual('dispatch-low', run('dispatch-tick', '--db', self.db, '--tick', 1)[0]['task_id'])
        result = run('dispatch-complete', '--db', self.db, '--task', 'dispatch-low',
                     '--worker', 'exec-a', '--generation', 1, '--tick', 2,
                     '--artifact', self.fixtures / 'dispatch_acquisition.txt')
        self.assertEqual('completed', result['status'])
        report = run('dispatch-report', '--db', self.db)
        self.assertEqual('completed', report['tasks'][0]['status'])
        self.assertEqual(hashlib.sha256((self.fixtures / 'dispatch_acquisition.txt').read_bytes()).hexdigest(),
                         report['artifacts'][0]['source_sha'])

    def test_same_worker_new_generation_fences_old_attempt(self):
        self._queue('j1', 'tenant-a')
        self.assertEqual(1, dispatch_tick(self.db, 1)[0]['generation'])
        self.assertEqual([], dispatch_tick(self.db, 1))
        with self.assertRaisesRegex(ValueError, 'stale or expired'):
            complete_dispatch(self.db, 'task-j1', 'exec-a', 1, 4, self._artifact('too-late'))
        self.assertEqual(2, dispatch_tick(self.db, 4)[0]['generation'])
        with self.assertRaisesRegex(ValueError, 'stale or expired'):
            complete_dispatch(self.db, 'task-j1', 'exec-a', 1, 4, self._artifact('old-token'))
        self.assertEqual('completed', complete_dispatch(
            self.db, 'task-j1', 'exec-a', 2, 5, self._artifact('new-token'))['status'])
        with self.assertRaisesRegex(ValueError, 'backwards'):
            dispatch_tick(self.db, 3)


if __name__ == '__main__':
    unittest.main()
