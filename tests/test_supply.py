import json
import tempfile
import unittest
from pathlib import Path

from cdcverdict.supply import (complete_recovery, observe, register_slot,
                               register_worker, request_recovery, supply_report)


class SupplyWorkflowTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / 'supply.sqlite'
        self.fixture = Path(__file__).parent / 'fixtures'
        register_slot(self.db, self.fixture / 'feed_slot.json')
        register_slot(self.db, self.fixture / 'feed_slot_east.json')
        register_worker(self.db, self.fixture / 'recovery_worker.json')

    def _write(self, name, value):
        path = self.root / name
        path.write_text(json.dumps(value, sort_keys=True), encoding='utf-8')
        return path

    def _observation(self, event, slot, producer, digest):
        return self._write(event + '.json', {'event_id': event, 'slot': slot,
            'producer': producer, 'digest': digest, 'row_count': 7,
            'observed_at': '2026-11-17T09:00:00Z'})

    def _job(self, job, slot, priority):
        return self._write(job + '.json', {'job_id': job, 'slot': slot,
            'worker': 'north-worker', 'priority': priority, 'units': 1,
            'reason': 'acquire independent source evidence'})

    def test_independent_quorum_and_late_conflict_reopen_source(self):
        print(__import__('cdcverdict').__file__)
        first = observe(self.db, self._observation('E1', 'north-day', 'producer-a', 'digest-A'))
        self.assertEqual('collecting', first['status'])
        repeated = observe(self.db, self._observation('E2', 'north-day', 'producer-a', 'digest-A'))
        self.assertEqual('collecting', repeated['status'])
        accepted = observe(self.db, self._observation('E3', 'north-day', 'producer-b', 'digest-A'))
        self.assertEqual('accepted', accepted['status'])
        self.assertEqual(['producer-a', 'producer-b'], accepted['independent_producers']['digest-A'])
        disputed = observe(self.db, self._observation('E4', 'north-day', 'producer-c', 'digest-B'))
        self.assertEqual('disputed', disputed['status'])
        report = supply_report(self.db)
        self.assertEqual(['collecting', 'collecting', 'accepted', 'disputed'],
                         [x['status'] for x in report['verdicts']])
        self.assertIsNone(next(x for x in report['slots'] if x['id'] == 'north-day')['selected_digest'])

    def test_capacity_priority_release_and_version_invalidation(self):
        observe(self.db, self._observation('E1', 'north-day', 'producer-a', 'digest-A'))
        observe(self.db, self._observation('E2', 'east-day', 'producer-a', 'digest-X'))
        low = request_recovery(self.db, self._job('J-low', 'north-day', 2))
        self.assertEqual('active', low['status'])
        high = request_recovery(self.db, self._job('J-high', 'east-day', 9))
        self.assertEqual('active', high['status'])
        report = supply_report(self.db)
        self.assertEqual({'J-high': 'active', 'J-low': 'waiting'},
                         {j['id']: j['status'] for j in report['jobs']})
        complete_recovery(self.db, 'J-high')
        report = supply_report(self.db)
        self.assertEqual({'J-high': 'completed', 'J-low': 'active'},
                         {j['id']: j['status'] for j in report['jobs']})
        observe(self.db, self._observation('E3', 'north-day', 'producer-b', 'digest-B'))
        report = supply_report(self.db)
        self.assertEqual('stale', next(j for j in report['jobs'] if j['id'] == 'J-low')['status'])
        self.assertEqual('disputed', next(s for s in report['slots'] if s['id'] == 'north-day')['status'])

    def test_replay_and_conflicting_id_do_not_mutate_ledger(self):
        path = self._observation('E1', 'north-day', 'producer-a', 'digest-A')
        observe(self.db, path)
        self.assertTrue(observe(self.db, path)['idempotent'])
        before = supply_report(self.db)
        path.write_text(path.read_text(encoding='utf-8').replace('digest-A', 'digest-B'), encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'event id reused'):
            observe(self.db, path)
        self.assertEqual(before['verdicts'], supply_report(self.db)['verdicts'])

    def test_accepted_delivery_closes_unnecessary_recovery(self):
        observe(self.db, self._observation('E1', 'north-day', 'producer-a', 'digest-A'))
        request_recovery(self.db, self._job('J1', 'north-day', 5))
        observe(self.db, self._observation('E2', 'north-day', 'producer-b', 'digest-A'))
        report = supply_report(self.db)
        self.assertEqual('closed', report['jobs'][0]['status'])
        with self.assertRaisesRegex(ValueError, 'only an active'):
            complete_recovery(self.db, 'J1')


if __name__ == '__main__':
    unittest.main()
