import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from cdcverdict.store import connect
from cdcverdict.supply import (_arbitrate, observe, register_lineage, register_slot,
                               register_worker, request_recovery, supply_report)


class PathIndependenceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.fixtures = Path(__file__).parent / 'fixtures'

    def _db(self, name='supply.sqlite'):
        return self.root / name

    def _write(self, name, value):
        path = self.root / name
        path.write_text(json.dumps(value, sort_keys=True), encoding='utf-8')
        return path

    def _observation(self, event, slot, producer, digest='digest-D', name=None):
        return self._write(name or event + '.json', {
            'event_id': event, 'slot': slot, 'producer': producer, 'digest': digest,
            'row_count': 7, 'observed_at': '2026-11-17T09:00:00Z'})

    def _lineage(self, event, producer, version, source_domain, ancestry, name=None):
        return self._write(name or event + '.json', {
            'event_id': event, 'producer': producer, 'version': version,
            'source_domain': source_domain, 'ancestry': ancestry,
            'attested_by': 'auditor-' + producer, 'proof_sha': 'proof-' + event})

    def _job(self, job, slot, priority=5):
        return self._write(job + '.json', {
            'job_id': job, 'slot': slot, 'worker': 'north-worker', 'priority': priority,
            'units': 1, 'reason': 'acquire independent source evidence'})

    def _verdicts(self, db):
        return supply_report(db)['verdicts']

    def _slot(self, db, slot):
        return next(x for x in supply_report(db)['slots'] if x['id'] == slot)

    def _prepare(self, db, slot_fixture, lineages):
        register_slot(db, self.fixtures / slot_fixture)
        register_worker(db, self.fixtures / 'recovery_worker.json')
        for fixture in lineages:
            register_lineage(db, self.fixtures / fixture)

    # ---- legacy contract -------------------------------------------------

    def test_terminal_origin_keeps_legacy_shared_relay_semantics(self):
        db = self._db()
        launched = register_slot(db, self.fixtures / 'feed_slot.json')
        self.assertEqual('terminal_origin', launched['independence_basis'])
        register_worker(db, self.fixtures / 'recovery_worker.json')
        register_lineage(db, self.fixtures / 'feed_lineage_pa.json')
        register_lineage(db, self.fixtures / 'feed_lineage_pb.json')

        observe(db, self._observation('E1', 'north-day', 'producer-a'))
        result = observe(db, self._observation('E2', 'north-day', 'producer-b'))

        # Both paths run through shared-warehouse, but the legacy guarantee only
        # compares the terminal origins and therefore still accepts.
        self.assertEqual('accepted', result['status'])
        self.assertEqual('digest-D', result['selected_digest'])
        self.assertEqual('terminal_origin', result['independence_basis'])
        self.assertEqual(['root-a', 'root-b'], result['independent_origins']['digest-D'])
        self.assertEqual([], result['selected_producers'])
        self.assertEqual({}, result['path_witness'])

    def test_slot_rejects_conflicting_or_unknown_basis(self):
        db = self._db()
        with self.assertRaisesRegex(ValueError, 'conflicting independence basis'):
            register_slot(db, self.fixtures / 'feed_slot_path.json', 'terminal_origin')
        with self.assertRaisesRegex(ValueError, 'known independence basis'):
            register_slot(db, self.fixtures / 'feed_slot.json', 'union_of_nodes')
        self.assertEqual([], supply_report(db)['slots'])

    def test_same_evidence_differs_by_business_contract(self):
        db = self._db()
        register_slot(db, self.fixtures / 'feed_slot.json')
        register_slot(db, self.fixtures / 'feed_slot_path.json')
        register_lineage(db, self.fixtures / 'feed_lineage_pa.json')
        register_lineage(db, self.fixtures / 'feed_lineage_pb.json')

        for slot in ('north-day', 'north-path'):
            observe(db, self._observation('E1-' + slot, slot, 'producer-a'))
            final = observe(db, self._observation('E2-' + slot, slot, 'producer-b'))
            self.assertEqual('digest-D' if slot == 'north-day' else None, final['selected_digest'])

        self.assertEqual('terminal_origin', self._slot(db, 'north-day')['independence_basis'])
        self.assertEqual('accepted', self._slot(db, 'north-day')['status'])
        self.assertEqual('path_disjoint', self._slot(db, 'north-path')['independence_basis'])
        self.assertEqual('collecting', self._slot(db, 'north-path')['status'])

    # ---- shared intermediate node counterexample -------------------------

    def test_path_disjoint_rejects_shared_intermediate_relay(self):
        db = self._db()
        self._prepare(db, 'feed_slot_path.json', ('feed_lineage_pa.json', 'feed_lineage_pb.json'))

        first = observe(db, self._observation('E1', 'north-path', 'producer-a'))
        self.assertEqual('collecting', first['status'])
        self.assertEqual('active', request_recovery(db, self._job('J1', 'north-path'))['status'])
        second = observe(db, self._observation('E2', 'north-path', 'producer-b'))
        self.assertEqual('collecting', second['status'])
        self.assertIsNone(second['selected_digest'])
        # Related evidence keeps recovery necessary: the job is never closed.
        self.assertNotEqual('closed', next(
            j for j in supply_report(db)['jobs'] if j['id'] == 'J1')['status'])

        verdict = json.loads(self._verdicts(db)[-1]['evidence'])
        self.assertEqual('path_disjoint', verdict['independence_basis'])
        self.assertEqual([], verdict['selected_producers'])
        self.assertEqual(['relay-a', 'shared-warehouse', 'root-a'],
                         verdict['candidate_paths_by_digest']['digest-D']['producer-a'])
        self.assertEqual(['relay-b', 'shared-warehouse', 'root-b'],
                         verdict['candidate_paths_by_digest']['digest-D']['producer-b'])

        # An attested path that drops the shared node restores independent evidence.
        changed = register_lineage(db, self.fixtures / 'feed_lineage_pb_disjoint.json')
        self.assertEqual(['north-path'], changed['reopened_slots'])
        slot = self._slot(db, 'north-path')
        self.assertEqual('accepted', slot['status'])
        self.assertEqual('digest-D', slot['selected_digest'])
        final = json.loads(self._verdicts(db)[-1]['evidence'])
        self.assertEqual(['producer-a', 'producer-b'], final['selected_producers'])
        self.assertEqual(['relay-a', 'shared-warehouse', 'root-a'],
                         final['path_witness']['producer-a']['ancestry'])
        self.assertEqual(['relay-b', 'root-b'], final['path_witness']['producer-b']['ancestry'])
        self.assertEqual(2, final['path_witness']['producer-b']['version'])

    # ---- exact maximum instead of greedy --------------------------------

    def test_exact_selection_beats_producer_order_greedy(self):
        db = self._db()
        self._prepare(db, 'feed_slot_path.json', (
            'feed_lineage_greedy_a.json', 'feed_lineage_greedy_b.json',
            'feed_lineage_greedy_c.json'))

        # A shares r1 with B and r2 with C; only B+C are pairwise disjoint.
        # Greedy over sorted producers would pick A and report a single vote.
        observe(db, self._observation('E1', 'north-path', 'producer-a'))
        observe(db, self._observation('E2', 'north-path', 'producer-b'))
        result = observe(db, self._observation('E3', 'north-path', 'producer-c'))

        self.assertEqual('accepted', result['status'])
        self.assertEqual('digest-D', result['selected_digest'])
        self.assertEqual(['producer-b', 'producer-c'], result['selected_producers'])
        self.assertEqual(['relay-b', 'r1', 'r3'],
                         result['path_witness']['producer-b']['ancestry'])
        self.assertEqual(['relay-c', 'r2', 'r4'],
                         result['path_witness']['producer-c']['ancestry'])
        self.assertNotIn('producer-a', result['path_witness'])

        verdict = json.loads(self._verdicts(db)[-1]['evidence'])
        self.assertEqual(['producer-b', 'producer-c'], verdict['selected_producers'])
        self.assertEqual({'producer-a', 'producer-b', 'producer-c'},
                         set(verdict['candidate_paths_by_digest']['digest-D']))

    def test_quorum_three_requires_all_three_disjoint_paths(self):
        db = self._db()
        slot = self._write('slot3.json', {
            'slot': 'north-three', 'partition': 'north', 'deadline': '2026-11-18T00:00:00Z',
            'quorum': 3, 'independence_basis': 'path_disjoint'})
        register_slot(db, slot)
        register_lineage(db, self.fixtures / 'feed_lineage_greedy_a.json')
        register_lineage(db, self.fixtures / 'feed_lineage_greedy_b.json')
        register_lineage(db, self.fixtures / 'feed_lineage_greedy_c.json')

        observe(db, self._observation('E1', 'north-three', 'producer-a'))
        observe(db, self._observation('E2', 'north-three', 'producer-b'))
        result = observe(db, self._observation('E3', 'north-three', 'producer-c'))
        # Maximum disjoint set is still 2 (B+C), so quorum 3 is not met.
        self.assertEqual('collecting', result['status'])
        self.assertEqual([], result['selected_producers'])
        self.assertIsNone(result['selected_digest'])

    # ---- repeated producer ----------------------------------------------

    def test_repeated_producer_casts_only_one_vote(self):
        db = self._db()
        self._prepare(db, 'feed_slot_path.json', ('feed_lineage_pa.json',))
        observe(db, self._observation('E1', 'north-path', 'producer-a'))
        repeated = observe(db, self._observation('E2', 'north-path', 'producer-a'))
        self.assertEqual('collecting', repeated['status'])
        self.assertEqual([], repeated['selected_producers'])
        # A was always credited with exactly one path, never two.
        self.assertEqual(['producer-a'], list(
            json.loads(self._verdicts(db)[-1]['evidence'])['candidate_paths_by_digest']['digest-D']))

    # ---- witness determinism and order independence ---------------------

    def test_witness_is_deterministic_across_input_order(self):
        lineage_fixtures = ('feed_lineage_greedy_a.json', 'feed_lineage_greedy_b.json',
                            'feed_lineage_greedy_c.json')
        observations = [
            ('E1', 'producer-a'), ('E2', 'producer-b'), ('E3', 'producer-c')]
        verdicts = []
        for name, order in (('first.sqlite', observations),
                            ('second.sqlite', list(reversed(observations)))):
            db = self._db(name)
            self._prepare(db, 'feed_slot_path.json', tuple(reversed(lineage_fixtures)))
            seen = None
            for event, producer in order:
                seen = observe(db, self._observation(event, 'north-path', producer))
            verdicts.append((seen, json.loads(self._verdicts(db)[-1]['evidence'])))
            # Re-arbitrating the settled slot yields the identical witness.
            with closing(connect(db)) as conn:
                again = _arbitrate(conn, 'north-path')
                conn.commit()
            verdicts.append((again, json.loads(self._verdicts(db)[-1]['evidence'])))

        reference = verdicts[0]
        self.assertEqual('accepted', reference[0]['status'])
        for result, evidence in verdicts[1:]:
            self.assertEqual(reference[0]['selected_producers'], result['selected_producers'])
            self.assertEqual(reference[0]['path_witness'], result['path_witness'])
            self.assertEqual(reference[1], evidence)

    # ---- lineage revision, verdict revision and recovery jobs -----------

    def test_lineage_change_reopens_accepted_path_verdict(self):
        db = self._db()
        self._prepare(db, 'feed_slot_path.json', ('feed_lineage_pa.json',))
        register_lineage(db, self.fixtures / 'feed_lineage_pb_disjoint.json')
        observe(db, self._observation('E1', 'north-path', 'producer-a'))
        accepted = observe(db, self._observation('E2', 'north-path', 'producer-b'))
        self.assertEqual('accepted', accepted['status'])
        prior_revision = accepted['revision']

        # producer-b's newly attested path now shares shared-warehouse with a.
        changed = register_lineage(db, self.fixtures / 'feed_lineage_pb_shared_v3.json')
        self.assertEqual(['north-path'], changed['reopened_slots'])

        slot = self._slot(db, 'north-path')
        self.assertEqual('collecting', slot['status'])
        self.assertEqual(prior_revision + 1, slot['revision'])
        self.assertIsNone(slot['selected_digest'])
        final = json.loads(self._verdicts(db)[-1]['evidence'])
        self.assertEqual('path_disjoint', final['independence_basis'])
        self.assertEqual([], final['selected_producers'])
        self.assertEqual({}, final['path_witness'])

    def test_lineage_change_invalidates_bound_recovery_job(self):
        db = self._db()
        self._prepare(db, 'feed_slot_path.json', ('feed_lineage_pa.json',))
        observe(db, self._observation('E1', 'north-path', 'producer-a'))
        self.assertEqual('active', request_recovery(db, self._job('J1', 'north-path'))['status'])

        revised = self._lineage('lineage-pa-v2', 'producer-a', 2, 'relay-a',
                                ['relay-a', 'other-warehouse', 'root-z'])
        register_lineage(db, revised)
        report = supply_report(db)
        self.assertEqual('stale', next(j for j in report['jobs'] if j['id'] == 'J1')['status'])
        self.assertEqual('collecting',
                         next(s for s in report['slots'] if s['id'] == 'north-path')['status'])

    # ---- fail-closed exactness bound ------------------------------------

    def test_search_limit_yields_indeterminate_and_never_accepts(self):
        db = self._db()
        self._prepare(db, 'feed_slot_path.json',
                      ('feed_lineage_pa.json', 'feed_lineage_pb.json'))
        # Both paths share shared-warehouse: the exact maximum is 1.
        observe(db, self._observation('E1', 'north-path', 'producer-a'))
        self.assertEqual('active', request_recovery(db, self._job('J1', 'north-path'))['status'])

        env = dict(os.environ, PYTHONPATH=str(Path(__file__).parents[1] / 'src'),
                   CDEVERDICT_PATH_SEARCH_LIMIT='1')
        subprocess.run([sys.executable, '-m', 'cdcverdict', 'feed-observe',
                        str(self._observation('E2', 'north-path', 'producer-b')),
                        '--db', str(db)], check=True, capture_output=True, env=env)

        self.assertEqual('indeterminate', self._slot(db, 'north-path')['status'])
        evidence = json.loads(self._verdicts(db)[-1]['evidence'])
        self.assertIn('search limit', evidence['indeterminate_reason'])
        self.assertEqual([], evidence['selected_producers'])
        self.assertIsNone(self._verdicts(db)[-1]['selected_digest'])
        # Fail-closed means an unproven maximum can never close recovery work.
        job = next(j for j in supply_report(db)['jobs'] if j['id'] == 'J1')
        self.assertNotEqual('closed', job['status'])
        self.assertEqual('stale', job['status'])

        # A later adjudication with the normal bound still refuses to accept a
        # shared path, but restores the proven collecting verdict.
        with closing(connect(db)) as conn:
            proven = _arbitrate(conn, 'north-path')
            conn.commit()
        self.assertEqual('collecting', proven['status'])
        self.assertEqual([], proven['selected_producers'])
        self.assertIsNone(proven['selected_digest'])

    def test_search_limit_override_via_environment_matches_public_path(self):
        db = self._db()
        self._prepare(db, 'feed_slot_path.json',
                      ('feed_lineage_greedy_a.json', 'feed_lineage_greedy_b.json',
                       'feed_lineage_greedy_c.json'))
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).parents[1] / 'src'),
                   CDEVERDICT_PATH_SEARCH_LIMIT='2')
        for event, producer in (('E1', 'producer-a'), ('E2', 'producer-b'), ('E3', 'producer-c')):
            subprocess.run([sys.executable, '-m', 'cdcverdict', 'feed-observe',
                            str(self._observation(event, 'north-path', producer)),
                            '--db', str(db)], check=True, capture_output=True, env=env)
        # The exact B+C maximum cannot be proven inside two nodes, so the slot is
        # indeterminate rather than a greedy single-producer acceptance.
        self.assertEqual('indeterminate', self._slot(db, 'north-path')['status'])
        self.assertIsNone(self._slot(db, 'north-path')['selected_digest'])

        with closing(connect(db)) as conn:
            proven = _arbitrate(conn, 'north-path')
            conn.commit()
        self.assertEqual('accepted', proven['status'])
        self.assertEqual(['producer-b', 'producer-c'], proven['selected_producers'])

    # ---- CLI wiring ------------------------------------------------------

    def test_cli_exposes_independence_basis(self):
        db = self._db('cli.sqlite')
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).parents[1] / 'src'))
        subprocess.run([sys.executable, '-m', 'cdcverdict', 'feed-slot',
                        str(self.fixtures / 'feed_slot_path.json'), '--db', str(db),
                        '--independence-basis', 'path_disjoint'],
                       check=True, capture_output=True, env=env)
        report = json.loads(subprocess.run(
            [sys.executable, '-m', 'cdcverdict', 'supply-report', '--db', str(db)],
            check=True, capture_output=True, text=True, env=env).stdout)
        self.assertEqual('path_disjoint', report['slots'][0]['independence_basis'])


if __name__ == '__main__':
    unittest.main()
