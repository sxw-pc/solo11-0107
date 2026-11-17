"""Independent metered warehouse period close and authorized restatement."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from cdcverdict.period_close import (approve_restatement, close_period, period_report,
                                     request_restatement)
from cdcverdict.period_store import connect
from cdcverdict.period_usage import choose_contract, import_file, register_contract

FIXTURES = Path(__file__).parent / 'fixtures'


class PeriodCloseTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.db = self.root / 'period.sqlite'

    def _ready(self, contract='train_first'):
        register_contract(self.db, FIXTURES / f'period_contract_{contract}.json')
        choose_contract(self.db, FIXTURES / f'period_choice_{contract}.json')
        import_file(self.db, FIXTURES / 'period_usage.json')

    def _close(self):
        return close_period(self.db, 'tenant-a', '2026-09', '2026-10-02T00:00:00Z', 'ops-close')

    def _write(self, name, payload):
        path = self.root / name
        path.write_text(json.dumps(payload, sort_keys=True), encoding='utf-8')
        return path

    def test_signed_contract_priority_changes_billable_product_not_usage_total(self):
        print(__import__('cdcverdict').__file__)
        self._ready('train_first')
        result = self._close()
        self.assertEqual(400, result['total_cents'])
        lines = {item['product']: item for item in result['lines']}
        self.assertEqual((6, 2, 0, 4, 400), tuple(lines['dashboard'][x] for x in
                         ('actual_units', 'reserved_used', 'shared_used', 'billable_units', 'charge_cents')))
        self.assertEqual((5, 0, 5, 0), tuple(lines['train'][x] for x in
                         ('actual_units', 'reserved_used', 'shared_used', 'billable_units')))
        alternative_db = self.root / 'alternative.sqlite'
        register_contract(alternative_db, FIXTURES / 'period_contract_dashboard_first.json')
        choose_contract(alternative_db, FIXTURES / 'period_choice_dashboard_first.json')
        import_file(alternative_db, FIXTURES / 'period_usage.json')
        alternative = close_period(alternative_db, 'tenant-a', '2026-09', '2026-10-02T00:00:00Z', 'ops-close')
        self.assertEqual(1600, alternative['total_cents'])
        self.assertEqual(11, sum(item['actual_units'] for item in alternative['lines']))
        self.assertEqual(11, sum(item['actual_units'] for item in result['lines']))

    def test_late_correction_reallocates_other_products_credit_only_after_authorized_restatement(self):
        self._ready()
        first = self._close()
        import_file(self.db, FIXTURES / 'period_correction_late.json')
        report = period_report(self.db, 'tenant-a', '2026-09')
        self.assertEqual('needs_restatement', report['state']['status'])
        self.assertEqual(400, report['closes'][0]['total_cents'])
        self.assertEqual(1, len(report['closes']))
        request_restatement(self.db, FIXTURES / 'period_restatement_request.json')
        with self.assertRaisesRegex(ValueError, 'independent authorized'):
            approve_restatement(self.db, 'restate-2026-09-1', 'data-steward', 'self-proof')
        with self.assertRaisesRegex(ValueError, 'independent authorized'):
            approve_restatement(self.db, 'restate-2026-09-1', 'stranger', 'bad-proof')
        restated = approve_restatement(self.db, 'restate-2026-09-1', 'finance-a', 'finance-proof-1')
        self.assertEqual((first['close_id'], 200, -200),
                         (restated['previous_close_id'], restated['new_total_cents'], restated['delta_cents']))
        history = period_report(self.db, 'tenant-a', '2026-09')
        self.assertEqual('closed', history['state']['status'])
        self.assertEqual([400, 200], [item['total_cents'] for item in history['closes']])
        self.assertEqual([5, 3], [item['usage'][1]['effective_units'] for item in history['closes']])
        self.assertEqual('correct-train-1', history['closes'][1]['usage'][1]['correction_id'])
        self.assertEqual(-200, history['restatements'][0]['delta_cents'])

    def test_source_change_after_request_requires_fresh_candidate_and_preserves_close(self):
        self._ready()
        self._close()
        import_file(self.db, FIXTURES / 'period_correction_late.json')
        request_restatement(self.db, FIXTURES / 'period_restatement_request.json')
        second = {'corrections': [{'event_id': 'correct-train-2', 'usage_id': 'meter-train-1',
            'revision': 2, 'corrected_units': 1, 'observed_at': '2026-10-04T09:00:00Z',
            'reason': 'second meter witness revised duplicate count'}]}
        import_file(self.db, self._write('second.json', second))
        with self.assertRaisesRegex(ValueError, 'source changed'):
            approve_restatement(self.db, 'restate-2026-09-1', 'finance-a', 'proof')
        self.assertEqual(1, len(period_report(self.db, 'tenant-a', '2026-09')['closes']))
        request = json.loads((FIXTURES / 'period_restatement_request.json').read_text(encoding='utf-8'))
        request['request_id'] = 'restate-2026-09-2'
        request_restatement(self.db, self._write('request2.json', request))
        approved = approve_restatement(self.db, 'restate-2026-09-2', 'finance-a', 'proof-2')
        self.assertEqual(0, approved['new_total_cents'])

    def test_choice_freeze_close_window_and_role_boundaries(self):
        register_contract(self.db, FIXTURES / 'period_contract_train_first.json')
        register_contract(self.db, FIXTURES / 'period_contract_dashboard_first.json')
        choose_contract(self.db, FIXTURES / 'period_choice_train_first.json')
        with self.assertRaisesRegex(ValueError, 'choice is frozen'):
            choose_contract(self.db, FIXTURES / 'period_choice_dashboard_first.json')
        overlapping = json.loads((FIXTURES / 'period_contract_dashboard_first.json').read_text(encoding='utf-8'))
        overlapping.update(contract_id='warehouse-overlap', period='2026-09-overlap')
        register_contract(self.db, self._write('overlap-contract.json', overlapping))
        with self.assertRaisesRegex(ValueError, 'overlap'):
            choose_contract(self.db, self._write('overlap-choice.json',
                            {'contract_id': 'warehouse-overlap', 'signer': 'owner-a', 'proof_sha': 'overlap-proof'}))
        import_file(self.db, FIXTURES / 'period_usage.json')
        with self.assertRaisesRegex(ValueError, 'before its end'):
            close_period(self.db, 'tenant-a', '2026-09', '2026-09-30T23:59:00Z', 'ops-close')
        with self.assertRaisesRegex(ValueError, 'authorization'):
            close_period(self.db, 'tenant-a', '2026-09', '2026-10-02T00:00:00Z', 'untrusted-actor')
        self._close()
        with self.assertRaisesRegex(ValueError, 'already closed'):
            self._close()

    def test_bad_correction_replay_and_cross_kind_event_id_roll_back_file(self):
        self._ready()
        unchanged = period_report(self.db, 'tenant-a', '2026-09')['source_sha']
        bad = {'corrections': [{'event_id': 'bad-correction', 'usage_id': 'meter-train-1',
            'revision': 2, 'corrected_units': 3, 'observed_at': '2026-10-03T09:00:00Z', 'reason': 'skipped revision'}]}
        with self.assertRaisesRegex(ValueError, 'advance by one'):
            import_file(self.db, self._write('bad.json', bad))
        collision = {'corrections': [{'event_id': 'meter-train-1', 'usage_id': 'meter-train-1',
            'revision': 1, 'corrected_units': 3, 'observed_at': '2026-10-03T09:00:00Z', 'reason': 'collision'}]}
        with self.assertRaisesRegex(ValueError, 'across kind'):
            import_file(self.db, self._write('collision.json', collision))
        self.assertEqual(unchanged, period_report(self.db, 'tenant-a', '2026-09')['source_sha'])
        with closing(connect(self.db)) as db:
            self.assertEqual(2, db.execute('SELECT COUNT(*) FROM period_usage').fetchone()[0])
            self.assertEqual(0, db.execute('SELECT COUNT(*) FROM period_correction').fetchone()[0])

    def test_cli_replays_period_close_and_restatement_from_fixture_files(self):
        project = Path(__file__).resolve().parents[1]
        env = dict(os.environ, PYTHONPATH=str(project / 'src'))

        def run(*args):
            process = subprocess.run([sys.executable, '-m', 'cdcverdict', *map(str, args)],
                                     cwd=project, env=env, capture_output=True, text=True, check=True)
            return json.loads(process.stdout)

        run('period-contract', FIXTURES / 'period_contract_train_first.json', '--db', self.db)
        run('period-choose', FIXTURES / 'period_choice_train_first.json', '--db', self.db)
        run('period-import', FIXTURES / 'period_usage.json', '--db', self.db)
        self.assertEqual(400, run('period-close', '--db', self.db, '--tenant', 'tenant-a', '--period', '2026-09',
                                  '--as-of', '2026-10-02T00:00:00Z', '--actor', 'ops-close')['total_cents'])
        run('period-import', FIXTURES / 'period_correction_late.json', '--db', self.db)
        run('period-restate-request', FIXTURES / 'period_restatement_request.json', '--db', self.db)
        self.assertEqual(-200, run('period-restate-approve', '--db', self.db, '--request', 'restate-2026-09-1',
                                   '--reviewer', 'finance-a', '--proof-sha', 'finance-proof-1')['delta_cents'])
        self.assertEqual([400, 200], [x['total_cents'] for x in run('period-report', '--db', self.db,
                             '--tenant', 'tenant-a', '--period', '2026-09')['closes']])


if __name__ == '__main__':
    unittest.main()
