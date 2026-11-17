"""Signed in-period price amendments: event-time pricing and authorized restatement."""

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from cdcverdict.period_close import (approve_restatement, close_period, period_report,
                                     request_restatement)
from cdcverdict.period_usage import choose_contract, import_file, register_contract, sign_amendment

FIXTURES = Path(__file__).parent / 'fixtures'
CONTRACT_SHA = hashlib.sha256((FIXTURES / 'period_contract_train_first.json').read_bytes()).hexdigest()


class PeriodAmendmentTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.db = self.root / 'period.sqlite'

    def _ready(self):
        register_contract(self.db, FIXTURES / 'period_contract_train_first.json')
        choose_contract(self.db, FIXTURES / 'period_choice_train_first.json')
        import_file(self.db, FIXTURES / 'period_usage.json')

    def _close(self):
        return close_period(self.db, 'tenant-a', '2026-09', '2026-10-02T00:00:00Z', 'ops-close')

    def _write(self, name, payload):
        path = self.root / name
        path.write_text(json.dumps(payload, sort_keys=True), encoding='utf-8')
        return path

    def _amendment(self, name, **overrides):
        payload = {'amendment_id': 'amend-dynamic-1', 'tenant': 'tenant-a', 'period': '2026-09',
                   'contract_id': 'warehouse-train-first', 'contract_sha': CONTRACT_SHA,
                   'effective_at': '2026-09-20T00:00:00Z', 'clause': 'prospective',
                   'prices_cents': {'dashboard': 300}, 'signer': 'owner-a',
                   'proof_sha': 'dynamic-proof'}
        payload.update(overrides)
        return self._write(name, payload)

    def _request(self, name='request.json', request_id='restate-2026-09-1'):
        return self._write(name, {'request_id': request_id, 'tenant': 'tenant-a', 'period': '2026-09',
                                  'previous_close_id': 1, 'requester': 'data-steward',
                                  'reason': 'signed amendment and late correction reprice the period'})

    def _lines(self, result):
        return {line['product']: line for line in result['lines']}

    def test_prospective_amendment_keeps_old_price_for_usage_before_effective_at(self):
        self._ready()
        first = self._close()
        self.assertEqual(400, first['total_cents'])
        import_file(self.db, FIXTURES / 'period_correction_late.json')
        sign_amendment(self.db, FIXTURES / 'period_amendment_prospective.json')
        report = period_report(self.db, 'tenant-a', '2026-09')
        self.assertEqual('needs_restatement', report['state']['status'])
        self.assertEqual([400], [close['total_cents'] for close in report['closes']])
        self.assertEqual('amend-dashboard-300-prospective', report['amendments'][0]['id'])
        request_restatement(self.db, self._request())
        restated = approve_restatement(self.db, 'restate-2026-09-1', 'finance-a', 'finance-proof-1')
        # 看板事件 occurred_at 2026-09-15 早于 effective_at 2026-09-20，仍按 100 分计。
        self.assertEqual((200, -200), (restated['new_total_cents'], restated['delta_cents']))
        dashboard = self._lines(restated)['dashboard']
        self.assertEqual((2, 100, 200), (dashboard['billable_units'], dashboard['price_cents'],
                                         dashboard['charge_cents']))
        self.assertEqual({'dashboard': -200, 'train': 0},
                         {row['product']: row['delta_cents'] for row in restated['product_deltas']})
        history = period_report(self.db, 'tenant-a', '2026-09')
        self.assertEqual([400, 200], [close['total_cents'] for close in history['closes']])
        self.assertEqual([], history['closes'][0]['amendments'])
        self.assertEqual(['amend-dashboard-300-prospective'], history['closes'][1]['amendments'])
        self.assertEqual('closed', history['state']['status'])
        restatement = history['restatements'][0]
        self.assertEqual({'dashboard': (400, 200), 'train': (0, 0)},
                         {row['product']: (row['old_charge_cents'], row['new_charge_cents'])
                          for row in restatement['lines']})

    def test_retroactive_amendment_reprices_all_period_usage(self):
        self._ready()
        self._close()
        import_file(self.db, FIXTURES / 'period_correction_late.json')
        sign_amendment(self.db, FIXTURES / 'period_amendment_retroactive.json')
        request_restatement(self.db, self._request())
        restated = approve_restatement(self.db, 'restate-2026-09-1', 'finance-a', 'finance-proof-1')
        # retroactive 按签署文本追溯：看板 2 个计费单位全部按 300 分。
        self.assertEqual((600, 200), (restated['new_total_cents'], restated['delta_cents']))
        dashboard = self._lines(restated)['dashboard']
        self.assertEqual((2, 300, 600), (dashboard['billable_units'], dashboard['price_cents'],
                                         dashboard['charge_cents']))
        history = period_report(self.db, 'tenant-a', '2026-09')
        self.assertEqual([400, 600], [close['total_cents'] for close in history['closes']])
        self.assertEqual({'dashboard': 200, 'train': 0},
                         {row['product']: row['delta_cents'] for row in history['restatements'][0]['lines']})

    def test_correction_observed_at_never_replaces_usage_occurred_at_for_pricing(self):
        self._ready()
        self._close()
        late = {'corrections': [{'event_id': 'correct-train-up', 'usage_id': 'meter-train-1',
                'revision': 1, 'corrected_units': 8, 'observed_at': '2026-10-03T09:00:00Z',
                'reason': 'meter replay found three uncounted jobs'}]}
        import_file(self.db, self._write('late-up.json', late))
        sign_amendment(self.db, self._amendment('train-amend.json', amendment_id='amend-train-100',
                                                prices_cents={'train': 100}))
        request_restatement(self.db, self._request())
        restated = approve_restatement(self.db, 'restate-2026-09-1', 'finance-a', 'finance-proof-1')
        # 训练更正后 8 单位：共享 5 后 3 个计费单位。事件 occurred_at 2026-09-15 早于
        # effective_at，prospective 仍按 400 分；observed_at 2026-10-03 不参与定价。
        lines = self._lines(restated)
        self.assertEqual((3, 400, 1200), (lines['train']['billable_units'], lines['train']['price_cents'],
                                          lines['train']['charge_cents']))
        self.assertEqual(1600, restated['new_total_cents'])
        snapshot = {row['usage_id']: row for row in
                    period_report(self.db, 'tenant-a', '2026-09')['closes'][1]['usage']}
        self.assertEqual((8, 3, 400, 1200), (snapshot['meter-train-1']['effective_units'],
                                             snapshot['meter-train-1']['billable_units'],
                                             snapshot['meter-train-1']['price_cents'],
                                             snapshot['meter-train-1']['charge_cents']))

    def test_prospective_boundary_event_exactly_at_effective_at_takes_new_price(self):
        self._ready()
        sign_amendment(self.db, self._amendment('boundary-on.json', amendment_id='amend-boundary-on',
                                                effective_at='2026-09-15T10:00:00Z'))
        result = self._close()
        dashboard = self._lines(result)['dashboard']
        self.assertEqual((4, 300, 1200), (dashboard['billable_units'], dashboard['price_cents'],
                                          dashboard['charge_cents']))
        other = self.root / 'other.sqlite'
        register_contract(other, FIXTURES / 'period_contract_train_first.json')
        choose_contract(other, FIXTURES / 'period_choice_train_first.json')
        import_file(other, FIXTURES / 'period_usage.json')
        sign_amendment(other, self._amendment('boundary-off.json', amendment_id='amend-boundary-off',
                                              effective_at='2026-09-15T10:00:01Z'))
        closed = close_period(other, 'tenant-a', '2026-09', '2026-10-02T00:00:00Z', 'ops-close')
        dashboard = self._lines(closed)['dashboard']
        self.assertEqual((4, 100, 400), (dashboard['billable_units'], dashboard['price_cents'],
                                         dashboard['charge_cents']))

    def test_amendment_before_close_enters_source_digest_and_initial_close(self):
        self._ready()
        before = period_report(self.db, 'tenant-a', '2026-09')['source_sha']
        sign_amendment(self.db, FIXTURES / 'period_amendment_retroactive.json')
        after = period_report(self.db, 'tenant-a', '2026-09')['source_sha']
        self.assertNotEqual(before, after)
        result = self._close()
        self.assertEqual(after, result['source_sha'])
        self.assertEqual(1200, result['total_cents'])
        report = period_report(self.db, 'tenant-a', '2026-09')
        self.assertEqual(['amend-dashboard-300-retroactive'], report['closes'][0]['amendments'])
        amendment = report['amendments'][0]
        self.assertEqual(('retroactive', {'dashboard': 300}, 'owner-a', CONTRACT_SHA),
                         (amendment['clause'], amendment['prices_cents'], amendment['signer'],
                          amendment['contract_sha']))

    def test_amendment_immutability_and_signing_validation(self):
        self._ready()
        path = FIXTURES / 'period_amendment_prospective.json'
        first = sign_amendment(self.db, path)
        self.assertEqual('amend-dashboard-300-prospective', first['amendment_id'])
        self.assertTrue(sign_amendment(self.db, path)['idempotent'])
        changed = json.loads(path.read_text(encoding='utf-8'))
        changed['proof_sha'] = 'tampered-proof'
        with self.assertRaisesRegex(ValueError, 'immutable'):
            sign_amendment(self.db, self._write('tampered.json', changed))
        cases = [
            ('contract_sha', '0' * 64, 'does not bind'),
            ('signer', 'intruder', 'authorized contract signer'),
            ('clause', 'whenever', 'prospective or retroactive'),
            ('prices_cents', {'unknown-product': 10}, 'only reprice contracted'),
            ('prices_cents', {'dashboard': -1}, 'nonnegative integer'),
            ('prices_cents', {}, 'at least one product'),
            ('effective_at', '2026-08-31T23:59:59Z', 'outside the contract period'),
            ('effective_at', '2026-10-01T00:00:00Z', 'outside the contract period'),
            ('contract_id', 'warehouse-dashboard-first', 'no chosen period contract'),
        ]
        for key, value, pattern in cases:
            with self.assertRaisesRegex(ValueError, pattern, msg=f'{key}={value}'):
                sign_amendment(self.db, self._amendment(f'bad-{pattern[:6]}.json',
                                                        amendment_id=f'bad-{pattern[:6]}',
                                                        **{key: value}))
        incomplete = {'amendment_id': 'amend-incomplete', 'clause': 'prospective'}
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            sign_amendment(self.db, self._write('incomplete.json', incomplete))

    def test_amendment_after_request_rejects_stale_approval_until_fresh_request(self):
        self._ready()
        self._close()
        import_file(self.db, FIXTURES / 'period_correction_late.json')
        sign_amendment(self.db, FIXTURES / 'period_amendment_prospective.json')
        request_restatement(self.db, self._request())
        sign_amendment(self.db, self._amendment('second.json', amendment_id='amend-train-500',
                                                prices_cents={'train': 500}))
        with self.assertRaisesRegex(ValueError, 'source changed'):
            approve_restatement(self.db, 'restate-2026-09-1', 'finance-a', 'proof')
        self.assertEqual(1, len(period_report(self.db, 'tenant-a', '2026-09')['closes']))
        request_restatement(self.db, self._request('request2.json', 'restate-2026-09-2'))
        restated = approve_restatement(self.db, 'restate-2026-09-2', 'finance-a', 'proof-2')
        # 训练计费单位为 0，第二份 amendment 不改应收；看板仍按旧价。
        self.assertEqual(200, restated['new_total_cents'])
        history = period_report(self.db, 'tenant-a', '2026-09')
        self.assertEqual(['amend-dashboard-300-prospective', 'amend-train-500'],
                         sorted(history['closes'][1]['amendments']))

    def test_restatement_roles_still_bound_in_amendment_flow(self):
        self._ready()
        self._close()
        sign_amendment(self.db, FIXTURES / 'period_amendment_retroactive.json')
        with self.assertRaisesRegex(ValueError, 'missing or no longer pending'):
            approve_restatement(self.db, 'restate-missing', 'finance-a', 'proof')
        request_restatement(self.db, self._request())
        with self.assertRaisesRegex(ValueError, 'independent authorized'):
            approve_restatement(self.db, 'restate-2026-09-1', 'data-steward', 'self-proof')
        with self.assertRaisesRegex(ValueError, 'independent authorized'):
            approve_restatement(self.db, 'restate-2026-09-1', 'stranger', 'bad-proof')
        restated = approve_restatement(self.db, 'restate-2026-09-1', 'finance-a', 'finance-proof-1')
        self.assertEqual(1200, restated['new_total_cents'])

    def test_layered_prospective_amendments_price_each_event_by_occurred_at(self):
        register_contract(self.db, FIXTURES / 'period_contract_train_first.json')
        choose_contract(self.db, FIXTURES / 'period_choice_train_first.json')
        usage = {'usage': [
            {'event_id': 'meter-dash-1', 'tenant': 'tenant-a', 'period': '2026-09',
             'product': 'dashboard', 'occurred_at': '2026-09-05T10:00:00Z', 'units': 4},
            {'event_id': 'meter-dash-2', 'tenant': 'tenant-a', 'period': '2026-09',
             'product': 'dashboard', 'occurred_at': '2026-09-15T10:00:00Z', 'units': 4},
            {'event_id': 'meter-dash-3', 'tenant': 'tenant-a', 'period': '2026-09',
             'product': 'dashboard', 'occurred_at': '2026-09-25T10:00:00Z', 'units': 4}]}
        import_file(self.db, self._write('usage.json', usage))
        sign_amendment(self.db, self._amendment('am1.json', amendment_id='amend-dash-150',
                                                effective_at='2026-09-10T00:00:00Z',
                                                prices_cents={'dashboard': 150}))
        sign_amendment(self.db, self._amendment('am2.json', amendment_id='amend-dash-300',
                                                effective_at='2026-09-20T00:00:00Z',
                                                prices_cents={'dashboard': 300}))
        result = self._close()
        # 12 单位：专属 2 + 共享 5 覆盖最早的 7 单位；计费的是 09-15 的 1 单位(150)
        # 与 09-25 的 4 单位(300)。
        dashboard = self._lines(result)['dashboard']
        self.assertEqual((12, 2, 5, 5, 1350), tuple(dashboard[key] for key in
                         ('actual_units', 'reserved_used', 'shared_used', 'billable_units',
                          'charge_cents')))
        snapshot = {row['usage_id']: row for row in
                    period_report(self.db, 'tenant-a', '2026-09')['closes'][0]['usage']}
        self.assertEqual((0, 100, 0), (snapshot['meter-dash-1']['billable_units'],
                                       snapshot['meter-dash-1']['price_cents'],
                                       snapshot['meter-dash-1']['charge_cents']))
        self.assertEqual((1, 150, 150), (snapshot['meter-dash-2']['billable_units'],
                                         snapshot['meter-dash-2']['price_cents'],
                                         snapshot['meter-dash-2']['charge_cents']))
        self.assertEqual((4, 300, 1200), (snapshot['meter-dash-3']['billable_units'],
                                          snapshot['meter-dash-3']['price_cents'],
                                          snapshot['meter-dash-3']['charge_cents']))

    def test_retroactive_amendment_overrides_earlier_prospective_for_whole_period(self):
        register_contract(self.db, FIXTURES / 'period_contract_train_first.json')
        choose_contract(self.db, FIXTURES / 'period_choice_train_first.json')
        usage = {'usage': [
            {'event_id': 'meter-dash-1', 'tenant': 'tenant-a', 'period': '2026-09',
             'product': 'dashboard', 'occurred_at': '2026-09-05T10:00:00Z', 'units': 4},
            {'event_id': 'meter-dash-2', 'tenant': 'tenant-a', 'period': '2026-09',
             'product': 'dashboard', 'occurred_at': '2026-09-25T10:00:00Z', 'units': 4}]}
        import_file(self.db, self._write('usage.json', usage))
        sign_amendment(self.db, self._amendment('am1.json', amendment_id='amend-dash-150',
                                                effective_at='2026-09-10T00:00:00Z',
                                                prices_cents={'dashboard': 150}))
        sign_amendment(self.db, self._amendment('am2.json', amendment_id='amend-dash-250',
                                                effective_at='2026-09-20T00:00:00Z',
                                                clause='retroactive', prices_cents={'dashboard': 250}))
        result = self._close()
        # retroactive 把全期改为 250 分；8 单位中专属 2 + 共享 5 覆盖 7，计费 1 单位。
        dashboard = self._lines(result)['dashboard']
        self.assertEqual((1, 250, 250), (dashboard['billable_units'], dashboard['price_cents'],
                                         dashboard['charge_cents']))

    def test_cli_replays_amendment_restatement_from_fixture_files(self):
        project = Path(__file__).resolve().parents[1]
        env = dict(os.environ, PYTHONPATH=str(project / 'src'))

        def run(*args):
            process = subprocess.run([sys.executable, '-m', 'cdcverdict', *map(str, args)],
                                     cwd=project, env=env, capture_output=True, text=True, check=True)
            return json.loads(process.stdout)

        run('period-contract', FIXTURES / 'period_contract_train_first.json', '--db', self.db)
        run('period-choose', FIXTURES / 'period_choice_train_first.json', '--db', self.db)
        run('period-import', FIXTURES / 'period_usage.json', '--db', self.db)
        self.assertEqual(400, run('period-close', '--db', self.db, '--tenant', 'tenant-a',
                                  '--period', '2026-09', '--as-of', '2026-10-02T00:00:00Z',
                                  '--actor', 'ops-close')['total_cents'])
        run('period-import', FIXTURES / 'period_correction_late.json', '--db', self.db)
        run('period-amend', FIXTURES / 'period_amendment_retroactive.json', '--db', self.db)
        run('period-restate-request', FIXTURES / 'period_restatement_request.json', '--db', self.db)
        approved = run('period-restate-approve', '--db', self.db, '--request', 'restate-2026-09-1',
                       '--reviewer', 'finance-a', '--proof-sha', 'finance-proof-1')
        self.assertEqual((600, 200), (approved['new_total_cents'], approved['delta_cents']))
        report = run('period-report', '--db', self.db, '--tenant', 'tenant-a', '--period', '2026-09')
        self.assertEqual([400, 600], [close['total_cents'] for close in report['closes']])
        self.assertEqual(['amend-dashboard-300-retroactive'], report['closes'][1]['amendments'])


if __name__ == '__main__':
    unittest.main()
