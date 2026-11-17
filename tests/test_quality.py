import json
import hashlib
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from cdcverdict.quality import (apply_quality_repairs, import_quality_batch,
                                propose_quality_repair, quality_report,publish_quality_product,
                                register_quality_contract, review_quality_repair,
                                withdraw_quality_repair)


class QualityProductTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / 'quality.sqlite'
        self.fixture = Path(__file__).parent / 'fixtures'
        register_quality_contract(self.db, self.fixture / 'quality_contract.json')

    def _write(self, name, value):
        path = self.root / name
        path.write_text(json.dumps(value, sort_keys=True), encoding='utf-8')
        return path

    def test_dependency_impact_and_competing_reviewed_repairs(self):
        print(__import__('cdcverdict').__file__)
        imported = import_quality_batch(self.db, self.fixture / 'quality_batch_bad_net.json')
        self.assertEqual('blocked', imported['products']['accounting']['status'])
        report = quality_report(self.db, 'orders-bad-net')
        initial = report['assessments'][0]
        self.assertEqual('failed', initial['rules']['net-nonnegative']['status'])
        self.assertEqual('blocked', initial['rules']['gross-balance']['status'])
        self.assertEqual('blocked', initial['rules']['paid-match']['status'])
        self.assertEqual('ready', initial['products']['fulfillment']['status'])
        blocked_path = self.root / 'accounting-blocked.json'
        with self.assertRaisesRegex(ValueError, 'blocked'):
            publish_quality_product(self.db, 'orders-bad-net', 'accounting', blocked_path)
        self.assertFalse(blocked_path.exists())
        publish_quality_product(self.db, 'orders-bad-net', 'fulfillment', self.root / 'fulfillment.json')
        fulfillment = json.loads((self.root / 'fulfillment.json').read_text(encoding='utf-8'))
        self.assertEqual({'delivery_code': 'DEL-17'}, fulfillment['facts'])
        self.assertEqual(['delivery-present'], list(fulfillment['rules']))
        propose_quality_repair(self.db, self.fixture / 'quality_proposal_net_5.json')
        propose_quality_repair(self.db, self.fixture / 'quality_proposal_net_7.json')
        disputed = apply_quality_repairs(self.db, 'orders-bad-net')
        self.assertEqual('disputed', disputed['status'])
        self.assertEqual(0, disputed['assessment']['revision'])
        withdraw_quality_repair(self.db, 'net-fix-7', 'ops-b', 'invoice 17 shows net 5')
        self.assertEqual('pending_review', apply_quality_repairs(self.db, 'orders-bad-net')['status'])
        self_review = self._write('self-review.json', {'proposal_id': 'net-fix-5',
            'reviewer': 'ops-a', 'verdict': 'approve', 'proof_sha': 'self'})
        with self.assertRaisesRegex(ValueError, 'independent reviewer'):
            review_quality_repair(self.db, self_review)
        review_quality_repair(self.db, self.fixture / 'quality_review_net_5.json')
        applied = apply_quality_repairs(self.db, 'orders-bad-net')
        self.assertEqual('applied', applied['status'])
        self.assertEqual(1, applied['assessment']['revision'])
        self.assertEqual('ready', applied['assessment']['products']['accounting']['status'])
        self.assertEqual('ready', applied['assessment']['products']['customer-receipt']['status'])
        report = quality_report(self.db, 'orders-bad-net')
        self.assertEqual(-5, report['original_facts']['net'])
        self.assertEqual(5, report['effective_facts']['net'])
        self.assertEqual(2, len(report['assessments']))
        self.assertEqual(-5, report['assessments'][0]['facts']['net'])
        self.assertEqual(5, report['assessments'][1]['facts']['net'])
        self.assertEqual(['disputed', 'pending_review', 'applied'],
                         [item['status'] for item in report['arbitrations']])
        self.assertEqual(1, len(report['reviews']))
        published = publish_quality_product(self.db, 'orders-bad-net', 'accounting', self.root / 'accounting.json')
        self.assertEqual(hashlib.sha256((self.root / 'accounting.json').read_bytes()).hexdigest(), published['manifest_sha'])
        self.assertEqual(5, json.loads((self.root / 'accounting.json').read_text(encoding='utf-8'))['facts']['net'])
        publications = quality_report(self.db, 'orders-bad-net')['publications']
        self.assertFalse(next(item for item in publications if item['product_id'] == 'fulfillment')['current'])
        self.assertTrue(next(item for item in publications if item['product_id'] == 'accounting')['current'])
        self.assertEqual('no_candidates', apply_quality_repairs(self.db, 'orders-bad-net')['status'])

    def test_upstream_passes_but_downstream_blocks_until_second_repair(self):
        import_quality_batch(self.db, self.fixture / 'quality_batch_downstream.json')
        first = quality_report(self.db, 'orders-downstream')['assessments'][0]
        self.assertEqual('passed', first['rules']['net-nonnegative']['status'])
        self.assertEqual('failed', first['rules']['gross-balance']['status'])
        self.assertEqual('blocked', first['rules']['paid-match']['status'])
        self.assertEqual('blocked', first['products']['accounting']['status'])
        self.assertEqual('ready', first['products']['fulfillment']['status'])
        propose_quality_repair(self.db, self.fixture / 'quality_proposal_gross_10.json')
        review_quality_repair(self.db, self.fixture / 'quality_review_gross_10.json')
        middle = apply_quality_repairs(self.db, 'orders-downstream')['assessment']
        self.assertEqual('passed', middle['rules']['gross-balance']['status'])
        self.assertEqual('failed', middle['rules']['paid-match']['status'])
        self.assertEqual('ready', middle['products']['customer-receipt']['status'])
        self.assertEqual('blocked', middle['products']['accounting']['status'])
        receipt = publish_quality_product(self.db, 'orders-downstream', 'customer-receipt', self.root / 'receipt.json')
        self.assertEqual(middle['assessment_id'], receipt['assessment_id'])
        accounting_path = self.root / 'accounting-premature.json'
        with self.assertRaisesRegex(ValueError, 'blocked'):
            publish_quality_product(self.db, 'orders-downstream', 'accounting', accounting_path)
        self.assertFalse(accounting_path.exists())
        stale = json.loads((self.fixture / 'quality_proposal_paid_10.json').read_text(encoding='utf-8'))
        stale['proposal_id'] = 'paid-stale'
        stale['expected_revision'] = 0
        with self.assertRaisesRegex(ValueError, 'stale repair proposal'):
            propose_quality_repair(self.db, self._write('paid-stale.json', stale))
        propose_quality_repair(self.db, self.fixture / 'quality_proposal_paid_10.json')
        review_quality_repair(self.db, self.fixture / 'quality_review_paid_10.json')
        final = apply_quality_repairs(self.db, 'orders-downstream')['assessment']
        self.assertEqual('ready', final['products']['accounting']['status'])
        self.assertEqual([0, 1, 2], [x['revision'] for x in quality_report(self.db, 'orders-downstream')['assessments']])
        self.assertFalse(quality_report(self.db, 'orders-downstream')['publications'][0]['current'])
        released = publish_quality_product(self.db, 'orders-downstream', 'accounting', self.root / 'accounting-final.json')
        self.assertEqual(final['assessment_id'], released['assessment_id'])

    def test_contract_cycle_and_evidence_id_reuse_rejected(self):
        contract = json.loads((self.fixture / 'quality_contract.json').read_text(encoding='utf-8'))
        contract['contract_id'] = 'cyclic-contract'
        contract['rules'][0]['depends_on'] = ['paid-match']
        with self.assertRaisesRegex(ValueError, 'cyclic'):
            register_quality_contract(self.db, self._write('cycle.json', contract))
        import_quality_batch(self.db, self.fixture / 'quality_batch_bad_net.json')
        proposal_path = self.fixture / 'quality_proposal_net_5.json'
        propose_quality_repair(self.db, proposal_path)
        altered = json.loads(proposal_path.read_text(encoding='utf-8'))
        altered['value'] = 99
        with self.assertRaisesRegex(ValueError, 'reused'):
            propose_quality_repair(self.db, self._write('altered.json', altered))
        before = quality_report(self.db, 'orders-bad-net')
        self.assertEqual(-5, before['effective_facts']['net'])

    def test_conflicting_independently_approved_values_still_disputed(self):
        import_quality_batch(self.db, self.fixture / 'quality_batch_bad_net.json')
        propose_quality_repair(self.db, self.fixture / 'quality_proposal_net_5.json')
        propose_quality_repair(self.db, self.fixture / 'quality_proposal_net_7.json')
        review_quality_repair(self.db, self.fixture / 'quality_review_net_5.json')
        review_quality_repair(self.db, self._write('review-net-7.json', {
            'proposal_id': 'net-fix-7', 'reviewer': 'second-auditor',
            'verdict': 'approve', 'proof_sha': 'checked-alternative-invoice'}))
        conflict = apply_quality_repairs(self.db, 'orders-bad-net')
        self.assertEqual('disputed', conflict['status'])
        self.assertEqual(0, conflict['assessment']['revision'])
        self.assertEqual(-5, quality_report(self.db, 'orders-bad-net')['effective_facts']['net'])

    def test_cli_competition_withdrawal_and_product_release(self):
        project = Path(__file__).parents[1]
        env = dict(os.environ, PYTHONPATH=str(project / 'src'))

        def run(*args):
            process = subprocess.run([sys.executable, '-m', 'cdcverdict', *map(str, args)],
                                     cwd=project, env=env, capture_output=True, text=True, check=True)
            return json.loads(process.stdout)

        # The contract registration in setUp used the same persistent DB.
        self.assertTrue(run('quality-contract', self.fixture / 'quality_contract.json',
                            '--db', self.db)['idempotent'])
        run('quality-batch', self.fixture / 'quality_batch_bad_net.json', '--db', self.db)
        run('quality-propose', self.fixture / 'quality_proposal_net_5.json', '--db', self.db)
        run('quality-propose', self.fixture / 'quality_proposal_net_7.json', '--db', self.db)
        self.assertEqual('disputed', run('quality-apply', '--db', self.db, '--batch', 'orders-bad-net')['status'])
        run('quality-withdraw', '--db', self.db, '--proposal', 'net-fix-7',
            '--proposer', 'ops-b', '--reason', 'superseded by invoice')
        run('quality-review', self.fixture / 'quality_review_net_5.json', '--db', self.db)
        self.assertEqual('ready', run('quality-apply', '--db', self.db,
                                      '--batch', 'orders-bad-net')['assessment']['products']['accounting']['status'])
        report = run('quality-report', '--db', self.db, '--batch', 'orders-bad-net')
        self.assertEqual(-5, report['original_facts']['net'])
        self.assertEqual(5, report['effective_facts']['net'])
        publication = run('quality-publish', '--db', self.db, '--batch', 'orders-bad-net',
                          '--product', 'accounting', '--out', self.root / 'cli-accounting.json')
        self.assertEqual(hashlib.sha256((self.root / 'cli-accounting.json').read_bytes()).hexdigest(),
                         publication['manifest_sha'])


if __name__ == '__main__':
    unittest.main()
