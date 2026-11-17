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
from cdcverdict.store import connect


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


class QualityAuditContractTest(unittest.TestCase):
    AUDIT_BATCH = 'orders-audit'

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / 'quality-audit.sqlite'
        self.fixture = Path(__file__).parent / 'fixtures'
        register_quality_contract(self.db, self.fixture / 'quality_contract_audit.json')

    def _write(self, name, value):
        path = self.root / name
        path.write_text(json.dumps(value, sort_keys=True), encoding='utf-8')
        return path

    def _two_repairs(self):
        """Drive net through -5 -> 5 -> 6, both independently reviewed and applied."""
        import_quality_batch(self.db, self.fixture / 'quality_batch_audit.json')
        propose_quality_repair(self.db, self.fixture / 'quality_proposal_audit_net_5.json')
        review_quality_repair(self.db, self.fixture / 'quality_review_audit_net_5.json')
        first = apply_quality_repairs(self.db, self.AUDIT_BATCH)
        self.assertEqual('applied', first['status'])
        self.assertEqual(1, first['assessment']['revision'])
        # gross 11 != 5 + 5 keeps the accounting products blocked after the first repair
        self.assertEqual('blocked', first['assessment']['products']['accounting-audit']['status'])
        propose_quality_repair(self.db, self.fixture / 'quality_proposal_audit_net_6.json')
        review_quality_repair(self.db, self.fixture / 'quality_review_audit_net_6.json')
        second = apply_quality_repairs(self.db, self.AUDIT_BATCH)
        self.assertEqual('applied', second['status'])
        self.assertEqual(2, second['assessment']['revision'])
        self.assertEqual('ready', second['assessment']['products']['accounting-audit']['status'])

    def test_operational_contract_projects_only_declared_fields_without_credentials(self):
        self._two_repairs()
        out = self.root / 'fulfillment-ops.json'
        publish_quality_product(self.db, self.AUDIT_BATCH, 'fulfillment', out)
        manifest = json.loads(out.read_text(encoding='utf-8'))
        self.assertEqual('operational', manifest['delivery'])
        self.assertEqual({'delivery_code': 'DEL-21'}, manifest['facts'])
        self.assertNotIn('net', manifest['facts'])
        self.assertNotIn('correction_chains', manifest)
        self.assertEqual(['delivery-present'], list(manifest['rules']))
        accounting = self.root / 'accounting-ops.json'
        publish_quality_product(self.db, self.AUDIT_BATCH, 'accounting-ops', accounting)
        ops_manifest = json.loads(accounting.read_text(encoding='utf-8'))
        self.assertEqual('operational', ops_manifest['delivery'])
        self.assertNotIn('correction_chains', ops_manifest)
        self.assertEqual({'net', 'tax', 'gross', 'paid'}, set(ops_manifest['facts']))
        self.assertNotIn('delivery_code', ops_manifest['facts'])
        self.assertEqual(
            hashlib.sha256(json.dumps(ops_manifest['facts'], sort_keys=True,
                                      separators=(',', ':')).encode('utf-8')).hexdigest(),
            ops_manifest['product_fact_sha'])

    def test_audit_manifest_carries_full_chain_from_frozen_source_to_current_value(self):
        self._two_repairs()
        out = self.root / 'accounting-audit.json'
        published = publish_quality_product(self.db, self.AUDIT_BATCH, 'accounting-audit', out)
        raw = out.read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(), published['manifest_sha'])
        manifest = json.loads(raw)
        self.assertEqual('audit', manifest['delivery'])
        self.assertEqual({'net', 'tax', 'gross', 'paid'}, set(manifest['facts']))
        self.assertNotIn('delivery_code', manifest['facts'])
        self.assertEqual(6, manifest['facts']['net'])
        chain = manifest['correction_chains']['net']
        self.assertEqual([-5, 5], [link['old_value'] for link in chain])
        self.assertEqual([5, 6], [link['new_value'] for link in chain])
        self.assertEqual([1, 2], [link['revision'] for link in chain])
        self.assertEqual(['audit-net-5', 'audit-net-6'], [link['proposal_id'] for link in chain])
        self.assertEqual(['invoice-21-a', 'invoice-21-corrected'],
                         [link['evidence_sha'] for link in chain])
        self.assertEqual(['finance-auditor', 'finance-auditor'],
                         [link['reviewer'] for link in chain])
        self.assertEqual(['checked-invoice-21-a', 'checked-invoice-21-corrected'],
                         [link['review_proof_sha'] for link in chain])
        self.assertEqual([True, False], [link['superseded'] for link in chain])
        # facts that never needed repair still get an explicit empty chain entry
        self.assertEqual([], manifest['correction_chains']['gross'])
        self.assertEqual([], manifest['correction_chains']['paid'])
        self.assertEqual([], manifest['correction_chains']['tax'])
        self.assertEqual(
            hashlib.sha256(json.dumps(manifest['facts'], sort_keys=True,
                                      separators=(',', ':')).encode('utf-8')).hexdigest(),
            manifest['product_fact_sha'])
        with connect(self.db) as db:
            stored = db.execute('SELECT raw,manifest_sha FROM quality_publication WHERE id=?',
                                (published['publication_id'],)).fetchone()
            self.assertEqual(raw, stored['raw'])
            self.assertEqual(published['manifest_sha'], stored['manifest_sha'])

    def test_audit_publish_rejected_when_review_is_missing_or_rejecting(self):
        def state_without_review(proposal):
            self._two_repairs()
            with connect(self.db) as db:
                db.execute('DELETE FROM quality_review WHERE proposal_id=?', (proposal,))

        state_without_review('audit-net-6')
        out = self.root / 'missing-terminal.json'
        with self.assertRaisesRegex(ValueError, 'audit product rejected'):
            publish_quality_product(self.db, self.AUDIT_BATCH, 'accounting-audit', out)
        self.assertFalse(out.exists())

        self.setUp()
        state_without_review('audit-net-5')
        out = self.root / 'missing-superseded.json'
        with self.assertRaisesRegex(ValueError, 'audit product rejected'):
            publish_quality_product(self.db, self.AUDIT_BATCH, 'accounting-audit', out)
        self.assertFalse(out.exists())

        self.setUp()
        self._two_repairs()
        with connect(self.db) as db:
            db.execute("UPDATE quality_review SET verdict='reject' WHERE proposal_id='audit-net-6'")
        out = self.root / 'rejected-review.json'
        with self.assertRaisesRegex(ValueError, 'audit product rejected'):
            publish_quality_product(self.db, self.AUDIT_BATCH, 'accounting-audit', out)
        self.assertFalse(out.exists())

    def test_audit_publish_rejected_when_current_value_escapes_its_chain(self):
        self._two_repairs()
        with connect(self.db) as db:
            db.execute("UPDATE quality_correction SET proposal_id='audit-net-5' "
                       "WHERE batch_id=? AND fact_key='net'", (self.AUDIT_BATCH,))
        out = self.root / 'rebound-correction.json'
        with self.assertRaisesRegex(ValueError, 'audit product rejected'):
            publish_quality_product(self.db, self.AUDIT_BATCH, 'accounting-audit', out)
        self.assertFalse(out.exists())
        with connect(self.db) as db:
            db.execute("UPDATE quality_correction SET proposal_id='audit-net-6',value_json='99' "
                       "WHERE batch_id=? AND fact_key='net'", (self.AUDIT_BATCH,))
        with self.assertRaisesRegex(ValueError, 'audit product rejected'):
            publish_quality_product(self.db, self.AUDIT_BATCH, 'accounting-audit',
                                    self.root / 'divergent-value.json')
        self.assertFalse((self.root / 'divergent-value.json').exists())

    def test_operational_publish_survives_a_broken_audit_chain(self):
        self._two_repairs()
        with connect(self.db) as db:
            db.execute('DELETE FROM quality_review WHERE proposal_id=?', ('audit-net-5',))
        out = self.root / 'ops-anyway.json'
        publish_quality_product(self.db, self.AUDIT_BATCH, 'accounting-ops', out)
        manifest = json.loads(out.read_text(encoding='utf-8'))
        self.assertEqual('operational', manifest['delivery'])
        self.assertEqual(6, manifest['facts']['net'])
        self.assertNotIn('correction_chains', manifest)
        with self.assertRaisesRegex(ValueError, 'audit product rejected'):
            publish_quality_product(self.db, self.AUDIT_BATCH, 'accounting-audit',
                                    self.root / 'audit-blocked.json')

    def test_historical_publication_bytes_remain_immutable_after_later_revision(self):
        self._two_repairs()
        old_out = self.root / 'audit-rev2.json'
        old = publish_quality_product(self.db, self.AUDIT_BATCH, 'accounting-audit', old_out)
        old_bytes = old_out.read_bytes()
        proposals = [
            ('audit-net-7', 'net', 7, 'invoice-21-final', 'cashier-auditor', 'proof-final-net'),
            ('audit-gross-12', 'gross', 12, 'invoice-21-gross', 'cashier-auditor', 'proof-final-gross'),
            ('audit-paid-12', 'paid', 12, 'receipt-21', 'cashier-auditor', 'proof-final-paid'),
        ]
        for proposal_id, key, value, evidence, reviewer, proof in proposals:
            proposal = self._write(proposal_id + '.json', {
                'proposal_id': proposal_id, 'batch_id': self.AUDIT_BATCH,
                'expected_revision': 2, 'fact_key': key, 'value': value,
                'proposer': 'ops-b', 'evidence_sha': evidence})
            review = self._write(proposal_id + '-review.json', {
                'proposal_id': proposal_id, 'reviewer': reviewer,
                'verdict': 'approve', 'proof_sha': proof})
            propose_quality_repair(self.db, proposal)
            review_quality_repair(self.db, review)
        third = apply_quality_repairs(self.db, self.AUDIT_BATCH)
        self.assertEqual(3, third['assessment']['revision'])
        new_out = self.root / 'audit-rev3.json'
        new = publish_quality_product(self.db, self.AUDIT_BATCH, 'accounting-audit', new_out)
        self.assertEqual(old_bytes, old_out.read_bytes())
        new_manifest = json.loads(new_out.read_text(encoding='utf-8'))
        net_chain = new_manifest['correction_chains']['net']
        self.assertEqual([(1, -5, 5), (2, 5, 6), (3, 6, 7)],
                         [(link['revision'], link['old_value'], link['new_value']) for link in net_chain])
        self.assertEqual([True, True, False], [link['superseded'] for link in net_chain])
        gross_chain = new_manifest['correction_chains']['gross']
        self.assertEqual(1, len(gross_chain))
        self.assertEqual((11, 12), (gross_chain[0]['old_value'], gross_chain[0]['new_value']))
        with connect(self.db) as db:
            rows = db.execute('SELECT id,manifest_sha,raw,assessment_id FROM quality_publication ORDER BY id').fetchall()
            self.assertEqual(2, len(rows))
            self.assertEqual(old['manifest_sha'], rows[0]['manifest_sha'])
            self.assertEqual(old_bytes, rows[0]['raw'])
            self.assertEqual(old['assessment_id'], rows[0]['assessment_id'])
            self.assertEqual(new['manifest_sha'], rows[1]['manifest_sha'])
            self.assertNotEqual(rows[0]['assessment_id'], rows[1]['assessment_id'])
        publications = quality_report(self.db, self.AUDIT_BATCH)['publications']
        self.assertEqual([False, True], [item['current'] for item in publications])

    def test_audit_publish_rejected_when_current_correction_has_no_adopted_chain(self):
        self._two_repairs()
        # An orphan correction row that no applied arbitration ever adopted: history cannot
        # explain the current value, so the audit product must refuse publication.
        with connect(self.db) as db:
            db.execute("INSERT INTO quality_correction(batch_id,fact_key,value_json,proposal_id,revision) "
                       "VALUES(?,?,?,?,?)", (self.AUDIT_BATCH, 'tax', '42', 'ghost-proposal', 2))
        out = self.root / 'orphan-correction.json'
        with self.assertRaisesRegex(ValueError, 'audit product rejected'):
            publish_quality_product(self.db, self.AUDIT_BATCH, 'accounting-audit', out)
        self.assertFalse(out.exists())
        # the operational contract makes no chain promise and still publishes
        ops = self.root / 'orphan-ops.json'
        publish_quality_product(self.db, self.AUDIT_BATCH, 'accounting-ops', ops)
        self.assertNotIn('correction_chains', json.loads(ops.read_text(encoding='utf-8')))

    def test_contract_with_unknown_delivery_mode_rejected(self):
        contract = json.loads((self.fixture / 'quality_contract_audit.json').read_text(encoding='utf-8'))
        contract['contract_id'] = 'bad-delivery-contract'
        contract['products'][0]['delivery'] = 'narrative'
        with self.assertRaisesRegex(ValueError, 'invalid quality product'):
            register_quality_contract(self.db, self._write('bad-delivery.json', contract))


if __name__ == '__main__':
    unittest.main()
