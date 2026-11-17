"""Purpose processing contracts and frozen recipient chains.

These tests exercise the difference between an owner-level grant and the
purpose-level decision of *which* processor may receive a value.  The replay
fixture is the one from ``demo``: key ``A`` / subject ``subject-A`` is visible at
watermark 3.
"""

import json

import pytest

from cdcverdict.__main__ import demo
from cdcverdict.consent import (acknowledge_recall, audit_withdrawals,
                                create_export, export_report, import_consent,
                                import_processing)
from cdcverdict.store import connect

DIRECT = 'pc-direct-research'
DELEGATED = 'pc-delegated-research'


def _write(tmp_path, name, data):
    path = tmp_path / name
    path.write_text(json.dumps(data, ensure_ascii=False), encoding='utf-8')
    return str(path)


def _grant(tmp_path, processors=('lab',), subjects=None, contracts=None):
    subjects = subjects or [{'id': 'subject-A', 'owner': 'owner-A', 'keys': ['A']}]
    if contracts is None:
        contracts = [
            {'id': DIRECT, 'purpose': 'research', 'mode': 'direct_only'},
            {'id': DELEGATED, 'purpose': 'research', 'mode': 'delegated_chain'},
        ]
    return _write(tmp_path, 'grant.json', {
        'subjects': subjects,
        'events': [{'id': 'g1', 'subject': 'subject-A', 'purpose': 'research',
                    'version': 1, 'action': 'grant', 'effective_watermark': 0,
                    'proof_sha': 'grant-proof-A'}],
        'processors': [{'subject': 'subject-A', 'purpose': 'research',
                        'processor': processor} for processor in processors],
        'contracts': contracts,
    })


def _delegation(delegation_id='d-lab-agency', delegator='lab', delegate='agency',
                revision=1, parent=None, effective=0, expires=None,
                subject='subject-A', purpose='research', contract=DELEGATED):
    item = {'id': delegation_id, 'revision': revision, 'contract': contract,
            'subject': subject, 'purpose': purpose, 'delegator': delegator,
            'delegate': delegate, 'action': 'delegate',
            'effective_watermark': effective,
            'proof_sha': f'proof-{delegation_id}-{revision}'}
    if expires is not None:
        item['expires_watermark'] = expires
    if parent is not None:
        item['parent_id'], item['parent_revision'] = parent
    return item


def _delegations(tmp_path, name, items):
    return _write(tmp_path, name, {'delegations': items})


def _withdraw(tmp_path, delegation_revoke=True):
    data = {'events': [{'id': 'r2', 'subject': 'subject-A', 'purpose': 'research',
                        'version': 2, 'action': 'revoke', 'effective_watermark': 3,
                        'proof_sha': 'withdraw-proof-A'}]}
    if delegation_revoke:
        data['delegations'] = [{'id': 'd-lab-agency', 'revision': 2,
                                'contract': DELEGATED, 'subject': 'subject-A',
                                'purpose': 'research', 'delegator': 'lab',
                                'delegate': 'agency', 'action': 'revoke',
                                'effective_watermark': 3,
                                'proof_sha': 'delegation-revoke-2'}]
    return _write(tmp_path, 'withdraw.json', data)


def _setup(tmp_path, processors=('lab',), subjects=None):
    root = tmp_path / 'work'
    demo(root)
    db = root / 'data.db'
    with connect(db) as conn:
        run = conn.execute('SELECT MAX(id) FROM replay_run').fetchone()[0]
    import_consent(db, _grant(tmp_path, processors, subjects))
    return db, run


def _delegated_export(db, run, contract=DELEGATED, recipient='agency'):
    return create_export(db, run, 'research', recipient, contract)


def _table_count(db, table):
    with connect(db) as conn:
        return conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]


def test_direct_only_contract_rejects_undeclared_recipient(tmp_path):
    db, run = _setup(tmp_path)
    eid, status = create_export(db, run, 'research', 'agency', DIRECT)
    report = export_report(db, eid)
    assert status == 'empty'
    assert report['rows'] == [] and report['chains'] == []
    assert report['excluded'][0]['reason'] == 'recipient_not_direct_processor'
    # The exclusion is atomic: no row or frozen chain was written for key A.
    assert _table_count(db, 'export_row') == 0
    assert _table_count(db, 'export_chain') == 0
    assert _table_count(db, 'export_chain_member') == 0

    eid2, status2 = create_export(db, run, 'research', 'lab', DIRECT)
    report2 = export_report(db, eid2)
    assert status2 == 'complete'
    assert report2['chains'][0]['mode'] == 'direct_only'
    assert [m['processor'] for m in report2['chains'][0]['members']] == ['lab']


def test_direct_only_does_not_implicitly_trust_owner(tmp_path):
    db, run = _setup(tmp_path, processors=('lab',))
    eid, status = create_export(db, run, 'research', 'owner-A', DIRECT)
    assert status == 'empty'
    assert export_report(db, eid)['excluded'][0]['reason'] == 'recipient_not_direct_processor'


def test_valid_delegation_allows_sub_processor_and_freezes_chain(tmp_path):
    db, run = _setup(tmp_path)
    import_processing(db, _delegations(
        tmp_path, 'delegations.json', [_delegation(expires=5)]))
    eid, status = _delegated_export(db, run)
    report = export_report(db, eid)
    assert status == 'complete'
    chain = report['chains'][0]
    assert chain['mode'] == 'delegated_chain'
    assert chain['contract_id'] == DELEGATED
    assert [member['processor'] for member in chain['members']] == ['lab', 'agency']
    assert [member['delegation_id'] for member in chain['members']] == [None, 'd-lab-agency']
    assert report['export']['chain_sha'] and chain['chain_sha']
    assert report['export']['contract_id'] == DELEGATED

    again, status_again = _delegated_export(db, run)
    assert status_again == 'complete'
    assert export_report(db, again)['chains'][0]['chain_sha'] == chain['chain_sha']


def test_delegation_root_must_be_declared_processor_or_owner(tmp_path):
    db, _ = _setup(tmp_path)
    path = _delegations(tmp_path, 'rogue.json', [
        _delegation(delegator='rogue-agency', delegate='agency')])
    with pytest.raises(ValueError, match='delegation_root_not_authorized'):
        import_processing(db, path)


def test_delegation_chain_is_rejected_after_it_expires(tmp_path):
    db, run = _setup(tmp_path)
    import_processing(db, _delegations(
        tmp_path, 'expired.json', [_delegation(expires=2)]))
    eid, status = _delegated_export(db, run)
    report = export_report(db, eid)
    assert status == 'empty'
    assert report['excluded'][0]['reason'] == 'delegation_chain_expired'
    assert report['chains'] == []
    assert _table_count(db, 'export_row') == 0
    assert _table_count(db, 'export_chain_member') == 0


def test_child_expiry_may_not_exceed_ancestor_expiry(tmp_path):
    db, _ = _setup(tmp_path)
    parent = _delegation('d-parent', expires=2)
    child = _delegation('d-child', delegator='agency', delegate='sub',
                        parent=('d-parent', 1), expires=5)
    path = _delegations(tmp_path, 'wider.json', [parent, child])
    with pytest.raises(ValueError, match='delegation_out_of_parent_range'):
        import_processing(db, path)
    assert _table_count(db, 'processing_delegation') == 0


def test_child_expiry_is_the_intersection_of_ancestor_windows(tmp_path):
    db, run = _setup(tmp_path)
    parent = _delegation('d-parent', effective=1, expires=5)
    child = _delegation('d-child', delegator='agency', delegate='sub',
                        parent=('d-parent', 1), effective=1, expires=3)
    import_processing(db, _delegations(tmp_path, 'narrow.json', [parent, child]))
    assert _delegated_export(db, run, recipient='sub')[1] == 'complete'

    # A grandchild cannot outlive the *intersection* of the child and parent
    # windows even though its own declared expiry is within the parent's.
    grandchild = _delegation('d-grandchild', delegator='sub', delegate='auditor',
                             parent=('d-child', 1), effective=1, expires=4)
    with pytest.raises(ValueError, match='delegation_out_of_parent_range'):
        import_processing(db, _delegations(tmp_path, 'wide.json', [parent, child, grandchild]))

    # Its effective start also cannot precede the inherited lower bound.
    early = _delegation('d-early', delegator='agency', delegate='other',
                        parent=('d-parent', 1), effective=0, expires=2)
    with pytest.raises(ValueError, match='delegation_effective_before_parent'):
        import_processing(db, _delegations(tmp_path, 'early.json', [early]))


def test_child_scope_cannot_widen_or_drift_from_parent(tmp_path):
    db, _ = _setup(tmp_path)
    parent = _delegation('d-parent', expires=5)
    wrong_subject = _delegation('d-child', delegator='agency', delegate='sub',
                                parent=('d-parent', 1), expires=4,
                                subject='subject-B')
    with pytest.raises(ValueError, match='unknown_subject'):
        import_processing(db, _delegations(tmp_path, 's.json', [parent, wrong_subject]))

    wrong_purpose = _delegation('d-child', delegator='agency', delegate='sub',
                                parent=('d-parent', 1), expires=4,
                                purpose='marketing')
    with pytest.raises(ValueError, match='processing_delegation_purpose_mismatch'):
        import_processing(db, _delegations(tmp_path, 'p.json', [parent, wrong_purpose]))

    wrong_contract = _delegation('d-child', delegator='agency', delegate='sub',
                                 parent=('d-parent', 1), expires=4,
                                 contract='pc-other-research')
    with pytest.raises(ValueError, match='unknown_processing_contract'):
        import_processing(db, _delegations(tmp_path, 'c.json', [parent, wrong_contract]))

    # A different contract with the same purpose must still match the parent.
    db2, _ = _setup(tmp_path / 'second')
    contract = {'contracts': [{'id': 'pc-other-research', 'purpose': 'research',
                               'mode': 'delegated_chain'}]}
    import_processing(db2, _write(tmp_path / 'second', 'contracts.json', contract))
    mismatch = _delegation('d-child', delegator='agency', delegate='sub',
                           parent=('d-parent', 1), expires=4,
                           contract='pc-other-research')
    with pytest.raises(ValueError, match='delegation_scope_mismatch'):
        import_processing(db2, _delegations(tmp_path / 'second', 'm.json', [parent, mismatch]))


def test_delegation_cycle_is_rejected(tmp_path):
    db, _ = _setup(tmp_path)
    parent = _delegation('d-parent', delegator='lab', delegate='agency', expires=5)
    reverse = _delegation('d-child', delegator='agency', delegate='lab',
                          parent=('d-parent', 1), expires=4)
    with pytest.raises(ValueError, match='delegation_cycle'):
        import_processing(db, _delegations(tmp_path, 'cycle.json', [parent, reverse]))
    with connect(db) as conn:
        assert conn.execute('SELECT COUNT(*) FROM processing_delegation').fetchone()[0] == 0


def test_ancestor_revocation_invalidates_child_chain(tmp_path):
    db, run = _setup(tmp_path)
    parent = _delegation('d-parent', expires=5)
    child = _delegation('d-child', delegator='agency', delegate='sub',
                        parent=('d-parent', 1), expires=4)
    import_processing(db, _delegations(tmp_path, 'chain.json', [parent, child]))
    assert _delegated_export(db, run, recipient='sub')[1] == 'complete'

    revoke = {'id': 'd-parent', 'revision': 2, 'contract': DELEGATED,
              'subject': 'subject-A', 'purpose': 'research', 'delegator': 'lab',
              'delegate': 'agency', 'action': 'revoke', 'effective_watermark': 3,
              'proof_sha': 'parent-revoke-2'}
    import_processing(db, _delegations(tmp_path, 'revoke.json', [revoke]))
    eid, status = _delegated_export(db, run, recipient='sub')
    assert status == 'empty'
    assert export_report(db, eid)['excluded'][0]['reason'] == 'delegation_chain_invalid'


def test_three_level_chain_recalls_every_processor(tmp_path):
    db, run = _setup(tmp_path)
    first = _delegation('d-1', delegator='lab', delegate='agency', expires=8)
    second = _delegation('d-2', delegator='agency', delegate='sub',
                         parent=('d-1', 1), effective=1, expires=6)
    third = _delegation('d-3', delegator='sub', delegate='auditor',
                        parent=('d-2', 1), effective=2, expires=4)
    import_processing(db, _delegations(tmp_path, 'deep.json', [first, second, third]))
    eid, status = _delegated_export(db, run, recipient='auditor')
    assert status == 'complete'
    chain = export_report(db, eid)['chains'][0]
    assert [member['processor'] for member in chain['members']] == [
        'lab', 'agency', 'sub', 'auditor']

    import_consent(db, _withdraw(tmp_path, delegation_revoke=False))
    report = export_report(db, eid)
    assert {recall['recipient'] for recall in report['recalls']} == {
        'lab', 'agency', 'sub', 'auditor'}
    assert len(report['recalls']) == 4


def test_delegation_revoke_requires_a_prior_delegation(tmp_path):
    db, _ = _setup(tmp_path)
    orphan = {'id': 'd-missing', 'revision': 1, 'contract': DELEGATED,
              'subject': 'subject-A', 'purpose': 'research', 'delegator': 'lab',
              'delegate': 'agency', 'action': 'revoke', 'effective_watermark': 3,
              'proof_sha': 'revoke-proof-1'}
    with pytest.raises(ValueError, match='unknown_delegation'):
        import_processing(db, _delegations(tmp_path, 'orphan.json', [orphan]))


def test_direct_contract_never_accepts_delegations(tmp_path):
    db, _ = _setup(tmp_path)
    delegation = _delegation(contract=DIRECT)
    with pytest.raises(ValueError, match='delegation_not_allowed_for_direct_contract'):
        import_processing(db, _delegations(tmp_path, 'direct.json', [delegation]))


def test_delegation_revision_must_be_monotonic(tmp_path):
    db, _ = _setup(tmp_path)
    first = _delegation(revision=1, effective=1, expires=8)
    import_processing(db, _delegations(tmp_path, 'rev1.json', [first]))
    gap = _delegation(revision=3, effective=2, expires=8)
    with pytest.raises(ValueError, match='delegation_revision_gap'):
        import_processing(db, _delegations(tmp_path, 'rev3.json', [gap]))
    regress = {'id': 'd-lab-agency', 'revision': 2, 'contract': DELEGATED,
               'subject': 'subject-A', 'purpose': 'research', 'delegator': 'lab',
               'delegate': 'agency', 'action': 'revoke',
               'effective_watermark': 0, 'proof_sha': 'revoke-proof-2'}
    with pytest.raises(ValueError, match='delegation_effective_regression'):
        import_processing(db, _delegations(tmp_path, 'regress.json', [regress]))



def test_export_before_child_validation_is_all_or_nothing(tmp_path):
    # The parent is valid and the child is the recipient, but the child expired.
    # Nothing from the parent may be persisted as a successful export.
    db, run = _setup(tmp_path)
    parent = _delegation('d-parent', expires=5)
    child = _delegation('d-child', delegator='agency', delegate='sub',
                        parent=('d-parent', 1), expires=2)
    import_processing(db, _delegations(tmp_path, 'short.json', [parent, child]))
    eid, status = _delegated_export(db, run, recipient='sub')
    assert status == 'empty'
    report = export_report(db, eid)
    assert report['excluded'] == [{'export_id': eid, 'key': 'A',
                                   'reason': 'delegation_chain_expired'}]
    assert report['rows'] == [] and report['chains'] == []
    assert _table_count(db, 'export_row') == 0
    assert _table_count(db, 'export_chain') == 0
    assert _table_count(db, 'export_chain_member') == 0


def test_late_revocation_recalls_every_frozen_chain_member(tmp_path):
    db, run = _setup(tmp_path)
    import_processing(db, _delegations(
        tmp_path, 'delegations.json', [_delegation(expires=5)]))
    eid, status = _delegated_export(db, run)
    assert status == 'complete'

    import_consent(db, _withdraw(tmp_path))
    report = export_report(db, eid)
    assert report['current_permission'] == 'recall_pending'
    assert {recall['recipient'] for recall in report['recalls']} == {'lab', 'agency'}
    assert len(report['recalls']) == 2
    recall_ids = [recall['id'] for recall in report['recalls']]
    assert audit_withdrawals(db, eid) == recall_ids
    assert audit_withdrawals(db, eid) == recall_ids

    agency_recall = next(r['id'] for r in report['recalls'] if r['recipient'] == 'agency')
    lab_recall = next(r['id'] for r in report['recalls'] if r['recipient'] == 'lab')
    with pytest.raises(ValueError, match='wrong_export_recipient'):
        acknowledge_recall(db, agency_recall, 'lab', 'wrong-recipient-proof')
    acknowledge_recall(db, agency_recall, 'agency', 'agency-receipt-1')
    after = export_report(db, eid)
    assert after['current_permission'] == 'recall_pending'
    assert {r['recipient']: r['status'] for r in after['recalls']} == {
        'lab': 'pending', 'agency': 'acknowledged'}
    # A child receipt does not close the parent's separate copy.
    assert acknowledge_recall(db, lab_recall, 'lab', 'lab-receipt-1')
    assert export_report(db, eid)['current_permission'] == 'recalled'
    assert acknowledge_recall(db, lab_recall, 'lab', 'lab-receipt-1')
    with pytest.raises(ValueError, match='recall_receipt_conflict'):
        acknowledge_recall(db, lab_recall, 'lab', 'other-proof')


def test_new_export_after_withdrawal_excludes_all_rows(tmp_path):
    db, run = _setup(tmp_path)
    import_processing(db, _delegations(
        tmp_path, 'delegations.json', [_delegation(expires=5)]))
    _delegated_export(db, run)
    import_consent(db, _withdraw(tmp_path))
    eid, status = _delegated_export(db, run)
    assert status == 'empty'
    assert export_report(db, eid)['excluded'][0]['reason'] == 'withdrawn'


def test_regrant_needs_a_new_authorization_chain_and_never_revives_old_copies(tmp_path):
    db, run = _setup(tmp_path)
    import_processing(db, _delegations(
        tmp_path, 'delegations.json', [_delegation(expires=5)]))
    first, _ = _delegated_export(db, run)
    import_consent(db, _withdraw(tmp_path))
    audit_withdrawals(db, first)

    import_consent(db, _write(tmp_path, 'regrant.json', {'events': [
        {'id': 'g3', 'subject': 'subject-A', 'purpose': 'research', 'version': 3,
         'action': 'grant', 'effective_watermark': 3, 'proof_sha': 'regrant-proof-A'}]}))
    # The old delegation was revoked; regranting consent alone does not restore it.
    blocked, blocked_status = _delegated_export(db, run)
    assert blocked_status == 'empty'
    assert export_report(db, blocked)['excluded'][0]['reason'] == 'delegation_chain_revoked'

    import_processing(db, _delegations(tmp_path, 'fresh.json', [
        _delegation('d-lab-agency-new', effective=3, expires=7)]))
    second, status = _delegated_export(db, run)
    assert status == 'complete'
    assert export_report(db, second)['rows'][0]['consent_event_id'] == 'g3'
    assert export_report(db, second)['chains'][0]['chain_sha'] != export_report(
        db, first)['chains'][0]['chain_sha']
    # The old export keeps its pending recall; the new export does not rewrite it.
    old = export_report(db, first)
    assert old['current_permission'] == 'recall_pending'
    assert {r['recipient'] for r in old['recalls']} == {'lab', 'agency'}


def test_ambiguous_purpose_requires_an_explicit_contract(tmp_path):
    db, run = _setup(tmp_path)
    import_processing(db, _delegations(
        tmp_path, 'delegations.json', [_delegation(expires=5)]))
    with pytest.raises(ValueError, match='processing_contract_required'):
        create_export(db, run, 'research', 'agency')
    assert create_export(db, run, 'research', 'agency', DELEGATED)[1] == 'complete'
    with pytest.raises(ValueError, match='unknown_processing_contract'):
        create_export(db, run, 'research', 'agency', 'missing-contract')
    with pytest.raises(ValueError, match='processing_contract_purpose_mismatch'):
        create_export(db, run, 'marketing', 'agency', DELEGATED)


def test_legacy_owner_grant_still_exports_without_a_contract(tmp_path):
    root = tmp_path / 'legacy'
    demo(root)
    db = root / 'data.db'
    with connect(db) as conn:
        run = conn.execute('SELECT MAX(id) FROM replay_run').fetchone()[0]
    import_consent(db, _grant(tmp_path, processors=(), contracts=[]))
    eid, status = create_export(db, run, 'research', 'lab')
    assert status == 'complete'
    report = export_report(db, eid)
    assert report['chains'][0]['mode'] == 'legacy'
    assert [m['processor'] for m in report['chains'][0]['members']] == ['lab']


def test_repeated_import_and_audit_are_idempotent(tmp_path):
    db, run = _setup(tmp_path)
    assert import_consent(db, _grant(tmp_path)) == 1
    assert import_consent(db, _grant(tmp_path)) == 1
    delegation_file = _delegations(
        tmp_path, 'delegations.json', [_delegation(expires=5)])
    assert import_processing(db, delegation_file)['idempotent'] is False
    assert import_processing(db, delegation_file)['idempotent'] is True

    first, status = _delegated_export(db, run)
    assert status == 'complete'
    second, status = _delegated_export(db, run)
    assert status == 'complete'
    first_report, second_report = export_report(db, first), export_report(db, second)
    assert first_report['export']['content_sha'] == second_report['export']['content_sha']
    assert first_report['chains'][0]['chain_sha'] == second_report['chains'][0]['chain_sha']

    import_consent(db, _withdraw(tmp_path))
    assert audit_withdrawals(db, first) == audit_withdrawals(db, first)
    assert len(export_report(db, first)['recalls']) == 2


def test_cli_processing_import_and_contract_selection(tmp_path):
    from cdcverdict.__main__ import main
    import sys
    db, run = _setup(tmp_path)
    path = _delegations(tmp_path, 'delegations.json', [_delegation(expires=5)])
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(sys, 'argv', ['cdcverdict', 'processing-import', path,
                                      '--db', str(db)])
    main()
    monkeypatch.setattr(sys, 'argv', ['cdcverdict', 'export-create', '--db', str(db),
                                      '--run', str(run), '--purpose', 'research',
                                      '--recipient', 'agency', '--contract', DELEGATED])
    main()
    monkeypatch.setattr(sys, 'argv', ['cdcverdict', 'export-create', '--db', str(db),
                                      '--run', str(run), '--purpose', 'research',
                                      '--recipient', 'agency', '--contract', DIRECT])
    main()
    monkeypatch.undo()
    with connect(db) as conn:
        statuses = [row[0] for row in conn.execute(
            'SELECT status FROM controlled_export ORDER BY id')]
    assert statuses == ['complete', 'empty']
