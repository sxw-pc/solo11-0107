import json
import pytest
from cdcverdict.ingest import snapshot,ingest
from cdcverdict.rules import replay
from cdcverdict.report import write
from cdcverdict.store import connect
def file(tmp,name,rows):
    p=tmp/name;p.write_text('\n'.join(json.dumps(x,sort_keys=True) for x in rows),encoding='utf-8');return p
def base(tmp,rows=None):
    db=tmp/'data.db';snapshot(file(tmp,'base.jsonl',rows or [{'key':'A','value':1}]),db,0);return db
def events(tmp,db,rows):return ingest(file(tmp,'events.jsonl',rows),db)
def tx(action='update',before=1,after=2,key='A',seq=2,alias_to=None):
    return [{'kind':'begin','txn':'T1','seq':1},
      {'kind':'change','txn':'T1','seq':seq,'key':key,'action':action,'before':before,'after':after,'alias_to':alias_to},
      {'kind':'commit','txn':'T1','seq':3}]
def test_snapshot_idempotent_and_conflict(tmp_path):
    db=base(tmp_path);assert snapshot(tmp_path/'base.jsonl',db,0)==1
    with pytest.raises(ValueError,match='snapshot_contract_changed'):
        snapshot(tmp_path/'base.jsonl',db,1)
    with pytest.raises(ValueError,match='snapshot_contract_changed'):
        snapshot(tmp_path/'base.jsonl',db,0,'v2')
    with pytest.raises(ValueError,match='snapshot_contract_changed'):
        snapshot(file(tmp_path,'changed.jsonl',[{'key':'A','value':3}]),db,0)
    with pytest.raises(ValueError,match='snapshot_duplicate_key'):
        snapshot(file(tmp_path,'dup.jsonl',[{'key':'X','value':1},{'key':'X','value':2}]),db,0)
    with pytest.raises(ValueError,match='invalid_watermark'):
        snapshot(file(tmp_path,'later.jsonl',[{'key':'B','value':1}]),db,-1)
def test_complete_transaction_and_before(tmp_path):
    db=base(tmp_path);events(tmp_path,db,tx());run,out,_=replay(db,3)
    assert out['A']=='存在'
    assert json.loads(connect(db).execute('SELECT value FROM decision WHERE run_id=?',(run,)).fetchone()[0])==2
def test_incomplete_transaction_does_not_apply(tmp_path):
    db=base(tmp_path);events(tmp_path,db,tx()[:-1]);_,out,_=replay(db,3)
    assert out['A']=='无法判定'
def test_missing_begin(tmp_path):
    db=base(tmp_path);events(tmp_path,db,tx()[1:]);_,out,_=replay(db,3)
    assert out['A']=='无法判定'
def test_duplicate_file_and_identity_conflict(tmp_path):
    db=base(tmp_path);rows=tx();assert events(tmp_path,db,rows)==3
    assert ingest(tmp_path/'events.jsonl',db)==0
    bad=rows.copy();bad[1]={**bad[1],'after':99}
    ingest(file(tmp_path,'conflict.jsonl',bad),db)
    _,out,_=replay(db,3);assert out['A']=='无法判定'
def test_conflicting_commit_blocks_transaction_keys(tmp_path):
    db=base(tmp_path);rows=tx();events(tmp_path,db,rows)
    conflicting=[{'kind':'commit','txn':'T1','seq':3,'note':'different'}]
    ingest(file(tmp_path,'conflict_commit.jsonl',conflicting),db)
    _,out,_=replay(db,3)
    assert out['A']=='无法判定'
def test_before_mismatch_freezes_key(tmp_path):
    db=base(tmp_path);events(tmp_path,db,tx(before=7));_,out,_=replay(db,3)
    assert out['A']=='无法判定'
def test_delete_and_recreate(tmp_path):
    db=base(tmp_path);rows=tx('delete',1,None)
    rows += [{'kind':'begin','txn':'T2','seq':4},{'kind':'change','txn':'T2','seq':5,'key':'A','action':'insert','after':3},{'kind':'commit','txn':'T2','seq':6}]
    events(tmp_path,db,rows)
    assert replay(db,3)[1]['A']=='已删除'
    assert replay(db,6)[1]['A']=='存在'
def test_disallow_recreate(tmp_path):
    db=base(tmp_path);rows=tx('delete',1,None)
    rows += [{'kind':'begin','txn':'T2','seq':4},{'kind':'change','txn':'T2','seq':5,'key':'A','action':'insert','after':3},{'kind':'commit','txn':'T2','seq':6}]
    events(tmp_path,db,rows);assert replay(db,6,False)[1]['A']=='无法判定'
def test_alias_rename(tmp_path):
    db=base(tmp_path);events(tmp_path,db,tx('rename',alias_to='B'))
    _,out,_=replay(db,3);assert out=={'B':'存在'}
def test_alias_merge_blocked(tmp_path):
    db=base(tmp_path,[{'key':'A','value':1},{'key':'B','value':2}]);events(tmp_path,db,tx('rename',alias_to='B'))
    assert replay(db,3)[1]['A']=='无法判定'
def test_watermark_exclusion(tmp_path):
    db=base(tmp_path);events(tmp_path,db,tx());_,out,excluded=replay(db,2)
    assert out['A']=='存在' and excluded==['T1']
def test_report_evidence_and_archive(tmp_path):
    db=base(tmp_path);events(tmp_path,db,tx());run,_,_=replay(db,3)
    path=write(db,run,tmp_path/'report');data=json.loads(path.read_text(encoding='utf-8'))
    assert data['decisions'][0]['status']=='存在' and data['evidence'][0]['event_id']
    assert data['sources'][0]['sha'] and path.with_suffix('.md').exists()
    assert connect(db).execute('SELECT status FROM replay_run WHERE id=?',(run,)).fetchone()[0]=='已归档'
def test_bad_report_does_not_archive(tmp_path):
    db=base(tmp_path);run,_,_=replay(db,0)
    blocker=tmp_path/'block';blocker.write_text('x')
    with pytest.raises(OSError):write(db,run,blocker/'report')
    assert connect(db).execute('SELECT status FROM replay_run WHERE id=?',(run,)).fetchone()[0]!='已归档'
def test_expected_digest_diff(tmp_path):
    db=base(tmp_path);run,_,_=replay(db,0)
    with connect(db) as c:c.execute('INSERT INTO expected VALUES (?,?)',('A','999'))
    data=json.loads(write(db,run,tmp_path/'report').read_text(encoding='utf-8'))
    assert data['expected_differences']==['A']
def test_duplicate_commit_is_not_applied(tmp_path):
    db=base(tmp_path);rows=tx()+[{'kind':'commit','txn':'T1','seq':4}]
    events(tmp_path,db,rows)
    assert replay(db,4)[1]['A']=='无法判定'
def test_change_after_commit_invalidates_whole_transaction(tmp_path):
    db=base(tmp_path)
    rows=tx()+[{'kind':'change','txn':'T1','seq':4,'key':'A','action':'update','before':2,'after':3}]
    events(tmp_path,db,rows)
    run,out,_=replay(db,4)
    assert out['A']=='无法判定'
    conn=connect(db)
    assert conn.execute('SELECT valid FROM txn WHERE id=?',('T1',)).fetchone()[0]==0
    assert conn.execute('SELECT COUNT(*) FROM evidence WHERE run_id=?',(run,)).fetchone()[0]==0
def test_before_failure_rejects_other_changes_in_same_transaction(tmp_path):
    db=base(tmp_path,[{'key':'A','value':1},{'key':'B','value':1}])
    rows=[{'kind':'begin','txn':'T1','seq':1},
          {'kind':'change','txn':'T1','seq':2,'key':'A','action':'update','before':1,'after':2},
          {'kind':'change','txn':'T1','seq':3,'key':'B','action':'update','before':99,'after':2},
          {'kind':'commit','txn':'T1','seq':4}]
    events(tmp_path,db,rows)
    run,out,_=replay(db,4)
    assert out=={'A':'无法判定','B':'无法判定'}
    conn=connect(db)
    assert conn.execute('SELECT COUNT(*) FROM evidence WHERE run_id=?',(run,)).fetchone()[0]==0
    assert conn.execute('SELECT status FROM replay_run WHERE id=?',(run,)).fetchone()[0]=='待核'
def test_historical_run_keeps_its_own_issues_and_exclusions(tmp_path):
    db=base(tmp_path);events(tmp_path,db,tx())
    early,_,_=replay(db,2)
    later,_,_=replay(db,3)
    early_report=json.loads(write(db,early,tmp_path/'early').read_text(encoding='utf-8'))
    assert early_report['excluded_transactions']==['T1']
    assert not early_report['issues']
    assert later!=early
def test_sequence_gap_is_uncertain(tmp_path):
    db=base(tmp_path);rows=tx();rows[1]['seq']=8;rows[2]['seq']=9
    events(tmp_path,db,rows)
    assert replay(db,9)[1]['A']=='无法判定'


# --- scoped repair branches and explicit merging ---------------------------

from cdcverdict.cutover import import_target, audit_target, configure_consumers, acknowledge, release_decision
from cdcverdict.cutover import audit_repair_merge, release_repair_merge
from cdcverdict.repair import plan_repair, apply_repair, plan_repair_merge, repair_merge_report
from cdcverdict.report import write_repair_merge


def divergent_target(tmp, rows=None):
    """T0 disagrees with replay on K1 and K2; K3 is an untouched target row."""
    db = base(tmp, [{'key': 'K1', 'value': 1}, {'key': 'K2', 'value': 1}, {'key': 'K3', 'value': 3}])
    events(tmp, db, [{'kind': 'begin', 'txn': 'T1', 'seq': 1},
                     {'kind': 'change', 'txn': 'T1', 'seq': 2, 'key': 'K1', 'action': 'update', 'before': 1, 'after': 2},
                     {'kind': 'change', 'txn': 'T1', 'seq': 3, 'key': 'K2', 'action': 'update', 'before': 1, 'after': 2},
                     {'kind': 'commit', 'txn': 'T1', 'seq': 4}])
    run, _, _ = replay(db, 4)
    t0 = file(tmp, 't0.jsonl', rows or [{'key': 'K1', 'value': 10}, {'key': 'K2', 'value': 20}, {'key': 'K3', 'value': 3}])
    target = import_target(db, t0, 4)
    audit, status, differences = audit_target(db, run, target)
    assert status == 'divergent' and sorted(x[0] for x in differences) == ['K1', 'K2']
    return db, run, target, audit


def manifest(tmp, name, rows):
    path = tmp / name
    path.write_text(json.dumps(rows), encoding='utf-8')
    return path


def only_a(tmp, decision='restore_replay'):
    return manifest(tmp, 'only-a.json', [{'key': 'K1', 'decision': decision, 'reason': 'K1 owner'} ])


def only_b(tmp):
    return manifest(tmp, 'only-b.json', [{'key': 'K2', 'decision': 'restore_replay', 'reason': 'K2 owner'}])


def target_rows(db, target_id):
    return {r['key']: json.loads(r['value']) for r in connect(db).execute(
        'SELECT key,value FROM target_row WHERE snapshot_id=?', (target_id,))}


def test_scoped_plans_merge_both_restores_regardless_of_apply_order(tmp_path):
    for order in (('a', 'b'), ('b', 'a')):
        case = tmp_path / '-'.join(order)
        case.mkdir()
        db, run, t0, audit = divergent_target(case)
        before = target_rows(db, t0)
        plans = {'a': plan_repair(db, audit, only_a(case)), 'b': plan_repair(db, audit, only_b(case))}
        assert connect(db).execute('SELECT COUNT(DISTINCT scope_kind) FROM repair_plan').fetchone()[0] >= 1
        assert all(r['scope_kind'] == 'partial' for r in connect(db).execute('SELECT scope_kind FROM repair_plan'))
        applied = [apply_repair(db, plans[step])[0] for step in order]
        # Each branch only changes its own key in its own output.
        assert target_rows(db, applied[0]) != before
        merge, merged_target, status = plan_repair_merge(db, [plans['a'], plans['b']])
        assert status == 'merged' and merged_target not in applied
        rows = target_rows(db, merged_target)
        assert rows == {'K1': 2, 'K2': 2, 'K3': 3}
        assert audit_target(db, run, merged_target)[1] == 'ready'
        # Applying a merge in the other insertion order is the same plan set.
        assert plan_repair_merge(db, [plans['b'], plans['a']]) == (merge, merged_target, 'merged')


def test_partial_scope_leaves_undeclared_keys_unresolved(tmp_path):
    db, run, t0, audit = divergent_target(tmp_path)
    pa = plan_repair(db, audit, only_a(tmp_path))
    apply_repair(db, pa)
    merge, merged_target, status = plan_repair_merge(db, [pa])
    assert status == 'merged'
    # K2 was never declared: it must be neither accepted nor deferred.
    report = write_repair_merge(db, merge, tmp_path / 'merge-report')
    data = json.loads(report.read_text(encoding='utf-8'))
    keys = {row['key']: row for row in data['keys']}
    assert keys['K1']['outcome'] == 'decided' and keys['K1']['decision'] == 'restore_replay'
    assert keys['K2']['outcome'] == 'unresolved' and keys['K2']['decision'] is None
    assert keys['K2']['unresolved'] and not keys['K2']['conflict']
    assert data['unresolved'] == ['K2'] and not data['resolved']
    # The merged snapshot still carries the original target value for K2.
    assert target_rows(db, merged_target)['K2'] == 20
    assert audit_target(db, run, merged_target)[1] == 'divergent'
    with pytest.raises(ValueError, match='repair_merge_incomplete'):
        release_repair_merge(db, merge)


def test_overlapping_same_outcome_is_deduplicated_with_rule(tmp_path):
    db, run, t0, audit = divergent_target(tmp_path)
    a = plan_repair(db, audit, only_a(tmp_path))
    b = plan_repair(db, audit, manifest(tmp_path, 'b-k1.json', [
        {'key': 'K1', 'decision': 'restore_replay', 'reason': 'second reviewer agrees'}]))
    apply_repair(db, a)
    apply_repair(db, b)
    merge, merged_target, status = plan_repair_merge(db, [a, b])
    assert status == 'merged'
    report = repair_merge_report(db, merge)
    row = next(row for row in report['keys'] if row['key'] == 'K1')
    assert row['outcome'] == 'agreed' and row['decision'] == 'restore_replay'
    assert row['value'] == 2 and row['primary_plan_id'] == a
    assert [source['plan_id'] for source in row['sources']] == [a, b]
    assert target_rows(db, merged_target)['K1'] == 2
    # Only K1 is settled; K2 still resolves to unresolved in the merge report.
    assert plan_repair_merge(db, [a, b]) == (merge, merged_target, 'merged')


def test_overlapping_conflicting_outcomes_block_and_preserve_evidence(tmp_path):
    db, run, t0, audit = divergent_target(tmp_path)
    a = plan_repair(db, audit, only_a(tmp_path))
    # One reviewer restores the replay value, another keeps the original target
    # value for the same key: the merge must block and keep both as evidence.
    b = plan_repair(db, audit, manifest(tmp_path, 'b-accept.json', [
        {'key': 'K1', 'decision': 'accept_target', 'reason': 'K1 owner keeps 10'}]))
    apply_repair(db, a)
    apply_repair(db, b)
    merge, merged_target, status = plan_repair_merge(db, [a, b])
    assert status == 'conflict' and merged_target is None
    report = repair_merge_report(db, merge)
    assert [row['key'] for row in report['keys'] if row['outcome'] == 'conflict'] == ['K1']
    row = next(row for row in report['keys'] if row['key'] == 'K1')
    assert {source['decision'] for source in row['sources']} == {'restore_replay', 'accept_target'}
    assert {source['value'] for source in row['sources']} == {2, 10}
    with pytest.raises(ValueError, match='repair_merge_conflict'):
        release_repair_merge(db, merge)
    # The conflict produced no snapshot and did not disturb the branch outputs.
    assert merged_target is None
    assert connect(db).execute('SELECT COUNT(*) FROM target_snapshot').fetchone()[0] >= 1


def test_same_decision_but_different_values_conflicts(tmp_path):
    db, run, t0, audit = divergent_target(tmp_path)
    a = plan_repair(db, audit, only_a(tmp_path))
    b = plan_repair(db, audit, manifest(tmp_path, 'b-restore.json', [
        {'key': 'K1', 'decision': 'restore_replay', 'reason': 'other reviewer'}]))
    # Both plans agree on the decision, but a stale/tampered frozen value would
    # make them disagree: the merge compares the recorded values, not just the
    # decision names.
    con = connect(db)
    with con:
        con.execute('UPDATE repair_choice SET replay_value=? WHERE plan_id=? AND key=?',
                    (json.dumps(99), b, 'K1'))
    apply_repair(db, a)
    con = connect(db)
    with con:
        con.execute("UPDATE repair_plan SET status='applied' WHERE id=?", (b,))
        con.execute("UPDATE repair_plan SET output_target_id=? WHERE id=?",
                    (con.execute('SELECT output_target_id FROM repair_plan WHERE id=?', (a,)).fetchone()[0], b))
    # Merge compares recorded branch values; it surfaces both plans' evidence.
    merge, merged_target, status = plan_repair_merge(db, [a, b])
    assert status == 'conflict' and merged_target is None
    row = next(row for row in repair_merge_report(db, merge)['keys'] if row['key'] == 'K1')
    assert row['outcome'] == 'conflict' and len(row['sources']) == 2


def test_stale_target_or_audit_digest_blocks_merge(tmp_path):
    db, run, t0, audit = divergent_target(tmp_path)
    a = plan_repair(db, audit, only_a(tmp_path))
    apply_repair(db, a)
    con = connect(db)
    with con:
        con.execute("UPDATE target_row SET value=? WHERE snapshot_id=? AND key='K3'", (json.dumps(4), t0))
    with pytest.raises(ValueError, match='audited_target_changed'):
        plan_repair_merge(db, [a])
    assert con.execute('SELECT COUNT(*) FROM repair_merge').fetchone()[0] == 0

    second = tmp_path.parent / (tmp_path.name + '-audit')
    second.mkdir()
    db2, run2, t02, audit2 = divergent_target(second)
    b = plan_repair(db2, audit2, only_a(second))
    apply_repair(db2, b)
    con2 = connect(db2)
    with con2:
        con2.execute('UPDATE cutover_audit SET target_digest=? WHERE id=?', ('0' * 64, audit2))
    with pytest.raises(ValueError, match='audited_target_changed|audit_digest_changed'):
        plan_repair_merge(db2, [b])

    # A changed frozen audit reason (not the target) invalidates the old plan
    # instead of silently rebasing it; a fresh plan can then be made.
    third = tmp_path.parent / (tmp_path.name + '-stale-plan')
    third.mkdir()
    db3, run3, t03, audit3 = divergent_target(third)
    c = plan_repair(db3, audit3, only_a(third))
    con3 = connect(db3)
    with con3:
        con3.execute("UPDATE cutover_audit SET reason='manufactured' WHERE id=?", (audit3,))
    with pytest.raises(ValueError, match='audit_digest_changed'):
        apply_repair(db3, c)
    with pytest.raises(ValueError, match='audit_digest_changed'):
        plan_repair_merge(db3, [c])


def test_merge_is_retryable_and_never_rewrites_old_evidence(tmp_path):
    db, run, t0, audit = divergent_target(tmp_path)
    a = plan_repair(db, audit, only_a(tmp_path))
    b = plan_repair(db, audit, only_b(tmp_path))
    apply_repair(db, a)
    apply_repair(db, b)
    con = connect(db)
    t0_rows = target_rows(db, t0)
    t0_sha = con.execute('SELECT source_sha FROM target_snapshot WHERE id=?', (t0,)).fetchone()[0]
    audit_row = dict(con.execute('SELECT * FROM cutover_audit WHERE id=?', (audit,)).fetchone())
    gates_before = [dict(row) for row in con.execute('SELECT * FROM release_gate')]
    first = plan_repair_merge(db, [a, b])
    second = plan_repair_merge(db, [a, b])
    con = connect(db)
    assert first == second
    assert con.execute('SELECT COUNT(*) FROM repair_merge').fetchone()[0] == 1
    # A retry is not a licence to rebase onto changed input.
    with con:
        con.execute('UPDATE target_row SET value=? WHERE snapshot_id=? AND key=?',
                    (json.dumps(99), t0, 'K3'))
    with pytest.raises(ValueError, match='audited_target_changed'):
        plan_repair_merge(db, [a, b])
    with con:
        con.execute('UPDATE target_row SET value=? WHERE snapshot_id=? AND key=?',
                    (json.dumps(t0_rows['K3']), t0, 'K3'))
    assert con.execute('SELECT COUNT(*) FROM target_snapshot WHERE id=?', (t0,)).fetchone()[0] == 1
    assert target_rows(db, t0) == t0_rows
    assert con.execute('SELECT source_sha FROM target_snapshot WHERE id=?', (t0,)).fetchone()[0] == t0_sha
    assert dict(con.execute('SELECT * FROM cutover_audit WHERE id=?', (audit,)).fetchone()) == audit_row
    assert [dict(row) for row in con.execute('SELECT * FROM release_gate')] == gates_before


def test_merge_snapshot_requires_new_audit_and_release_gate(tmp_path):
    db, run, t0, audit = divergent_target(tmp_path)
    contract = tmp_path / 'consumers.json'
    contract.write_text(json.dumps([{'id': 'ledger', 'critical': True, 'minimum_watermark': 4}]), encoding='utf-8')
    configure_consumers(db, contract)
    old_sha = connect(db).execute('SELECT source_sha FROM target_snapshot WHERE id=?', (t0,)).fetchone()[0]
    acknowledge(db, 'ledger', run, 1, 'accepted', old_sha)
    a = plan_repair(db, audit, only_a(tmp_path))
    b = plan_repair(db, audit, only_b(tmp_path))
    apply_repair(db, a)
    apply_repair(db, b)
    merge, merged_target, status = plan_repair_merge(db, [a, b])
    new_audit, audit_status, _ = audit_repair_merge(db, merge)
    assert audit_status == 'ready' and new_audit != audit
    # The old audit and its earlier gate remain intact.
    assert connect(db).execute('SELECT status FROM cutover_audit WHERE id=?', (audit,)).fetchone()[0] == 'divergent'
    # A merged run with an unresolved key cannot produce a ready gate.
    assert release_decision(db, new_audit)[1] == 'blocked'
    gate_audit, gate_status, _ = release_repair_merge(db, merge)
    assert gate_audit == new_audit and gate_status == 'blocked'
    new_sha = connect(db).execute('SELECT source_sha FROM target_snapshot WHERE id=?', (merged_target,)).fetchone()[0]
    with pytest.raises(ValueError, match='ack_identity_conflict'):
        acknowledge(db, 'ledger', run, 1, 'accepted', new_sha)
    acknowledge(db, 'ledger', run, 2, 'accepted', new_sha)
    gate_audit, gate_status, reason = release_repair_merge(db, merge)
    assert gate_audit == new_audit and gate_status == 'ready' and reason == 'all_critical_accepted'
    con = connect(db)
    assert con.execute('SELECT COUNT(*) FROM release_gate WHERE audit_id=?', (audit,)).fetchone()[0] == 0
    assert con.execute('SELECT COUNT(*) FROM release_gate WHERE audit_id=? AND status=?',
                       (new_audit, 'ready')).fetchone()[0] == 1
    # A merged snapshot remains auditable and a later mutation blocks release.
    with con:
        con.execute("UPDATE target_row SET value=? WHERE snapshot_id=? AND key='K3'", (json.dumps(5), merged_target))
    assert release_repair_merge(db, merge)[1] == 'blocked'
    assert release_decision(db, new_audit)[1] == 'blocked'
    # The earlier ready gate and the frozen audit rows are unchanged.
    assert connect(db).execute('SELECT COUNT(*) FROM release_gate WHERE status=?', ('ready',)).fetchone()[0] == 1
    assert connect(db).execute('SELECT status FROM cutover_audit WHERE id=?', (audit,)).fetchone()[0] == 'divergent'


def test_deferred_key_stays_unresolved_and_blocks_release(tmp_path):
    db, run, t0, audit = divergent_target(tmp_path)
    a = plan_repair(db, audit, manifest(tmp_path, 'defer-a.json', [
        {'key': 'K1', 'decision': 'defer', 'reason': 'needs finance sign-off'}]))
    b = plan_repair(db, audit, only_b(tmp_path))
    apply_repair(db, a)
    apply_repair(db, b)
    merge, merged_target, status = plan_repair_merge(db, [a, b])
    assert status == 'merged'
    # Deferring K1 does not change its value and is not treated as acceptance.
    assert target_rows(db, merged_target) == {'K1': 10, 'K2': 2, 'K3': 3}
    data = json.loads(write_repair_merge(db, merge, tmp_path / 'defer-report').read_text(encoding='utf-8'))
    keys = {row['key']: row for row in data['keys']}
    assert keys['K1']['outcome'] == 'deferred' and keys['K1']['unresolved']
    assert data['unresolved'] == ['K1']
    with pytest.raises(ValueError, match='repair_merge_incomplete'):
        release_repair_merge(db, merge)


def test_deferral_does_not_conflict_with_concrete_decision(tmp_path):
    db, run, t0, audit = divergent_target(tmp_path)
    a = plan_repair(db, audit, manifest(tmp_path, 'a-defer.json', [
        {'key': 'K1', 'decision': 'defer', 'reason': 'review pending'}]))
    b = plan_repair(db, audit, only_a(tmp_path))
    apply_repair(db, a)
    apply_repair(db, b)
    merge, merged_target, status = plan_repair_merge(db, [a, b])
    assert status == 'merged'
    row = next(row for row in repair_merge_report(db, merge)['keys'] if row['key'] == 'K1')
    assert row['outcome'] == 'decided' and row['decision'] == 'restore_replay'
    assert {source['decision'] for source in row['sources']} == {'defer', 'restore_replay'}
    assert target_rows(db, merged_target)['K1'] == 2


def test_restoring_a_deleted_key_removes_target_row(tmp_path):
    db, run, t0, audit = divergent_target(tmp_path)
    t0_rows = target_rows(db, t0)
    assert 'K2' in t0_rows
    # Both branches restore, but the replay for this second scenario deletes K2.
    delete_case = tmp_path / 'delete-case'
    delete_case.mkdir()
    db2 = base(delete_case, [{'key': 'K1', 'value': 1}, {'key': 'K2', 'value': 1}])
    events(delete_case, db2, [
        {'kind': 'begin', 'txn': 'T1', 'seq': 1},
        {'kind': 'change', 'txn': 'T1', 'seq': 2, 'key': 'K1', 'action': 'update', 'before': 1, 'after': 2},
        {'kind': 'change', 'txn': 'T1', 'seq': 3, 'key': 'K2', 'action': 'delete', 'before': 1, 'after': None},
        {'kind': 'commit', 'txn': 'T1', 'seq': 4}])
    run2, _, _ = replay(db2, 4)
    t02 = import_target(db2, file(delete_case, 't0.jsonl', [
        {'key': 'K1', 'value': 10}, {'key': 'K2', 'value': 20}]), 4)
    audit2, status, _ = audit_target(db2, run2, t02)
    assert status == 'divergent'
    a = plan_repair(db2, audit2, manifest(delete_case, 'a.json', [
        {'key': 'K1', 'decision': 'restore_replay', 'reason': 'replay wins'}]))
    b = plan_repair(db2, audit2, manifest(delete_case, 'b.json', [
        {'key': 'K2', 'decision': 'restore_replay', 'reason': 'replay deletion wins'}]))
    apply_repair(db2, a)
    apply_repair(db2, b)
    merge, merged_target, status = plan_repair_merge(db2, [a, b])
    assert status == 'merged'
    assert target_rows(db2, merged_target) == {'K1': 2}
    assert audit_target(db2, run2, merged_target)[1] == 'ready'


def test_full_scope_plan_and_merge_into_one_snapshot(tmp_path):
    db, run, t0, audit = divergent_target(tmp_path)
    full = manifest(tmp_path, 'full.json', [
        {'key': 'K1', 'decision': 'restore_replay', 'reason': 'replay wins'},
        {'key': 'K2', 'decision': 'restore_replay', 'reason': 'replay wins'}])
    plan = plan_repair(db, audit, full, scope='full')
    assert connect(db).execute('SELECT scope_kind FROM repair_plan WHERE id=?', (plan,)).fetchone()[0] == 'full'
    out = apply_repair(db, plan)[0]
    merge, merged_target, status = plan_repair_merge(db, [plan])
    assert status == 'merged' and merged_target == out
    assert target_rows(db, merged_target) == {'K1': 2, 'K2': 2, 'K3': 3}


def test_merge_rejects_unapplied_plan(tmp_path):
    db, run, t0, audit = divergent_target(tmp_path)
    a = plan_repair(db, audit, only_a(tmp_path))
    with pytest.raises(ValueError, match='repair_plan_not_applied'):
        plan_repair_merge(db, [a])


def test_merge_rejects_plans_from_different_audits(tmp_path):
    db, run, t0, audit = divergent_target(tmp_path)
    a = plan_repair(db, audit, only_a(tmp_path))
    apply_repair(db, a)
    # A second divergent target of the same run yields a different frozen audit.
    second_target = import_target(db, file(tmp_path, 't0b.jsonl', [
        {'key': 'K1', 'value': 11}, {'key': 'K2', 'value': 21}, {'key': 'K3', 'value': 3}]), 4)
    audit2, status, _ = audit_target(db, run, second_target)
    assert status == 'divergent' and audit2 != audit
    b = plan_repair(db, audit2, only_b(tmp_path))
    apply_repair(db, b)
    with pytest.raises(ValueError, match='repair_merge_audit_mismatch'):
        plan_repair_merge(db, [a, b])


def test_merge_rejects_unknown_plan_id(tmp_path):
    db, run, t0, audit = divergent_target(tmp_path)
    a = plan_repair(db, audit, only_a(tmp_path))
    apply_repair(db, a)
    with pytest.raises(ValueError, match='unknown_repair_plan'):
        plan_repair_merge(db, [a, 999])
    assert connect(db).execute('SELECT COUNT(*) FROM repair_merge').fetchone()[0] == 0
