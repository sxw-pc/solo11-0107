import json
from pathlib import Path

import pytest

from cdcverdict.__main__ import demo
from cdcverdict.cutover import import_target, audit_target, configure_consumers, acknowledge, release_decision
from cdcverdict.repair import plan_repair, apply_repair, repair_report
from cdcverdict.store import connect


FIXTURES = Path(__file__).parent / 'fixtures'


def seeded(tmp_path):
    demo(tmp_path)
    db = tmp_path / 'data.db'
    run = connect(db).execute('SELECT MAX(id) FROM replay_run').fetchone()[0]
    file = tmp_path / 'divergent.jsonl'
    file.write_text('\n'.join(json.dumps(x) for x in [
        {'key': 'A', 'value': {'n': 9}}, {'key': 'B', 'value': {'n': 7}}]), encoding='utf-8')
    target = import_target(db, file, 3)
    audit, status, differences = audit_target(db, run, target)
    assert status == 'divergent' and len(differences) == 2
    return db, run, target, audit


def test_partial_choice_creates_new_snapshot_then_second_repair_closes_gap(tmp_path):
    db, run, original, audit = seeded(tmp_path)
    plan = plan_repair(db, audit, FIXTURES / 'repair_partial.json')
    assert plan_repair(db, audit, FIXTURES / 'repair_partial.json') == plan
    first, result = apply_repair(db, plan)
    assert result == 'partial' and first != original
    assert apply_repair(db, plan) == (first, 'partial')
    con = connect(db)
    assert json.loads(con.execute("SELECT value FROM target_row WHERE snapshot_id=? AND key='A'", (original,)).fetchone()[0]) == {'n': 9}
    assert json.loads(con.execute("SELECT value FROM target_row WHERE snapshot_id=? AND key='A'", (first,)).fetchone()[0]) == {'n': 2}
    mid_audit, mid_status, remaining = audit_target(db, run, first)
    assert mid_status == 'divergent' and [x[0] for x in remaining] == ['B']
    second_plan = plan_repair(db, mid_audit, FIXTURES / 'repair_finish.json')
    second, result = apply_repair(db, second_plan)
    assert result == 'repaired' and second != first
    assert audit_target(db, run, second)[1] == 'ready'
    assert [r['decision'] for r in repair_report(db, plan)['choices']] == ['restore_replay', 'accept_target']


def test_repaired_snapshot_requires_new_consumer_ack(tmp_path):
    db, run, original, audit = seeded(tmp_path)
    contract = tmp_path / 'consumer.json'
    contract.write_text(json.dumps([{'id': 'ledger', 'critical': True, 'minimum_watermark': 3}]), encoding='utf-8')
    configure_consumers(db, contract)
    old_sha = connect(db).execute('SELECT source_sha FROM target_snapshot WHERE id=?', (original,)).fetchone()[0]
    acknowledge(db, 'ledger', run, 1, 'accepted', old_sha)
    assert release_decision(db, audit)[1] == 'blocked'
    first_plan = plan_repair(db, audit, FIXTURES / 'repair_partial.json')
    first = apply_repair(db, first_plan)[0]
    mid = audit_target(db, run, first)[0]
    second_plan = plan_repair(db, mid, FIXTURES / 'repair_finish.json')
    final_target = apply_repair(db, second_plan)[0]
    final_audit = audit_target(db, run, final_target)[0]
    assert release_decision(db, final_audit)[1] == 'blocked'
    new_sha = connect(db).execute('SELECT source_sha FROM target_snapshot WHERE id=?', (final_target,)).fetchone()[0]
    acknowledge(db, 'ledger', run, 2, 'accepted', new_sha)
    assert release_decision(db, final_audit)[1] == 'ready'


def test_repair_plan_rejects_missing_decision_and_stale_target(tmp_path):
    db, run, target, audit = seeded(tmp_path)
    with pytest.raises(ValueError, match='repair_manifest_incomplete'):
        plan_repair(db, audit, FIXTURES / 'repair_incomplete.json')
    plan = plan_repair(db, audit, FIXTURES / 'repair_partial.json')
    con = connect(db)
    before = con.execute('SELECT COUNT(*) FROM target_snapshot').fetchone()[0]
    with con:
        con.execute("UPDATE target_row SET value=? WHERE snapshot_id=? AND key='B'", (json.dumps({'n': 8}), target))
    with pytest.raises(ValueError, match='audited_target_changed'):
        apply_repair(db, plan)
    assert con.execute('SELECT COUNT(*) FROM target_snapshot').fetchone()[0] == before
    assert repair_report(db, plan)['plan']['status'] == 'planned'


def test_accepting_all_deviations_does_not_claim_repaired_state(tmp_path):
    db, run, target, audit = seeded(tmp_path)
    file = tmp_path / 'accept.json'
    file.write_text(json.dumps([
        {'key': key, 'decision': 'accept_target', 'reason': 'business owner retained this target state'}
        for key in ['A', 'B']]), encoding='utf-8')
    plan = plan_repair(db, audit, file)
    same_target, result = apply_repair(db, plan)
    assert same_target == target and result == 'accepted_deviation'
    assert audit_target(db, run, same_target)[1] == 'divergent'
