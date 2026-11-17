import json
from pathlib import Path
import pytest
from cdcverdict.__main__ import demo,decision_demo,consent_demo
from cdcverdict.consent import import_consent,create_export,audit_withdrawals,acknowledge_recall,export_report
from cdcverdict.store import connect

FIX=Path(__file__).parent/'fixtures'

def setup(tmp_path,decision=False):
    root=tmp_path/'work'
    state=decision_demo(root) if decision else None
    if state is None:demo(root)
    db=root/'data.db';run=connect(db).execute('SELECT MAX(id) FROM replay_run').fetchone()[0]
    import_consent(db,FIX/'consent_grant.json')
    return db,run,state

def test_purpose_limited_export_uses_replay_visible_value(tmp_path):
    db,run,_=setup(tmp_path)
    eid,status=create_export(db,run,'research','lab')
    r=export_report(db,eid)
    assert status=='complete' and len(r['rows'])==1
    assert r['rows'][0]['value']==json.dumps({'n':2},sort_keys=True,ensure_ascii=False)
    other,other_status=create_export(db,run,'marketing','agency')
    assert other_status=='empty' and export_report(db,other)['excluded'][0]['reason']=='no_grant'

def test_late_withdrawal_recalls_old_export_and_excludes_new(tmp_path):
    db,run,_=setup(tmp_path)
    first,_=create_export(db,run,'research','lab')
    import_consent(db,FIX/'consent_withdraw_late.json')
    assert export_report(db,first)['current_permission']=='recall_pending'
    recalls=audit_withdrawals(db,first)
    assert len(recalls)==1 and audit_withdrawals(db,first)==recalls
    second,status=create_export(db,run,'research','lab')
    assert status=='empty' and export_report(db,second)['excluded'][0]['reason']=='withdrawn'
    assert export_report(db,first)['rows'][0]['key']=='A' and export_report(db,first)['current_permission']=='recall_pending'

def test_recall_ack_requires_original_recipient_and_is_idempotent(tmp_path):
    db,run,_=setup(tmp_path);first,_=create_export(db,run,'research','lab')
    import_consent(db,FIX/'consent_withdraw_late.json');rid=audit_withdrawals(db,first)[0]
    with pytest.raises(ValueError,match='wrong_export_recipient'):acknowledge_recall(db,rid,'other-lab','receipt-proof-1')
    receipt=acknowledge_recall(db,rid,'lab','receipt-proof-1')
    assert acknowledge_recall(db,rid,'lab','receipt-proof-1')==receipt
    with pytest.raises(ValueError,match='recall_receipt_conflict'):acknowledge_recall(db,rid,'lab','different-proof')
    assert export_report(db,first)['recalls'][0]['status']=='acknowledged'

def test_future_withdrawal_does_not_revoke_earlier_watermark(tmp_path):
    db,run,_=setup(tmp_path);first,_=create_export(db,run,'research','lab')
    import_consent(db,FIX/'consent_withdraw_future.json')
    assert export_report(db,first)['current_permission']=='valid'
    assert audit_withdrawals(db,first)==[]
    second,status=create_export(db,run,'research','lab')
    assert status=='complete' and export_report(db,second)['rows']

def test_conflicting_consent_version_preserves_original_and_raw_failure(tmp_path):
    db,run,_=setup(tmp_path)
    with pytest.raises(ValueError,match='consent_version_conflict'):import_consent(db,FIX/'consent_conflict.json')
    conn=connect(db)
    assert conn.execute("SELECT action FROM consent_event WHERE id='g1'").fetchone()[0]=='grant'
    bad=conn.execute("SELECT raw,error FROM consent_source WHERE status='rejected'").fetchone()
    assert bad['raw'] and 'consent_version_conflict' in bad['error'];conn.close()
    assert create_export(db,run,'research','lab')[1]=='complete'

def test_regrant_allows_new_export_but_does_not_erase_recall(tmp_path):
    db,run,_=setup(tmp_path);first,_=create_export(db,run,'research','lab')
    import_consent(db,FIX/'consent_withdraw_late.json');rid=audit_withdrawals(db,first)[0]
    import_consent(db,FIX/'consent_regrant.json')
    second,status=create_export(db,run,'research','lab')
    assert status=='complete' and export_report(db,second)['rows'][0]['consent_event_id']=='g3'
    assert export_report(db,first)['recalls'][0]['id']==rid

def test_release_gate_can_be_ready_while_export_recall_pending(tmp_path):
    db,run,state=setup(tmp_path,decision=True)
    assert state['release_status']=='ready'
    first,_=create_export(db,run,'research','lab')
    import_consent(db,FIX/'consent_withdraw_late.json');audit_withdrawals(db,first)
    conn=connect(db)
    assert conn.execute('SELECT status FROM release_gate WHERE id=?',(state['release_gate'],)).fetchone()[0]=='ready'
    assert conn.execute("SELECT status FROM export_recall WHERE export_id=?",(first,)).fetchone()[0]=='pending'
    assert create_export(db,run,'research','lab')[1]=='empty'
    conn.close()

def test_subject_key_cannot_be_reassigned(tmp_path):
    db,run,_=setup(tmp_path)
    file=tmp_path/'reassign.json';file.write_text(json.dumps({'subjects':[{'id':'other','owner':'owner-B','keys':['A']}]}))
    with pytest.raises(ValueError,match='subject_key_conflict'):import_consent(db,file)
    conn=connect(db);assert conn.execute("SELECT subject_id FROM subject_key WHERE key='A'").fetchone()[0]=='subject-A';conn.close()

def test_demo_consent(tmp_path):
    result=consent_demo(tmp_path/'demo')
    assert result['initial_status']=='complete' and result['after_status']=='empty'
    assert export_report(result['db'],result['first_export'])['recalls'][0]['status']=='acknowledged'
