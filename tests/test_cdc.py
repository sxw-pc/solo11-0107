import json
import hashlib
import shutil
import pytest
from cdcverdict.ingest import snapshot,ingest
from cdcverdict.rules import replay
import cdcverdict.report as report_mod
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
def judged(tmp_path):
    db=base(tmp_path);events(tmp_path,db,tx());run,_,_=replay(db,3)
    assert connect(db).execute('SELECT status FROM replay_run WHERE id=?',(run,)).fetchone()[0]=='已判定'
    return db,run
def sha_of(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def manifest_of(bundle):return json.loads((bundle/'manifest.json').read_text(encoding='utf-8'))
def assert_bundle_verified(bundle):
    manifest=manifest_of(bundle)
    assert sorted(p.name for p in bundle.iterdir())==['manifest.json','report.json','report.md']
    for m in manifest['members']:assert sha_of(bundle/m['name'])==m['sha']
    return manifest
def test_certified_bundle_contains_manifest_and_members(tmp_path):
    db,run=judged(tmp_path)
    path=write(db,run,tmp_path/'report');bundle=path.parent
    manifest=assert_bundle_verified(bundle)
    assert manifest['contract']=='certified' and manifest['provisional'] is False and manifest['missing']==[]
    assert {m['name'] for m in manifest['members']}=={'report.json','report.md'}
    assert connect(db).execute('SELECT status FROM replay_run WHERE id=?',(run,)).fetchone()[0]=='已归档'
def test_report_retry_is_idempotent(tmp_path):
    db,run=judged(tmp_path)
    first=write(db,run,tmp_path/'report')
    before={p.name:p.read_bytes() for p in first.parent.iterdir()}
    again=write(db,run,tmp_path/'report')
    assert again==first
    assert {p.name:p.read_bytes() for p in again.parent.iterdir()}==before
    con=connect(db)
    rows=con.execute('SELECT status,generation FROM report_batch WHERE run_id=?',(run,)).fetchall()
    assert len(rows)==1 and rows[0]['status']=='complete' and rows[0]['generation']==1
def test_content_change_opens_new_generation_without_touching_old_bundle(tmp_path):
    db,run=judged(tmp_path)
    first=write(db,run,tmp_path/'report')
    gen1={p.name:p.read_bytes() for p in first.parent.iterdir()}
    with connect(db) as c:c.execute('INSERT INTO expected VALUES (?,?)',('A','999'))
    second=write(db,run,tmp_path/'report')
    assert second.parent!=first.parent and second.parent.name.endswith('-gen2')
    assert {p.name:p.read_bytes() for p in first.parent.iterdir()}==gen1
    assert json.loads(second.read_text(encoding='utf-8'))['expected_differences']==['A']
    digests=[r[0] for r in connect(db).execute('SELECT digest FROM report_batch WHERE run_id=? ORDER BY generation',(run,))]
    assert len(digests)==2 and digests[0]!=digests[1]
def test_certified_failure_leaves_no_final_bundle_then_converges(tmp_path):
    db,run=judged(tmp_path)
    bundle=tmp_path/'report-run1-gen1';(bundle/'report.md').mkdir(parents=True)
    with pytest.raises(OSError):write(db,run,tmp_path/'report')
    con=connect(db)
    assert con.execute('SELECT status FROM replay_run WHERE id=?',(run,)).fetchone()[0]=='待核'
    assert con.execute("SELECT status FROM report_batch WHERE run_id=?",(run,)).fetchone()[0]=='staging'
    assert (bundle/'report.json').exists() and not (bundle/'manifest.json').exists()
    partial=(bundle/'report.json').read_bytes()
    shutil.rmtree(bundle/'report.md')
    path=write(db,run,tmp_path/'report')
    assert path.parent==bundle
    assert (bundle/'report.json').read_bytes()==partial
    manifest=assert_bundle_verified(bundle)
    assert manifest['provisional'] is False and manifest['missing']==[]
    assert connect(db).execute('SELECT status FROM replay_run WHERE id=?',(run,)).fetchone()[0]=='已归档'
def test_crash_after_files_before_status_update_recovers(tmp_path,monkeypatch):
    db,run=judged(tmp_path)
    def crash(*a,**k):raise RuntimeError('power_loss')
    monkeypatch.setattr(report_mod,'archive',crash)
    with pytest.raises(RuntimeError):write(db,run,tmp_path/'report')
    monkeypatch.undo()
    con=connect(db)
    assert con.execute("SELECT status FROM report_batch WHERE run_id=?",(run,)).fetchone()[0]=='complete'
    assert con.execute('SELECT status FROM replay_run WHERE id=?',(run,)).fetchone()[0]=='已判定'
    bundle=tmp_path/'report-run1-gen1'
    assert sorted(p.name for p in bundle.iterdir())==['manifest.json','report.json','report.md']
    path=write(db,run,tmp_path/'report')
    assert path==bundle/'report.json'
    assert connect(db).execute('SELECT status FROM replay_run WHERE id=?',(run,)).fetchone()[0]=='已归档'
def test_preview_provisional_then_upgrade_with_same_json_digest(tmp_path):
    db,run=judged(tmp_path)
    path=write(db,run,tmp_path/'report',contract='preview',missing=('report.md',))
    bundle=path.parent
    assert json.loads(path.read_text(encoding='utf-8'))['decisions'][0]['status']=='存在'
    manifest=manifest_of(bundle)
    assert manifest['provisional'] is True and manifest['missing']==['report.md']
    assert not (bundle/'report.md').exists()
    assert connect(db).execute('SELECT status FROM replay_run WHERE id=?',(run,)).fetchone()[0]!='已归档'
    json_sha=sha_of(path)
    final=write(db,run,tmp_path/'report',contract='preview')
    assert final.parent==bundle
    upgraded=assert_bundle_verified(bundle)
    assert upgraded['provisional'] is False and upgraded['missing']==[]
    assert sha_of(final)==json_sha
    assert connect(db).execute('SELECT status FROM replay_run WHERE id=?',(run,)).fetchone()[0]=='已归档'
def test_shared_failure_fixture_both_contracts_converge(tmp_path):
    db,run=judged(tmp_path)
    certified=tmp_path/'report-run1-gen1';(certified/'report.md').mkdir(parents=True)
    with pytest.raises(OSError):write(db,run,tmp_path/'report')
    assert not (certified/'manifest.json').exists()
    con=connect(db)
    assert con.execute('SELECT status FROM replay_run WHERE id=?',(run,)).fetchone()[0]=='待核'
    provisional=write(db,run,tmp_path/'report',contract='preview',missing=('report.md',))
    preview_bundle=provisional.parent
    pmanifest=manifest_of(preview_bundle)
    assert pmanifest['provisional'] is True and pmanifest['missing']==['report.md']
    assert json.loads(provisional.read_text(encoding='utf-8'))['decisions']
    assert connect(db).execute('SELECT status FROM replay_run WHERE id=?',(run,)).fetchone()[0]=='待核'
    preview_json_sha=sha_of(provisional)
    shutil.rmtree(certified/'report.md')
    cpath=write(db,run,tmp_path/'report')
    assert cpath.parent==certified
    assert_bundle_verified(certified)
    ppath=write(db,run,tmp_path/'report',contract='preview')
    assert ppath.parent==preview_bundle
    final=assert_bundle_verified(preview_bundle)
    assert final['provisional'] is False and final['missing']==[]
    assert sha_of(ppath)==preview_json_sha
    assert connect(db).execute('SELECT status FROM replay_run WHERE id=?',(run,)).fetchone()[0]=='已归档'
def test_publish_contract_validation(tmp_path):
    db=base(tmp_path);run,_,_=replay(db,0)
    with pytest.raises(ValueError,match='unknown_publish_contract'):
        write(db,run,tmp_path/'r',contract='final')
    with pytest.raises(ValueError,match='certified_requires_complete_bundle'):
        write(db,run,tmp_path/'r',missing=('report.md',))
    with pytest.raises(ValueError,match='unknown_report_member'):
        write(db,run,tmp_path/'r',contract='preview',missing=('manifest.json',))
def test_undecided_run_is_not_archived_by_complete_bundle(tmp_path):
    db=base(tmp_path);events(tmp_path,db,tx(before=7));run,_,_=replay(db,3)
    con=connect(db)
    assert con.execute('SELECT status FROM replay_run WHERE id=?',(run,)).fetchone()[0]=='待核'
    write(db,run,tmp_path/'report')
    assert con.execute("SELECT status FROM report_batch WHERE run_id=?",(run,)).fetchone()[0]=='complete'
    assert con.execute('SELECT status FROM replay_run WHERE id=?',(run,)).fetchone()[0]=='待核'
