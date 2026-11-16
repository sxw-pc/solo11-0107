import json
import hashlib
import pytest
from cdcverdict.ingest import snapshot,ingest
from cdcverdict.rules import replay,configure_retention,compact
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
def delete_then_recreate():
    return [{'kind':'begin','txn':'T1','seq':1},
      {'kind':'change','txn':'T1','seq':2,'key':'K','action':'delete','before':1},
      {'kind':'commit','txn':'T1','seq':3},
      {'kind':'begin','txn':'T2','seq':4},
      {'kind':'change','txn':'T2','seq':5,'key':'K','action':'insert','after':9},
      {'kind':'change','txn':'T2','seq':6,'key':'K','action':'update','before':9,'after':10},
      {'kind':'change','txn':'T2','seq':7,'key':'K','action':'update','before':10,'after':11},
      {'kind':'commit','txn':'T2','seq':8}]
def test_full_history_contract_replays_any_watermark(tmp_path):
    db=base(tmp_path,[{'key':'K','value':1}])
    assert configure_retention(db,'full_history')=='full_history'
    assert configure_retention(db,'full_history')=='full_history'
    with pytest.raises(ValueError,match='retention_contract_changed'):
        configure_retention(db,'retention_line')
    events(tmp_path,db,delete_then_recreate())
    with pytest.raises(ValueError,match='retention_contract_forbids_compaction'):
        compact(db,6)
    assert replay(db,3)[1]['K']=='已删除'
    assert replay(db,5)[1]['K']=='已删除'
    assert replay(db,8)[1]['K']=='存在'
    run,_,_=replay(db,5)
    data=json.loads(write(db,run,tmp_path/'full').read_text(encoding='utf-8'))
    assert data['retention']=={'mode':'full_history','floor':None,'compactions':[]}
def test_retention_line_expires_queries_below_floor(tmp_path):
    db=base(tmp_path,[{'key':'K','value':1}])
    configure_retention(db,'retention_line')
    events(tmp_path,db,delete_then_recreate())
    floor,digest=compact(db,6)
    assert floor==6 and digest
    conn=connect(db)
    assert conn.execute("SELECT COUNT(*) FROM change_event WHERE txn_id='T1'").fetchone()[0]==0
    assert {r[0] for r in conn.execute('SELECT seq FROM change_event')}=={4,5,6,7,8}
    row=conn.execute('SELECT floor,summary,digest FROM retention_compaction').fetchone()
    summary=json.loads(row['summary'])
    assert row['floor']==6 and row['digest']==digest
    assert hashlib.sha256(row['summary'].encode('utf-8')).hexdigest()==row['digest']
    assert summary['tombstones']['K']['deleted_seq']==2
    assert summary['tombstones']['K']['before_digest']==hashlib.sha256(b'1').hexdigest()
    run,out,excluded=replay(db,5)
    assert out['K']=='历史已过期' and excluded==[]
    assert conn.execute('SELECT status FROM replay_run WHERE id=?',(run,)).fetchone()[0]=='历史已过期'
    assert conn.execute("SELECT COUNT(*) FROM issue WHERE code='history_expired'").fetchone()[0]==1
    assert replay(db,6)[1]['K']=='已删除'
    assert replay(db,8)[1]['K']=='存在'
    with pytest.raises(ValueError,match='retention_floor_regression'):
        compact(db,6)
    with pytest.raises(ValueError,match='retention_floor_regression'):
        compact(db,4)
def test_retention_contract_required_for_compaction(tmp_path):
    db=base(tmp_path)
    with pytest.raises(ValueError,match='retention_contract_missing'):
        compact(db,1)
    with pytest.raises(ValueError,match='invalid_retention_mode'):
        configure_retention(db,'sometimes')
    configure_retention(db,'retention_line')
    with pytest.raises(ValueError,match='invalid_retention_floor'):
        compact(db,-1)
def test_compaction_preserves_archived_run_reports(tmp_path):
    db=base(tmp_path,[{'key':'K','value':1}])
    events(tmp_path,db,delete_then_recreate())
    old,_,_=replay(db,3)
    first=json.loads(write(db,old,tmp_path/'old').read_text(encoding='utf-8'))
    assert first['decisions'][0]['status']=='已删除'
    configure_retention(db,'retention_line')
    compact(db,6)
    conn=connect(db)
    summary=json.loads(conn.execute('SELECT summary FROM retention_compaction WHERE floor=6').fetchone()[0])
    pinned=[r for r in summary['runs'] if r['id']==old]
    assert pinned and pinned[0]['watermark']==3 and pinned[0]['status']=='已归档' and pinned[0]['digest']
    second=json.loads(write(db,old,tmp_path/'old-again').read_text(encoding='utf-8'))
    assert second['decisions']==first['decisions']
    assert second['evidence']==first['evidence']
    assert second['issues']==first['issues']
    assert second['excluded_transactions']==first['excluded_transactions']==['T2']
    assert second['retention']['mode']=='retention_line' and second['retention']['floor']==6
    assert conn.execute('SELECT status FROM visible_row WHERE run_id=? AND key=?',(old,'K')).fetchone()[0]=='已删除'
    _,out,_=replay(db,3)
    assert out['K']=='历史已过期'
    assert replay(db,8)[1]['K']=='存在'
def test_compaction_preserves_alias_chain_and_before_guard(tmp_path):
    db=base(tmp_path,[{'key':'A','value':1}])
    events(tmp_path,db,[{'kind':'begin','txn':'T1','seq':1},
      {'kind':'change','txn':'T1','seq':2,'key':'A','action':'rename','alias_to':'B'},
      {'kind':'commit','txn':'T1','seq':3}])
    old,old_out,_=replay(db,3)
    assert old_out=={'B':'存在'}
    first=json.loads(write(db,old,tmp_path/'old').read_text(encoding='utf-8'))
    configure_retention(db,'retention_line')
    compact(db,5)
    conn=connect(db)
    assert conn.execute('SELECT new_key FROM key_alias WHERE old_key=?',('A',)).fetchone()[0]=='B'
    again=json.loads(write(db,old,tmp_path/'old-again').read_text(encoding='utf-8'))
    assert again['decisions']==first['decisions'] and again['evidence']==first['evidence']
    events(tmp_path,db,[{'kind':'begin','txn':'T2','seq':6},
      {'kind':'change','txn':'T2','seq':7,'key':'A','action':'update','before':1,'after':2},
      {'kind':'commit','txn':'T2','seq':8}])
    run,out,_=replay(db,8)
    assert out=={'B':'存在'}
    assert json.loads(conn.execute('SELECT value FROM decision WHERE run_id=? AND key=?',(run,'B')).fetchone()[0])==2
    events(tmp_path,db,[{'kind':'begin','txn':'T3','seq':9},
      {'kind':'change','txn':'T3','seq':10,'key':'A','action':'update','before':999,'after':3},
      {'kind':'commit','txn':'T3','seq':11}])
    _,out,_=replay(db,11)
    assert out['A']=='无法判定' and out['B']=='存在'
    run,_,_=replay(db,11)
    assert json.loads(conn.execute("SELECT value FROM decision WHERE run_id=? AND key='B'",(run,)).fetchone()[0])==2
def test_compacted_tombstone_blocks_stale_insert(tmp_path):
    db=base(tmp_path,[{'key':'K','value':1}])
    configure_retention(db,'retention_line')
    events(tmp_path,db,delete_then_recreate()[:3])
    compact(db,6)
    events(tmp_path,db,[{'kind':'begin','txn':'T9','seq':1},
      {'kind':'change','txn':'T9','seq':2,'key':'K','action':'insert','after':7},
      {'kind':'commit','txn':'T9','seq':9}])
    _,out,_=replay(db,9)
    assert out['K']=='无法判定'
def test_compacted_tombstone_blocks_before_resurrection(tmp_path):
    db=base(tmp_path,[{'key':'K','value':1}])
    configure_retention(db,'retention_line')
    events(tmp_path,db,delete_then_recreate()[:3])
    compact(db,6)
    events(tmp_path,db,[{'kind':'begin','txn':'T2','seq':7},
      {'kind':'change','txn':'T2','seq':8,'key':'K','action':'update','before':1,'after':5},
      {'kind':'commit','txn':'T2','seq':9}])
    _,out,_=replay(db,9)
    assert out['K']=='无法判定'
def test_expired_run_report_shows_retention_line(tmp_path):
    db=base(tmp_path,[{'key':'K','value':1}])
    configure_retention(db,'retention_line')
    events(tmp_path,db,delete_then_recreate())
    compact(db,6)
    run,out,_=replay(db,5)
    assert out=={'K':'历史已过期'}
    data=json.loads(write(db,run,tmp_path/'expired').read_text(encoding='utf-8'))
    assert data['retention']['mode']=='retention_line' and data['retention']['floor']==6
    assert len(data['retention']['compactions'])==1 and data['retention']['compactions'][0]['digest']
    assert data['decisions']==[{'run_id':run,'key':'K','status':'历史已过期','value':None,'rule_version':'1'}]
    assert [i['code'] for i in data['issues']]==['history_expired']
    assert connect(db).execute('SELECT status FROM replay_run WHERE id=?',(run,)).fetchone()[0]=='历史已过期'
