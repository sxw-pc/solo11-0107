import json
import pytest
from cdcverdict.ingest import snapshot,ingest
from cdcverdict.rules import replay
from cdcverdict.report import write
from cdcverdict.store import connect
from cdcverdict.contract import register_schema,register_mapping
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

V2_FIELDS=[{'name':'display_name','required':True},{'name':'locale','required':False},{'name':'count','required':False}]
def schema_base(tmp,mappings='full',v2_watermark=1):
    """v1 基线 A={name:'Lin'}, B={count:2}；v2 事件迁移 A 并把 B.count 改为 3。"""
    db=tmp/'data.db'
    snapshot(file(tmp,'base.jsonl',[{'key':'A','value':{'name':'Lin'}},{'key':'B','value':{'count':2}}]),db,0,schema='v1')
    register_schema(db,'v1',0,['name','count'])
    register_schema(db,'v2',v2_watermark,V2_FIELDS)
    ms=[{'target':'locale','kind':'default','value':'zh'},
        {'source':'count','target':'count','kind':'identity'}]
    if mappings=='full':ms.insert(0,{'source':'name','target':'display_name','kind':'rename'})
    register_mapping(db,'v1','v2',ms)
    rows=[{'kind':'begin','txn':'T1','seq':1},
      {'kind':'change','txn':'T1','seq':2,'key':'A','action':'update','before':{'name':'Lin'},'after':{'display_name':'Lin','locale':'zh'},'schema':'v2'},
      {'kind':'commit','txn':'T1','seq':3},
      {'kind':'begin','txn':'T2','seq':4},
      {'kind':'change','txn':'T2','seq':5,'key':'B','action':'update','before':{'count':2},'after':{'count':3},'schema':'v2'},
      {'kind':'commit','txn':'T2','seq':6}]
    events(tmp,db,rows)
    return db
def test_hard_strategy_rejects_version_drift(tmp_path):
    db=schema_base(tmp_path)
    run,out,_=replay(db,6,strategy='hard',target_schema='v2')
    assert out=={'A':'无法判定','B':'无法判定'}
    con=connect(db)
    assert con.execute("SELECT COUNT(*) FROM issue WHERE code='schema_version_blocked'").fetchone()[0]==1
    assert con.execute('SELECT status FROM replay_run WHERE id=?',(run,)).fetchone()[0]=='待核'
    assert con.execute('SELECT COUNT(*) FROM decision WHERE run_id=? AND value IS NOT NULL',(run,)).fetchone()[0]==0
def test_hard_strategy_allows_identical_versions(tmp_path):
    db=base(tmp_path);events(tmp_path,db,tx())
    run,out,_=replay(db,3,strategy='hard',target_schema='v1')
    assert out['A']=='存在'
    row=connect(db).execute('SELECT value,rule_version FROM decision WHERE run_id=?',(run,)).fetchone()
    assert json.loads(row['value'])==2 and row['rule_version']=='2'
def test_compatible_projection_rename_default_and_identity(tmp_path):
    db=schema_base(tmp_path)
    run,out,_=replay(db,6,strategy='compatible',target_schema='v2')
    assert out=={'A':'存在','B':'存在'}
    con=connect(db)
    got={r['key']:json.loads(r['value']) for r in con.execute('SELECT key,value FROM decision WHERE run_id=?',(run,))}
    assert got['A']=={'display_name':'Lin','locale':'zh'} and got['B']=={'count':3}
    data=json.loads(write(db,run,tmp_path/'report').read_text(encoding='utf-8'))
    versions={r['key']:r for r in data['schema_versions']}
    assert versions['A']['baseline_schema']=='v1' and versions['A']['event_schema']=='v2'
    assert versions['A']['target_schema']=='v2' and versions['A']['before_status']=='matched'
    assert versions['B']['before_status']=='matched'
    lineage={(r['key'],r['field']):r for r in data['field_lineage']}
    assert lineage[('A','display_name')]['source_field']=='name'
    assert 'rename' in lineage[('A','display_name')]['chain']
    assert lineage[('A','locale')]['source_field'] is None
    assert 'default' in lineage[('A','locale')]['chain']
    assert lineage[('B','count')]['source_field']=='count' and 'identity' in lineage[('B','count')]['chain']
def test_unmapped_required_field_blocks_only_its_key(tmp_path):
    db=schema_base(tmp_path,mappings='partial')
    run,out,_=replay(db,6,strategy='compatible',target_schema='v2')
    assert out=={'A':'无法判定','B':'存在'}
    con=connect(db)
    row=con.execute('SELECT value,status FROM visible_row WHERE run_id=? AND key=?',(run,'A')).fetchone()
    assert row['value'] is None and row['status']=='无法判定'
    assert json.loads(con.execute('SELECT value FROM decision WHERE run_id=? AND key=?',(run,'B')).fetchone()[0])=={'count':3}
    issue=con.execute("SELECT detail FROM issue WHERE code='schema_projection' AND key='A'").fetchone()
    assert issue and 'display_name' in issue['detail']
def test_type_conversion_lossless_and_lossy(tmp_path):
    db=tmp_path/'data.db'
    snapshot(file(tmp_path,'base.jsonl',[{'key':'K1','value':{'code':'42'}},{'key':'K2','value':{'code':'4.5'}},{'key':'K3','value':{'code':'abc'}}]),db,0,schema='v1')
    register_schema(db,'v1',0,['code']);register_schema(db,'v2',1,['code'])
    register_mapping(db,'v1','v2',[{'source':'code','target':'code','kind':'convert','value':'str_to_int'}])
    rows=[{'kind':'begin','txn':'T1','seq':1},
      {'kind':'change','txn':'T1','seq':2,'key':'K1','action':'update','before':{'code':'42'},'after':{'code':42},'schema':'v2'},
      {'kind':'commit','txn':'T1','seq':3},
      {'kind':'begin','txn':'T2','seq':4},
      {'kind':'change','txn':'T2','seq':5,'key':'K2','action':'update','before':{'code':'4.5'},'after':{'code':4},'schema':'v2'},
      {'kind':'commit','txn':'T2','seq':6}]
    events(tmp_path,db,rows)
    run,out,_=replay(db,6,strategy='compatible',target_schema='v2')
    assert out=={'K1':'存在','K2':'无法判定','K3':'无法判定'}
    con=connect(db)
    assert json.loads(con.execute('SELECT value FROM decision WHERE run_id=? AND key=?',(run,'K1')).fetchone()[0])=={'code':42}
    assert con.execute("SELECT COUNT(*) FROM issue WHERE code='schema_projection' AND key='K2'").fetchone()[0]==1
    assert con.execute("SELECT COUNT(*) FROM issue WHERE code='schema_unprojectable' AND key='K3'").fetchone()[0]==1
def test_field_conversion_and_before_are_atomic_per_transaction(tmp_path):
    db=tmp_path/'data.db'
    snapshot(file(tmp_path,'base.jsonl',[{'key':'A','value':{'name':'Lin'}},{'key':'C','value':{'name':'Bo'}}]),db,0,schema='v1')
    register_schema(db,'v1',0,['name'])
    register_schema(db,'v2',1,[{'name':'display_name','required':True},{'name':'locale','required':False}])
    register_mapping(db,'v1','v2',[{'source':'name','target':'display_name','kind':'rename'},{'target':'locale','kind':'default','value':'zh'}])
    rows=[{'kind':'begin','txn':'T1','seq':1},
      {'kind':'change','txn':'T1','seq':2,'key':'A','action':'update','before':{'name':'Lin'},'after':{'display_name':'Lin','locale':'zh'},'schema':'v2'},
      {'kind':'change','txn':'T1','seq':3,'key':'C','action':'update','before':{'name':'WRONG'},'after':{'display_name':'Bo','locale':'zh'},'schema':'v2'},
      {'kind':'commit','txn':'T1','seq':4}]
    events(tmp_path,db,rows)
    run,out,_=replay(db,4,strategy='compatible',target_schema='v2')
    assert out=={'A':'无法判定','C':'无法判定'}
    con=connect(db)
    assert con.execute('SELECT value FROM visible_row WHERE run_id=? AND key=?',(run,'A')).fetchone()[0] is None
    assert con.execute('SELECT COUNT(*) FROM evidence WHERE run_id=?',(run,)).fetchone()[0]==0
    ks={r['key']:r['before_status'] for r in con.execute('SELECT key,before_status FROM key_schema WHERE run_id=?',(run,))}
    assert ks=={'A':'matched','C':'mismatch'}
def test_schema_versions_take_effect_by_watermark(tmp_path):
    db=schema_base(tmp_path,v2_watermark=5)
    _,out_early,_=replay(db,3,strategy='compatible',target_schema='v2')
    assert out_early=={'A':'无法判定','B':'无法判定'}
    _,out_late,_=replay(db,6,strategy='compatible',target_schema='v2')
    assert out_late=={'A':'存在','B':'存在'}
def test_legacy_run_is_not_rewritten_by_strategy_run(tmp_path):
    db=schema_base(tmp_path)
    legacy_run,_,_=replay(db,6)
    con=connect(db)
    before={r['key']:(r['status'],r['value'],r['rule_version']) for r in con.execute('SELECT * FROM decision WHERE run_id=?',(legacy_run,))}
    run,out,_=replay(db,6,strategy='compatible',target_schema='v2')
    after={r['key']:(r['status'],r['value'],r['rule_version']) for r in con.execute('SELECT * FROM decision WHERE run_id=?',(legacy_run,))}
    assert before==after and all(v[2]=='1' for v in after.values())
    assert con.execute('SELECT DISTINCT rule_version FROM decision WHERE run_id=?',(run,)).fetchone()[0]=='2'
    assert out=={'A':'存在','B':'存在'}
def test_schema_registration_conflicts(tmp_path):
    db=tmp_path/'data.db'
    register_schema(db,'v1',0,['name']);register_schema(db,'v1',0,['name'])
    with pytest.raises(ValueError,match='schema_contract_changed'):register_schema(db,'v1',2,['name'])
    with pytest.raises(ValueError,match='schema_contract_changed'):register_schema(db,'v1',0,['other'])
    with pytest.raises(ValueError,match='unknown_schema_version'):
        register_mapping(db,'v1','v9',[{'source':'name','target':'name','kind':'identity'}])
    register_schema(db,'v2',1,['name'])
    register_mapping(db,'v1','v2',[{'source':'name','target':'name','kind':'identity'}])
    register_mapping(db,'v1','v2',[{'source':'name','target':'name','kind':'identity'}])
    with pytest.raises(ValueError,match='mapping_contract_changed'):
        register_mapping(db,'v1','v2',[{'target':'name','kind':'default','value':'x'}])
    with pytest.raises(ValueError,match='unknown_conversion'):
        register_mapping(db,'v2','v1',[{'source':'name','target':'name','kind':'convert','value':'nope'}])
def test_unknown_strategy_rejected(tmp_path):
    db=base(tmp_path)
    with pytest.raises(ValueError,match='unknown_strategy'):replay(db,0,strategy='loose')
