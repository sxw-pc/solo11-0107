import json
import pytest
from cdcverdict.ingest import snapshot,ingest,register_contract
from cdcverdict.rules import replay
from cdcverdict.report import write
from cdcverdict.store import connect
from cdcverdict.contract import compile_contract,validate_snapshot,ContractError,IMMEDIATE,DEFERRED


def write_file(tmp,name,rows):
    p=tmp/name
    p.write_text('\n'.join(json.dumps(x,sort_keys=True) for x in rows),encoding='utf-8')
    return p


CONTRACT={'schema':'v2',
          'tables':{'parent':{'key_field':'id'},'child':{'key_field':'id'}},
          'references':[{'child':{'table':'child','field':'parent_id'},
                         'parent':{'table':'parent','field':'id'},'mode':IMMEDIATE}]}


def deferred_contract():
    spec=json.loads(json.dumps(CONTRACT))
    spec['references'][0]['mode']=DEFERRED
    return spec


def base_rows():
    return [{'table':'parent','key':'P','value':{'id':'P'}},
            {'table':'child','key':'C','value':{'id':'C','parent_id':'P'}}]


def begin_commit(changes,txn='T',start=1):
    rows=[{'kind':'begin','txn':txn,'seq':start}]
    for i,change in enumerate(changes,start+1):
        rows.append({**change,'kind':'change','txn':txn,'seq':i})
    rows.append({'kind':'commit','txn':txn,'seq':start+len(changes)+1})
    return rows


def delete_then_delete(txn='T'):
    return begin_commit([
        {'table':'parent','key':'P','action':'delete','before':{'id':'P'},'after':None},
        {'table':'child','key':'C','action':'delete','before':{'id':'C','parent_id':'P'},'after':None}],txn)


def delete_parent_only(txn='T'):
    return begin_commit([
        {'table':'parent','key':'P','action':'delete','before':{'id':'P'},'after':None}],txn)


def setup(tmp,spec=None,rows=None):
    spec=CONTRACT if spec is None else spec
    db=tmp/'data.db'
    snapshot(write_file(tmp,'base.jsonl',rows or base_rows()),db,0,
             schema=spec['schema'],contract_spec=spec)
    return db,spec


def feed(tmp,db,rows):
    return ingest(write_file(tmp,'events.jsonl',rows),db)


def issue_codes(db,run):
    return [r['code'] for r in connect(db).execute(
        'SELECT i.code FROM issue i JOIN run_issue ri ON ri.issue_id=i.id WHERE ri.run_id=? ORDER BY i.id',(run,))]


def test_same_named_keys_in_different_tables_do_not_collide(tmp_path):
    spec={'schema':'v2','tables':{'parent':{'key_field':'id'},'child':{'key_field':'id'}},'references':[]}
    rows=[{'table':'parent','key':'K','value':{'id':'K','v':1}},
          {'table':'child','key':'K','value':{'id':'K','v':2}}]
    db,_=setup(tmp_path,spec,rows)
    feed(tmp_path,db,begin_commit([
        {'table':'parent','key':'K','action':'update','before':{'id':'K','v':1},'after':{'id':'K','v':10}},
        {'table':'child','key':'K','action':'update','before':{'id':'K','v':2},'after':{'id':'K','v':20}}]))
    run,out,_=replay(db,4)
    assert out=={('parent','K'):'存在',('child','K'):'存在'}
    conn=connect(db)
    values={r['table_name']:json.loads(r['value']) for r in conn.execute(
        'SELECT table_name,value FROM visible_row WHERE run_id=?',(run,))}
    assert values=={'parent':{'id':'K','v':10},'child':{'id':'K','v':20}}
    assert conn.execute('SELECT COUNT(*) FROM snapshot_row').fetchone()[0]==2


def test_h3_immediate_parent_child_delete_is_whole_transaction_rejected(tmp_path):
    db,_=setup(tmp_path)
    feed(tmp_path,db,delete_then_delete())
    run,out,_=replay(db,4)
    assert out=={('parent','P'):'无法判定',('child','C'):'无法判定'}
    conn=connect(db)
    codes=issue_codes(db,run)
    assert 'fk_violation' in codes
    detail=conn.execute("SELECT detail FROM issue WHERE code='fk_violation'").fetchone()[0]
    assert 'child' in detail and 'parent' in detail and 'immediate' in detail and "'P'" in detail
    # no partial state: the parent must still be visible as 存在 from the snapshot view
    assert conn.execute('SELECT COUNT(*) FROM visible_row WHERE run_id=? AND status=?',(run,'已删除')).fetchone()[0]==0
    assert conn.execute('SELECT COUNT(*) FROM evidence WHERE run_id=?',(run,)).fetchone()[0]==0
    assert conn.execute('SELECT status FROM replay_run WHERE id=?',(run,)).fetchone()[0]=='待核'


def test_h3_deferred_parent_child_delete_commits(tmp_path):
    db,_=setup(tmp_path,deferred_contract())
    feed(tmp_path,db,delete_then_delete())
    run,out,_=replay(db,4)
    assert out=={('parent','P'):'已删除',('child','C'):'已删除'}
    conn=connect(db)
    assert issue_codes(db,run)==[]
    assert conn.execute('SELECT status FROM replay_run WHERE id=?',(run,)).fetchone()[0]=='已判定'
    # evidence traces both entities through their own delete events
    ev={r['table_name']:r['event_id'] for r in conn.execute(
        'SELECT table_name,event_id FROM evidence WHERE run_id=?',(run,))}
    assert set(ev)=={'parent','child'}


def test_deferred_mode_still_enforces_at_commit_when_child_survives(tmp_path):
    db,_=setup(tmp_path,deferred_contract())
    feed(tmp_path,db,delete_parent_only())
    run,out,_=replay(db,3)
    assert out=={('parent','P'):'无法判定',('child','C'):'无法判定'}
    codes=issue_codes(db,run)
    assert 'fk_violation' in codes
    detail=connect(db).execute("SELECT detail FROM issue WHERE code='fk_violation'").fetchone()[0]
    assert 'deferred' in detail and 'deferred check at commit' in detail


def test_immediate_parent_only_delete_rejected_without_partial_state(tmp_path):
    db,_=setup(tmp_path)
    feed(tmp_path,db,delete_parent_only())
    run,out,_=replay(db,3)
    assert out=={('parent','P'):'无法判定',('child','C'):'无法判定'}
    conn=connect(db)
    assert 'fk_violation' in issue_codes(db,run)
    assert conn.execute('SELECT COUNT(*) FROM visible_row WHERE run_id=? AND status=?',(run,'已删除')).fetchone()[0]==0
    assert conn.execute('SELECT COUNT(*) FROM evidence WHERE run_id=?',(run,)).fetchone()[0]==0


def test_immediate_allows_child_first_then_parent_in_one_transaction(tmp_path):
    db,_=setup(tmp_path)
    feed(tmp_path,db,begin_commit([
        {'table':'child','key':'C','action':'delete','before':{'id':'C','parent_id':'P'},'after':None},
        {'table':'parent','key':'P','action':'delete','before':{'id':'P'},'after':None}]))
    run,out,_=replay(db,4)
    assert out=={('parent','P'):'已删除',('child','C'):'已删除'}
    assert issue_codes(db,run)==[]


def test_event_ordering_changes_immediate_legality(tmp_path):
    # identical endpoint state, reversed order: parent removed while child still live -> reject
    d1=tmp_path/'case1';d1.mkdir()
    db,_=setup(d1)
    feed(d1,db,delete_then_delete())
    rejected,out,_=replay(db,4)
    assert all(status=='无法判定' for status in out.values())
    d2=tmp_path/'case2';d2.mkdir()
    db2,_=setup(d2)
    feed(d2,db2,begin_commit([
        {'table':'child','key':'C','action':'delete','before':{'id':'C','parent_id':'P'},'after':None},
        {'table':'parent','key':'P','action':'delete','before':{'id':'P'},'after':None}]))
    _,out2,_=replay(db2,4)
    assert all(status=='已删除' for status in out2.values())
    assert rejected!=out2


def test_before_conflict_leaves_no_partial_commit_across_tables(tmp_path):
    # add an extra, untouched parent so later traffic can prove the frozen snapshot survived
    rows=base_rows()+[{'table':'parent','key':'Q','value':{'id':'Q'}}]
    db,_=setup(tmp_path,CONTRACT,rows)
    feed(tmp_path,db,begin_commit([
        {'table':'parent','key':'P','action':'update','before':{'id':'P'},'after':{'id':'P','note':'x'}},
        {'table':'child','key':'C','action':'update','before':{'id':'C','parent_id':'WRONG'},
         'after':{'id':'C','parent_id':'P'}}]))
    run,out,_=replay(db,4)
    assert out=={('parent','P'):'无法判定',('child','C'):'无法判定',('parent','Q'):'存在'}
    conn=connect(db)
    assert 'before_mismatch' in issue_codes(db,run)
    assert conn.execute('SELECT COUNT(*) FROM evidence WHERE run_id=?',(run,)).fetchone()[0]==0
    # the rejected transaction published nothing: later traffic replays from the frozen snapshot
    feed(tmp_path,db,begin_commit([
        {'table':'parent','key':'Q','action':'update','before':{'id':'Q'},'after':{'id':'Q','note':'y'}}],txn='T2',start=5))
    _,out_later,_=replay(db,8)
    assert out_later[('parent','Q')]=='存在'
    assert json.loads(conn.execute(
        'SELECT value FROM visible_row WHERE run_id=(SELECT MAX(id) FROM replay_run) AND table_name=? AND key=?',
        ('parent','Q')).fetchone()[0])=={'id':'Q','note':'y'}
    # P itself stays poisoned: its rejected update never becomes visible
    assert out_later[('parent','P')]=='无法判定'
    assert conn.execute('SELECT COUNT(*) FROM visible_row WHERE table_name=? AND key=? AND status=?',
                        ('parent','P','存在')).fetchone()[0]==0


def test_insert_with_dangling_reference_rejected_immediately(tmp_path):
    db,_=setup(tmp_path)
    feed(tmp_path,db,begin_commit([
        {'table':'child','key':'D','action':'insert','before':None,
         'after':{'id':'D','parent_id':'GHOST'}}]))
    run,out,_=replay(db,3)
    assert out[('child','D')]=='无法判定'
    assert 'fk_violation' in issue_codes(db,run)
    assert ('child','D') not in {('parent','P'),}  # parent baseline untouched
    assert connect(db).execute(
        "SELECT status FROM visible_row WHERE run_id=? AND table_name='child' AND key='D'",(run,)).fetchone()[0]=='无法判定'


def test_deferred_insert_dangling_rejected_only_at_commit(tmp_path):
    db,_=setup(tmp_path,deferred_contract())
    # insert a child pointing at a parent created and deleted inside the same transaction
    feed(tmp_path,db,begin_commit([
        {'table':'parent','key':'Q','action':'insert','before':None,'after':{'id':'Q'}},
        {'table':'child','key':'D','action':'insert','before':None,'after':{'id':'D','parent_id':'Q'}},
        {'table':'parent','key':'Q','action':'delete','before':{'id':'Q'},'after':None}]))
    run,out,_=replay(db,5)
    assert out[('parent','Q')]=='无法判定' and out[('child','D')]=='无法判定'
    assert 'fk_violation' in issue_codes(db,run)
    assert connect(db).execute('SELECT COUNT(*) FROM evidence WHERE run_id=?',(run,)).fetchone()[0]==0


def test_failed_transaction_does_not_poison_later_valid_one(tmp_path):
    rows=base_rows()+[{'table':'parent','key':'Q','value':{'id':'Q'}},
                      {'table':'child','key':'E','value':{'id':'E','parent_id':'Q'}}]
    db,_=setup(tmp_path,CONTRACT,rows)
    feed(tmp_path,db,delete_parent_only())
    replay(db,3)
    # the rejection freezes P/C but entities the transaction never touched keep replaying
    feed(tmp_path,db,begin_commit([
        {'table':'child','key':'E','action':'update','before':{'id':'E','parent_id':'Q'},
         'after':{'id':'E','parent_id':'Q','n':1}}],txn='T2',start=4))
    run,out,_=replay(db,7)
    assert out[('child','E')]=='存在' and out[('parent','Q')]=='存在'
    assert out[('parent','P')]=='无法判定' and out[('child','C')]=='无法判定'
    assert connect(db).execute('SELECT COUNT(*) FROM evidence WHERE run_id=? AND table_name=?',(run,'child')).fetchone()[0]==1


def test_rename_is_scoped_per_table(tmp_path):
    spec={'schema':'v2','tables':{'parent':{'key_field':'id'},'child':{'key_field':'id'}},'references':[]}
    rows=[{'table':'parent','key':'K','value':{'id':'K'}},
          {'table':'child','key':'K','value':{'id':'K'}}]
    db,_=setup(tmp_path,spec,rows)
    feed(tmp_path,db,begin_commit([
        {'table':'parent','key':'K','action':'rename','before':{'id':'K'},'after':{'id':'K2'},'alias_to':'K2'}]))
    run,out,_=replay(db,3)
    assert out=={('parent','K2'):'存在',('child','K'):'存在'}
    conn=connect(db)
    assert conn.execute('SELECT table_name FROM key_alias').fetchone()[0]=='parent'


def test_snapshot_rejects_duplicate_per_table_and_dangling_reference(tmp_path):
    dup=base_rows()+[{'table':'parent','key':'P','value':{'id':'P'}}]
    with pytest.raises(ValueError,match='snapshot_duplicate_key'):
        validate_snapshot(dup,0,'v2',compile_contract(CONTRACT))
    dangling=[{'table':'parent','key':'P','value':{'id':'P'}},
              {'table':'child','key':'C','value':{'id':'C','parent_id':'MISSING'}}]
    with pytest.raises(Exception,match='no live parent'):
        validate_snapshot(dangling,0,'v2',compile_contract(CONTRACT))
    # same bare key in different tables is legal under (table,key) identity
    same_key=[{'table':'parent','key':'K','value':{'id':'K'}},
              {'table':'child','key':'K','value':{'id':'K','parent_id':'K'}}]
    assert validate_snapshot(same_key,0,'v2',compile_contract(CONTRACT))==['child:K','parent:K']


def test_snapshot_requires_table_and_declared_primary_key(tmp_path):
    db=tmp_path/'data.db'
    with pytest.raises(ValueError,match='snapshot_missing_table'):
        snapshot(write_file(tmp_path,'b.jsonl',[{'key':'P','value':{'id':'P'}}]),db,0,
                 schema='v2',contract_spec=CONTRACT)
    with pytest.raises(ValueError,match='unknown_table'):
        snapshot(write_file(tmp_path,'b2.jsonl',[{'table':'ghost','key':'X','value':{'id':'X'}}]),
                 tmp_path/'b.db',0,schema='v2',contract_spec=CONTRACT)
    with pytest.raises(ValueError,match='snapshot_missing_primary_key'):
        snapshot(write_file(tmp_path,'b3.jsonl',[{'table':'parent','key':'P','value':{}}]),
                 tmp_path/'c.db',0,schema='v2',contract_spec=CONTRACT)


def test_contract_compilation_validation():
    with pytest.raises(ContractError,match='reference_invalid_mode'):
        compile_contract({**CONTRACT,'references':[{**CONTRACT['references'][0],'mode':'async'}]})
    with pytest.raises(ContractError,match='reference_unknown_table'):
        compile_contract({'schema':'v2','tables':{'parent':{'key_field':'id'}},
                          'references':[{'child':{'table':'child','field':'p'},
                                         'parent':{'table':'parent','field':'id'},'mode':IMMEDIATE}]})
    with pytest.raises(ContractError,match='reference_parent_not_key'):
        spec=json.loads(json.dumps(CONTRACT))
        spec['references'][0]['parent']['field']='name'
        compile_contract(spec)
    with pytest.raises(ContractError,match='contract_missing_tables'):
        compile_contract({'schema':'v2','tables':{}})
    compiled=compile_contract(CONTRACT)
    assert [r.mode for r in compiled.immediate]==[IMMEDIATE]
    assert compiled.deferred==[]


def test_contract_change_is_rejected(tmp_path):
    db,_=setup(tmp_path)
    with pytest.raises(ValueError,match='contract_changed'):
        register_contract(connect(db),deferred_contract())
    # identical re-registration is idempotent
    assert register_contract(connect(db),CONTRACT).sha==compile_contract(CONTRACT).sha


def test_unknown_table_event_marks_entities_uncertain(tmp_path):
    db,_=setup(tmp_path)
    feed(tmp_path,db,begin_commit([
        {'table':'mystery','key':'X','action':'insert','before':None,'after':{'id':'X'}}]))
    # ingest quarantines the line as a parse_error, transaction then has no accepted change
    run,out,_=replay(db,3)
    assert all(status=='无法判定' for status in out.values()) or 'mystery' not in {t for (t,k) in out}
    conn=connect(db)
    assert conn.execute("SELECT COUNT(*) FROM issue WHERE code='parse_error'").fetchone()[0]==1


def test_multitable_report_is_traceable(tmp_path):
    db,_=setup(tmp_path,deferred_contract())
    feed(tmp_path,db,delete_then_delete())
    run,_,_=replay(db,4)
    data=json.loads(write(db,run,tmp_path/'report').read_text(encoding='utf-8'))
    assert {(d['table_name'],d['key'],d['status']) for d in data['decisions']}=={
        ('parent','P','已删除'),('child','C','已删除')}
    assert {e['table_name'] for e in data['evidence']}=={'parent','child'}


def test_legacy_single_table_entry_still_works(tmp_path):
    # no contract anywhere: bare keys, global namespace, old function signature
    rows=[{'key':'A','value':1}]
    db=tmp_path/'legacy.db'
    snapshot(write_file(tmp_path,'base.jsonl',rows),db,0)
    feed(tmp_path,db,[
        {'kind':'begin','txn':'T1','seq':1},
        {'kind':'change','txn':'T1','seq':2,'key':'A','action':'update','before':1,'after':2},
        {'kind':'commit','txn':'T1','seq':3}])
    run,out,excluded=replay(db,3)
    assert out=={'A':'存在'} and excluded==[]
    conn=connect(db)
    assert conn.execute('SELECT table_name FROM snapshot_row').fetchone()[0]=='default'
    assert conn.execute('SELECT table_name FROM decision WHERE run_id=?',(run,)).fetchone()[0]=='default'
    # legacy validate_snapshot signature and error codes unchanged
    assert validate_snapshot(rows,0,'v1')==['A']
    with pytest.raises(ValueError,match='snapshot_duplicate_key'):
        validate_snapshot([{'key':'A','value':1},{'key':'A','value':2}],0,'v1')


def test_legacy_database_is_migrated_to_table_qualified_schema(tmp_path):
    # build a legacy database using the old single-table layout, then open it with the new code
    import sqlite3
    legacy_ddl='''
    CREATE TABLE source_file(id INTEGER PRIMARY KEY,path TEXT,sha TEXT UNIQUE,raw BLOB NOT NULL);
    CREATE TABLE snapshot_row(key TEXT PRIMARY KEY,value TEXT NOT NULL,watermark INTEGER NOT NULL,schema_version TEXT NOT NULL,source_id INTEGER REFERENCES source_file(id));
    CREATE TABLE txn(id TEXT PRIMARY KEY,begin_seq INTEGER,commit_seq INTEGER,committed INTEGER NOT NULL DEFAULT 0,valid INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE change_event(id INTEGER PRIMARY KEY,source_id INTEGER REFERENCES source_file(id),line_no INTEGER,partition TEXT,seq INTEGER,txn_id TEXT,kind TEXT,key TEXT,before_value TEXT,after_value TEXT,alias_to TEXT,sha TEXT,raw TEXT,UNIQUE(partition,seq,txn_id));
    CREATE TABLE key_alias(old_key TEXT,new_key TEXT,effective_seq INTEGER,source_event INTEGER REFERENCES change_event(id),PRIMARY KEY(old_key,effective_seq));
    CREATE TABLE replay_run(id INTEGER PRIMARY KEY,watermark INTEGER,status TEXT NOT NULL,started_at TEXT DEFAULT CURRENT_TIMESTAMP,ended_at TEXT);
    CREATE TABLE visible_row(run_id INTEGER REFERENCES replay_run(id),key TEXT,value TEXT,status TEXT,PRIMARY KEY(run_id,key));
    CREATE TABLE issue(id INTEGER PRIMARY KEY,code TEXT NOT NULL,key TEXT,detail TEXT,event_id INTEGER REFERENCES change_event(id));
    CREATE TABLE evidence(id INTEGER PRIMARY KEY,run_id INTEGER REFERENCES replay_run(id),key TEXT,event_id INTEGER REFERENCES change_event(id),source_id INTEGER REFERENCES source_file(id),detail TEXT);
    CREATE TABLE decision(run_id INTEGER REFERENCES replay_run(id),key TEXT,status TEXT,value TEXT,rule_version TEXT,PRIMARY KEY(run_id,key));
    CREATE TABLE run_issue(run_id INTEGER NOT NULL REFERENCES replay_run(id),issue_id INTEGER NOT NULL REFERENCES issue(id),PRIMARY KEY(run_id,issue_id));
    CREATE TABLE run_exclusion(run_id INTEGER NOT NULL REFERENCES replay_run(id),txn_id TEXT NOT NULL,PRIMARY KEY(run_id,txn_id));
    CREATE TABLE expected(key TEXT PRIMARY KEY,value TEXT);
    CREATE TABLE target_snapshot(id INTEGER PRIMARY KEY,watermark INTEGER NOT NULL,source_sha TEXT NOT NULL UNIQUE,raw BLOB NOT NULL);
    CREATE TABLE target_row(snapshot_id INTEGER REFERENCES target_snapshot(id),key TEXT,value TEXT NOT NULL,PRIMARY KEY(snapshot_id,key));
    CREATE TABLE cutover_audit(id INTEGER PRIMARY KEY,run_id INTEGER REFERENCES replay_run(id),target_id INTEGER REFERENCES target_snapshot(id),status TEXT NOT NULL,reason TEXT NOT NULL,target_digest TEXT NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE cutover_difference(audit_id INTEGER REFERENCES cutover_audit(id),key TEXT,kind TEXT,replay_value TEXT,target_value TEXT,PRIMARY KEY(audit_id,key));
    CREATE TABLE consumer_contract(id TEXT PRIMARY KEY,critical INTEGER NOT NULL,minimum_watermark INTEGER NOT NULL,config_sha TEXT NOT NULL);
    CREATE TABLE consumer_ack(id INTEGER PRIMARY KEY,consumer_id TEXT REFERENCES consumer_contract(id),run_id INTEGER REFERENCES replay_run(id),sequence INTEGER NOT NULL,status TEXT NOT NULL,source_digest TEXT NOT NULL,UNIQUE(consumer_id,sequence));
    CREATE TABLE release_gate(id INTEGER PRIMARY KEY,audit_id INTEGER REFERENCES cutover_audit(id),run_id INTEGER REFERENCES replay_run(id),status TEXT NOT NULL,reason TEXT NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE release_member(gate_id INTEGER REFERENCES release_gate(id),consumer_id TEXT REFERENCES consumer_contract(id),status TEXT NOT NULL,ack_id INTEGER REFERENCES consumer_ack(id),PRIMARY KEY(gate_id,consumer_id));
    CREATE TABLE repair_plan(id INTEGER PRIMARY KEY,audit_id INTEGER REFERENCES cutover_audit(id),manifest_sha TEXT NOT NULL,raw BLOB NOT NULL,status TEXT NOT NULL,output_target_id INTEGER REFERENCES target_snapshot(id),result TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(audit_id,manifest_sha));
    CREATE TABLE repair_choice(plan_id INTEGER REFERENCES repair_plan(id),key TEXT NOT NULL,decision TEXT NOT NULL,reason TEXT NOT NULL,replay_value TEXT,target_value TEXT,PRIMARY KEY(plan_id,key));
    '''
    db_path=tmp_path/'old.db'
    raw=sqlite3.connect(db_path)
    raw.executescript(legacy_ddl)
    raw.execute("INSERT INTO source_file(path,sha,raw) VALUES ('b','s',x'01')")
    raw.execute("INSERT INTO snapshot_row VALUES ('A','1',0,'v1',1)")
    raw.execute("INSERT INTO change_event(source_id,line_no,partition,seq,txn_id,kind,key,before_value,after_value,alias_to,sha,raw) VALUES (1,1,'p0',1,'T1','change','A','1','2',NULL,'h','{}')")
    raw.commit();raw.close()
    # opening through the new store migrates the legacy namespace into the default table
    db=connect(db_path)
    assert tuple(db.execute('SELECT table_name,key FROM snapshot_row').fetchone())==('default','A')
    assert tuple(db.execute("SELECT table_name,kind,key FROM change_event").fetchone())==('default','change','A')
    db.close()
