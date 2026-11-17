import json
import pytest
from cdcverdict.ingest import snapshot,ingest
from cdcverdict.rules import replay
from cdcverdict.identity import add_alias
from cdcverdict.report import write
from cdcverdict.store import connect

def make(tmp,rows,events):
    b=tmp/'base.jsonl';b.write_text('\n'.join(json.dumps(x) for x in rows),encoding='utf-8')
    e=tmp/'events.jsonl';e.write_text('\n'.join(json.dumps(x) for x in events),encoding='utf-8')
    db=tmp/'x.db';snapshot(b,db,0);ingest(e,db);return db
def transaction(action,before=1,after=2):
    return [{'kind':'begin','txn':'T','seq':1},
      {'kind':'change','txn':'T','seq':2,'key':'K','action':action,'before':before,'after':after},
      {'kind':'commit','txn':'T','seq':3}]
def test_snapshot_keeps_original_bytes_and_version(tmp_path):
    db=make(tmp_path,[{'key':'K','value':1}],[])
    c=connect(db)
    assert c.execute('SELECT schema_version FROM snapshot_row').fetchone()[0]=='v1'
    assert c.execute('SELECT raw FROM source_file ORDER BY id LIMIT 1').fetchone()[0]==(tmp_path/'base.jsonl').read_bytes()
def test_transaction_out_of_order_lines_still_commit(tmp_path):
    db=make(tmp_path,[{'key':'K','value':1}],list(reversed(transaction('update'))))
    assert replay(db,3)[1]['K']=='存在'
def test_same_event_reimport_is_idempotent(tmp_path):
    db=make(tmp_path,[{'key':'K','value':1}],transaction('update'))
    assert ingest(tmp_path/'events.jsonl',db)==0
    assert connect(db).execute('SELECT COUNT(*) FROM change_event').fetchone()[0]==3
def test_alias_cycle_rejected(tmp_path):
    alias={'A':('B',1)}
    with pytest.raises(ValueError,match='alias_cycle'):add_alias(alias,'B','A',2,{})
def test_delete_tombstone_has_evidence(tmp_path):
    db=make(tmp_path,[{'key':'K','value':1}],transaction('delete',1,None))
    run,out,_=replay(db,3);assert out['K']=='已删除'
    report=json.loads(write(db,run,tmp_path/'r').read_text(encoding='utf-8'))
    assert report['evidence'][0]['event_id'] and report['decisions'][0]['rule_version']=='1'
def test_watermark_before_commit_excludes_transaction(tmp_path):
    db=make(tmp_path,[{'key':'K','value':1}],transaction('delete',1,None))
    run,out,excluded=replay(db,2)
    assert out['K']=='存在' and excluded==['T']
