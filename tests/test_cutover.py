import json
import pytest
from cdcverdict.__main__ import demo
from cdcverdict.cutover import import_target,audit_target,configure_consumers,acknowledge,release_decision
from cdcverdict.store import connect


def seeded(tmp_path):
    demo(tmp_path)
    db=tmp_path/'data.db'
    run=connect(db).execute('SELECT id FROM replay_run ORDER BY id DESC LIMIT 1').fetchone()[0]
    return db,run


def target(tmp_path,db,value,watermark=3):
    file=tmp_path/f'target-{value}-{watermark}.jsonl'
    file.write_text(json.dumps({'key':'A','value':{'n':value}}),encoding='utf-8')
    return import_target(db,file,watermark)


def test_cutover_distinguishes_lag_divergence_and_ready(tmp_path):
    db,run=seeded(tmp_path)
    assert audit_target(db,run,target(tmp_path,db,2))[1]=='ready'
    assert audit_target(db,run,target(tmp_path,db,1,watermark=1))[1]=='lagging'
    assert audit_target(db,run,target(tmp_path,db,9))[1]=='divergent'
    con=connect(db)
    assert con.execute("SELECT kind FROM cutover_difference ORDER BY audit_id DESC LIMIT 1").fetchone()[0]=='value_mismatch'


def test_release_gate_requires_target_and_critical_ack(tmp_path):
    db,run=seeded(tmp_path)
    tid=target(tmp_path,db,2)
    aid,status,_=audit_target(db,run,tid)
    assert status=='ready'
    config=tmp_path/'consumers.json';config.write_text(json.dumps([
        {'id':'billing','critical':True,'minimum_watermark':3},
        {'id':'preview','critical':False,'minimum_watermark':0}]),encoding='utf-8')
    configure_consumers(db,config)
    assert release_decision(db,aid)[1]=='blocked'
    sha=connect(db).execute('SELECT source_sha FROM target_snapshot WHERE id=?',(tid,)).fetchone()[0]
    assert acknowledge(db,'billing',run,1,'accepted','wrong')
    assert release_decision(db,aid)[1]=='blocked'
    with pytest.raises(ValueError,match='ack_identity_conflict'):
        acknowledge(db,'billing',run,1,'accepted',sha)
    acknowledge(db,'billing',run,2,'accepted',sha)
    assert release_decision(db,aid)[1]=='ready'
    con=connect(db)
    with con:con.execute("UPDATE target_row SET value=? WHERE snapshot_id=? AND key='A'",(json.dumps({'n':99}),tid))
    assert release_decision(db,aid)[1]=='blocked'


def test_conflicting_target_contract_is_not_reinterpreted(tmp_path):
    db,run=seeded(tmp_path)
    file=tmp_path/'same.jsonl';file.write_text(json.dumps({'key':'A','value':{'n':2}}),encoding='utf-8')
    import_target(db,file,3)
    with pytest.raises(ValueError,match='target_snapshot_contract_changed'):
        import_target(db,file,4)
