import json
from pathlib import Path
from .store import connect
def write(path,run,out):
    db=connect(path)
    data={'run':dict(db.execute('SELECT * FROM replay_run WHERE id=?',(run,)).fetchone()),
      'decisions':[dict(x) for x in db.execute('SELECT * FROM decision WHERE run_id=? ORDER BY key',(run,))],
      'evidence':[dict(x) for x in db.execute('SELECT * FROM evidence WHERE run_id=? ORDER BY id',(run,))],
      'issues':[dict(x) for x in db.execute('SELECT i.* FROM issue i JOIN run_issue ri ON ri.issue_id=i.id WHERE ri.run_id=? ORDER BY i.id',(run,))],
      'sources':[dict(x) for x in db.execute('SELECT id,path,sha FROM source_file ORDER BY id')],
      'excluded_transactions':[r['txn_id'] for r in db.execute('SELECT txn_id FROM run_exclusion WHERE run_id=? ORDER BY txn_id',(run,))],
      'schema_versions':[dict(x) for x in db.execute('SELECT key,baseline_schema,event_schema,target_schema,before_status FROM key_schema WHERE run_id=? ORDER BY key',(run,))],
      'field_lineage':[dict(x) for x in db.execute('SELECT key,field,source_field,chain FROM field_trace WHERE run_id=? ORDER BY key,field',(run,))]}
    expected={r['key']:r['value'] for r in db.execute('SELECT * FROM expected')}
    actual={r['key']:r['value'] for r in data['decisions'] if r['status']=='存在'}
    data['expected_differences']=[k for k in sorted(set(expected)|set(actual)) if expected.get(k)!=actual.get(k)] if expected else []
    out=Path(out);out.parent.mkdir(parents=True,exist_ok=True)
    try:
        out.with_suffix('.json').write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
        out.with_suffix('.md').write_text('# 回放结论\n\n'+'\n'.join(f"- {x['key']}: {x['status']}" for x in data['decisions']),encoding='utf-8')
    except OSError:
        with db:db.execute("UPDATE replay_run SET status='待核' WHERE id=?",(run,))
        raise
    with db:db.execute("UPDATE replay_run SET status='已归档' WHERE id=? AND status='已判定'",(run,))
    db.close();return out.with_suffix('.json')
