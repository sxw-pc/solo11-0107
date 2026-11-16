import json
from .store import connect
from .timeline import transactions
from .identity import resolve,add_alias
VERSION='1'
def replay(path,watermark,allow_recreate=True):
    db=connect(path);out={}
    with db:
        run=db.execute("INSERT INTO replay_run(watermark,status) VALUES (?,'待接收')",(watermark,)).lastrowid
        db.execute("UPDATE replay_run SET status='已校验' WHERE id=?",(run,))
        events,bad,excluded=transactions(db,watermark)
        live={r['key']:r['value'] for r in db.execute('SELECT * FROM snapshot_row')}
        deleted={};alias={};chains={k:[] for k in live}
        conflicts=[r for r in db.execute("SELECT key FROM issue WHERE code='identity_conflict'")]
        bad.update(r['key'] for r in conflicts if r['key'])
        for e in events:
            original=e['key'];key=resolve(alias,original,e['seq']) if original else None
            if not key:continue
            if key in bad:continue
            payload=json.loads(e['raw']);action=payload.get('action')
            before=json.dumps(payload.get('before'),sort_keys=True);after=json.dumps(payload.get('after'),sort_keys=True)
            try:
                if action=='rename':
                    new=payload['alias_to'];add_alias(alias,key,new,e['seq'],live)
                    if key in live:live[new]=live.pop(key)
                    chains[new]=chains.pop(key,[]);db.execute('INSERT OR REPLACE INTO key_alias VALUES (?,?,?,?)',(key,new,e['seq'],e['id']));key=new
                elif action=='update':
                    if key not in live or live[key]!=before:raise ValueError('before_mismatch')
                    live[key]=after
                elif action=='delete':
                    if key not in live or live[key]!=before:raise ValueError('before_mismatch')
                    live.pop(key);deleted[key]=e['seq']
                elif action=='insert':
                    if key in live:raise ValueError('duplicate_insert')
                    if key in deleted and (not allow_recreate or e['seq']<=deleted[key]):raise ValueError('stale_recreate')
                    live[key]=after;deleted.pop(key,None)
                else:raise ValueError('unknown_action')
                chains.setdefault(key,[]).append(e)
            except (ValueError,KeyError) as exc:
                bad.add(key);db.execute('INSERT INTO issue(code,key,detail,event_id) VALUES (?,?,?,?)',(str(exc),key,str(exc),e['id']))
        all_keys=set(live)|set(deleted)|set(bad)
        for key in sorted(all_keys):
            status='无法判定' if key in bad else ('存在' if key in live else '已删除')
            value=live.get(key) if status=='存在' else None
            db.execute('INSERT INTO visible_row VALUES (?,?,?,?)',(run,key,value,status))
            db.execute('INSERT INTO decision VALUES (?,?,?,?,?)',(run,key,status,value,VERSION))
            for e in chains.get(key,[]):db.execute('INSERT INTO evidence(run_id,key,event_id,source_id,detail) VALUES (?,?,?,?,?)',(run,key,e['id'],e['source_id'],'replay'))
            out[key]=status
        db.execute("UPDATE replay_run SET status=?,ended_at=CURRENT_TIMESTAMP WHERE id=?",('待核' if bad else '已判定',run))
    db.close();return run,out,excluded
