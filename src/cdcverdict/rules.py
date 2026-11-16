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
        previous_issue_id=db.execute('SELECT COALESCE(MAX(id),0) FROM issue').fetchone()[0]
        events,bad,excluded=transactions(db,watermark)
        for txn_id in excluded:db.execute('INSERT INTO run_exclusion VALUES (?,?)',(run,txn_id))
        live={r['key']:r['value'] for r in db.execute('SELECT * FROM snapshot_row')}
        deleted={};alias={};chains={k:[] for k in live}
        conflicts=[r for r in db.execute("SELECT key FROM issue WHERE code='identity_conflict'")]
        bad.update(r['key'] for r in conflicts if r['key'])
        for _,tx,group in events:
            touched={e['key'] for e in group if e['key']}
            touched.update(json.loads(e['raw']).get('alias_to') for e in group if json.loads(e['raw']).get('alias_to'))
            if touched & bad:
                bad.update(touched)
                db.execute('INSERT INTO issue(code,detail) VALUES (?,?)',('txn_blocked_by_prior_conflict',tx))
                continue
            staged_live=live.copy();staged_deleted=deleted.copy();staged_alias=alias.copy()
            staged_chains={key:items.copy() for key,items in chains.items()};new_aliases=[]
            failure=None
            for e in group:
                try:
                    original=e['key'];key=resolve(staged_alias,original,e['seq']) if original else None
                    if not key:raise ValueError('missing_key')
                    payload=json.loads(e['raw']);action=payload.get('action')
                    before=json.dumps(payload.get('before'),sort_keys=True);after=json.dumps(payload.get('after'),sort_keys=True)
                    if action=='rename':
                        new=payload['alias_to'];add_alias(staged_alias,key,new,e['seq'],staged_live)
                        if key in staged_live:staged_live[new]=staged_live.pop(key)
                        staged_chains[new]=staged_chains.pop(key,[])
                        new_aliases.append((key,new,e['seq'],e['id']));key=new
                    elif action=='update':
                        if key not in staged_live or staged_live[key]!=before:raise ValueError('before_mismatch')
                        staged_live[key]=after
                    elif action=='delete':
                        if key not in staged_live or staged_live[key]!=before:raise ValueError('before_mismatch')
                        staged_live.pop(key);staged_deleted[key]=e['seq']
                    elif action=='insert':
                        if key in staged_live:raise ValueError('duplicate_insert')
                        if key in staged_deleted and (not allow_recreate or e['seq']<=staged_deleted[key]):raise ValueError('stale_recreate')
                        staged_live[key]=after;staged_deleted.pop(key,None)
                    else:raise ValueError('unknown_action')
                    staged_chains.setdefault(key,[]).append(e)
                except (ValueError,KeyError) as exc:
                    failure=(e,str(exc));break
            if failure:
                bad.update(touched)
                failed_event,code=failure
                db.execute('INSERT INTO issue(code,key,detail,event_id) VALUES (?,?,?,?)',(code,failed_event['key'],f'{tx}: entire transaction rejected',failed_event['id']))
                continue
            live,deleted,alias,chains=staged_live,staged_deleted,staged_alias,staged_chains
            for old,new,seq,eid in new_aliases:
                db.execute('INSERT OR REPLACE INTO key_alias VALUES (?,?,?,?)',(old,new,seq,eid))
        all_keys=set(live)|set(deleted)|set(bad)
        for key in sorted(all_keys):
            status='无法判定' if key in bad else ('存在' if key in live else '已删除')
            value=live.get(key) if status=='存在' else None
            db.execute('INSERT INTO visible_row VALUES (?,?,?,?)',(run,key,value,status))
            db.execute('INSERT INTO decision VALUES (?,?,?,?,?)',(run,key,status,value,VERSION))
            for e in chains.get(key,[]):db.execute('INSERT INTO evidence(run_id,key,event_id,source_id,detail) VALUES (?,?,?,?,?)',(run,key,e['id'],e['source_id'],'replay'))
            out[key]=status
        db.execute("UPDATE replay_run SET status=?,ended_at=CURRENT_TIMESTAMP WHERE id=?",('待核' if bad else '已判定',run))
        db.execute("INSERT INTO run_issue(run_id,issue_id) SELECT ?,id FROM issue WHERE id>? OR code IN ('identity_conflict','parse_error')",(run,previous_issue_id))
    db.close();return run,out,excluded
