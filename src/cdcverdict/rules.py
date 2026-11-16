import json
from .store import connect
from .timeline import transactions
from .identity import resolve,add_alias
from .contract import load_contract,DEFAULT_TABLE,ReferenceViolation
VERSION='1'
def _resolve(alias,identity,seq):
    table,key=identity
    return table,resolve(alias.setdefault(table,{}),key,seq)
def _add_alias(alias,table,old,new,seq,live):
    add_alias(alias.setdefault(table,{}),old,new,seq,{k[1]:v for k,v in live.items() if k[0]==table})
def replay(path,watermark,allow_recreate=True):
    db=connect(path);out={}
    contract=load_contract(db)
    multi=contract is not None
    with db:
        run=db.execute("INSERT INTO replay_run(watermark,status) VALUES (?,'待接收')",(watermark,)).lastrowid
        db.execute("UPDATE replay_run SET status='已校验' WHERE id=?",(run,))
        previous_issue_id=db.execute('SELECT COALESCE(MAX(id),0) FROM issue').fetchone()[0]
        events,bad,excluded=transactions(db,watermark)
        for txn_id in excluded:db.execute('INSERT INTO run_exclusion VALUES (?,?)',(run,txn_id))
        live={(r['table_name'],r['key']):r['value']
              for r in db.execute('SELECT * FROM snapshot_row')}
        deleted={};alias={};chains={k:[] for k in live}
        conflicts=db.execute("SELECT table_name,key FROM issue WHERE code='identity_conflict'").fetchall()
        bad.update(((r['table_name'] or DEFAULT_TABLE),r['key']) for r in conflicts if r['key'])
        for _,tx,group in events:
            touched={((e['table_name'] or DEFAULT_TABLE),e['key']) for e in group if e['key']}
            for e in group:
                payload=json.loads(e['raw'])
                target=payload.get('alias_to')
                if target:touched.add((e['table_name'],target))
            if touched & bad:
                bad.update(touched)
                db.execute('INSERT INTO issue(code,detail) VALUES (?,?)',('txn_blocked_by_prior_conflict',tx))
                continue
            staged_live=live.copy();staged_deleted=deleted.copy();staged_alias={t:a.copy() for t,a in alias.items()}
            staged_chains={key:items.copy() for key,items in chains.items()};new_aliases=[]
            failure=None;failure_entities=frozenset()
            for e in group:
                try:
                    table=e['table_name'];original=e['key']
                    identity=_resolve(staged_alias,(table,original),e['seq']) if original else None
                    if not identity:raise ValueError('missing_key')
                    if multi and table not in contract.table_names:
                        raise ValueError(f'unknown_table:{table}')
                    payload=json.loads(e['raw']);action=payload.get('action')
                    before=json.dumps(payload.get('before'),sort_keys=True);after=json.dumps(payload.get('after'),sort_keys=True)
                    if action=='rename':
                        new=payload['alias_to']
                        _add_alias(staged_alias,table,identity[1],new,e['seq'],staged_live)
                        old_identity=identity;identity=(table,new)
                        if old_identity in staged_live:staged_live[identity]=staged_live.pop(old_identity)
                        staged_chains[identity]=staged_chains.pop(old_identity,[])
                        new_aliases.append((table,old_identity[1],new,e['seq'],e['id']))
                    elif action=='update':
                        if identity not in staged_live or staged_live[identity]!=before:raise ValueError('before_mismatch')
                        staged_live[identity]=after
                    elif action=='delete':
                        if identity not in staged_live or staged_live[identity]!=before:raise ValueError('before_mismatch')
                        staged_live.pop(identity);staged_deleted[identity]=e['seq']
                    elif action=='insert':
                        if identity in staged_live:raise ValueError('duplicate_insert')
                        if identity in staged_deleted and (not allow_recreate or e['seq']<=staged_deleted[identity]):raise ValueError('stale_recreate')
                        staged_live[identity]=after;staged_deleted.pop(identity,None)
                    else:raise ValueError('unknown_action')
                    if multi:
                        key_field=contract.key_fields[table]
                        row=json.loads(after) if action in ('insert','update','rename') else None
                        if row is not None:
                            if not isinstance(row,dict) or key_field not in row or str(row[key_field])!=identity[1]:
                                raise ValueError(f'event_key_field_mismatch:{table}.{key_field}')
                        contract.check_references(staged_live,contract.immediate,event=e)
                    staged_chains.setdefault(identity,[]).append(e)
                except ReferenceViolation as exc:
                    failure=(e,exc.code,exc.detail);failure_entities=exc.entities;break
                except (ValueError,KeyError) as exc:
                    failure=(e,str(exc),f'{tx}: {str(exc)}');break
            if not failure and multi:
                try:
                    contract.check_references(staged_live,contract.deferred)
                except ReferenceViolation as exc:
                    anchor=group[-1]
                    failure=(anchor,exc.code,f'{tx}: deferred check at commit: {exc.detail}')
                    failure_entities=exc.entities
            if failure:
                bad.update(touched);bad.update(failure_entities)
                failed_event,code,detail=failure
                db.execute('INSERT INTO issue(code,table_name,key,detail,event_id) VALUES (?,?,?,?,?)',
                           (code,failed_event['table_name'] or None,failed_event['key'],
                            detail if multi or detail.startswith(tx) else f'{tx}: entire transaction rejected: {detail}',
                            failed_event['id']))
                continue
            live,deleted,alias,chains=staged_live,staged_deleted,staged_alias,staged_chains
            for table,old,new,seq,eid in new_aliases:
                db.execute('INSERT OR REPLACE INTO key_alias(table_name,old_key,new_key,effective_seq,source_event) VALUES (?,?,?,?,?)',
                           (table,old,new,seq,eid))
        all_identities=set(live)|set(deleted)|set(bad)
        for identity in sorted(all_identities):
            table,key=identity
            status='无法判定' if identity in bad else ('存在' if identity in live else '已删除')
            value=live.get(identity) if status=='存在' else None
            db.execute('INSERT INTO visible_row(run_id,table_name,key,value,status) VALUES (?,?,?,?,?)',
                       (run,table,key,value,status))
            db.execute('INSERT INTO decision(run_id,table_name,key,status,value,rule_version) VALUES (?,?,?,?,?,?)',
                       (run,table,key,status,value,VERSION))
            for e in chains.get(identity,[]):
                db.execute('INSERT INTO evidence(run_id,table_name,key,event_id,source_id,detail) VALUES (?,?,?,?,?,?)',
                           (run,table,key,e['id'],e['source_id'],'replay'))
            out[key if not multi else identity]=status
        db.execute("UPDATE replay_run SET status=?,ended_at=CURRENT_TIMESTAMP WHERE id=?",('待核' if bad else '已判定',run))
        db.execute("INSERT INTO run_issue(run_id,issue_id) SELECT ?,id FROM issue WHERE id>? OR code IN ('identity_conflict','parse_error')",(run,previous_issue_id))
    db.close();return run,out,excluded
