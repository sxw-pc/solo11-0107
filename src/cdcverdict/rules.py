import json
from .store import connect
from .timeline import transactions
from .identity import resolve,add_alias
from .contract import load_graph,load_fields,find_path,project_row,validate_migration,ProjectionError
VERSION='1'
STRATEGY_VERSION='2'
def replay(path,watermark,allow_recreate=True,strategy=None,target_schema=None):
    if strategy not in (None,'hard','compatible'):raise ValueError('unknown_strategy')
    if strategy is None:
        if target_schema is not None:raise ValueError('strategy_required')
        return _legacy(path,watermark,allow_recreate)
    return _strategic(path,watermark,allow_recreate,strategy,target_schema)
def _legacy(path,watermark,allow_recreate=True):
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
def _strategic(path,watermark,allow_recreate,strategy,target_schema):
    """按水位生效的 schema 版本图回放：hard 要求基线/事件/目标版本完全一致，
    否则阻止整次切换；compatible 只对声明无损映射的字段放行，无法投影的 key
    判无法判定，无关 key 保持确定。决策标记规则版本 2。"""
    db=connect(path);out={}
    with db:
        run=db.execute("INSERT INTO replay_run(watermark,status) VALUES (?,'待接收')",(watermark,)).lastrowid
        db.execute("UPDATE replay_run SET status='已校验' WHERE id=?",(run,))
        previous_issue_id=db.execute('SELECT COALESCE(MAX(id),0) FROM issue').fetchone()[0]
        events,bad,excluded=transactions(db,watermark)
        for txn_id in excluded:db.execute('INSERT INTO run_exclusion VALUES (?,?)',(run,txn_id))
        snap=list(db.execute('SELECT * FROM snapshot_row'))
        baseline_schema=snap[0]['schema_version'] if snap else None
        target=target_schema if target_schema is not None else baseline_schema
        graph=load_graph(db,watermark);fields=load_fields(db)
        live={r['key']:(r['value'],baseline_schema) for r in snap}
        prov={}
        for r in snap:
            value=json.loads(r['value'])
            prov[r['key']]={f:(f,f'{baseline_schema}.{f}') for f in value} if isinstance(value,dict) else {}
        before_state={}
        event_schemas={json.loads(e['raw']).get('schema') or baseline_schema
          for e in (dict(x) for x in db.execute("SELECT raw FROM change_event WHERE kind='change' AND seq<=?",(watermark,)))}
        hard_blocked=strategy=='hard' and len({baseline_schema,target}|event_schemas)>1
        if hard_blocked:
            keys=set(live)|set(bad)|{e['key'] for _,_,group in events for e in group if e['key']}
            versions=','.join(sorted(str(v) for v in {baseline_schema,target}|event_schemas))
            db.execute('INSERT INTO issue(code,detail) VALUES (?,?)',('schema_version_blocked',f'baseline/events/target differ: {versions}'))
            for key in sorted(keys):
                db.execute('INSERT INTO visible_row VALUES (?,?,?,?)',(run,key,None,'无法判定'))
                db.execute('INSERT INTO decision VALUES (?,?,?,?,?)',(run,key,'无法判定',None,STRATEGY_VERSION))
                db.execute('INSERT INTO key_schema VALUES (?,?,?,?,?,?)',(run,key,baseline_schema,None,target,'unchecked'))
                out[key]='无法判定'
            db.execute("UPDATE replay_run SET status='待核',ended_at=CURRENT_TIMESTAMP WHERE id=?",(run,))
            db.execute("INSERT INTO run_issue(run_id,issue_id) SELECT ?,id FROM issue WHERE id>? OR code IN ('identity_conflict','parse_error')",(run,previous_issue_id))
        else:
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
                staged_prov={k:v.copy() for k,v in prov.items()}
                staged_chains={key:items.copy() for key,items in chains.items()};new_aliases=[]
                txn_before={};failure=None
                for e in group:
                    try:
                        original=e['key'];key=resolve(staged_alias,original,e['seq']) if original else None
                        if not key:raise ValueError('missing_key')
                        payload=json.loads(e['raw']);action=payload.get('action')
                        before=json.dumps(payload.get('before'),sort_keys=True);after=json.dumps(payload.get('after'),sort_keys=True)
                        if action=='rename':
                            new=payload['alias_to'];add_alias(staged_alias,key,new,e['seq'],{k:v[0] for k,v in staged_live.items()})
                            if key in staged_live:staged_live[new]=staged_live.pop(key)
                            staged_prov[new]=staged_prov.pop(key,{})
                            staged_chains[new]=staged_chains.pop(key,[])
                            new_aliases.append((key,new,e['seq'],e['id']));key=new
                        elif action=='update':
                            if key not in staged_live or staged_live[key][0]!=before:
                                txn_before[key]='mismatch';raise ValueError('before_mismatch')
                            txn_before[key]='matched'
                            current=staged_live[key][1];declared=payload.get('schema') or current
                            if declared!=current:
                                path_edges=find_path(graph,current,declared)
                                if path_edges is None:raise ProjectionError('*',f'schema_no_path:{current}->{declared}')
                                staged_prov[key]=validate_migration(json.loads(before),json.loads(after),path_edges,fields)
                            else:
                                after_value=json.loads(after)
                                staged_prov[key]={f:(f,f'{declared}.{f}') for f in after_value} if isinstance(after_value,dict) else {}
                            staged_live[key]=(after,declared)
                        elif action=='delete':
                            if key not in staged_live or staged_live[key][0]!=before:
                                txn_before[key]='mismatch';raise ValueError('before_mismatch')
                            txn_before[key]='matched'
                            staged_live.pop(key);staged_deleted[key]=e['seq'];staged_prov.pop(key,None)
                        elif action=='insert':
                            if key in staged_live:raise ValueError('duplicate_insert')
                            if key in staged_deleted and (not allow_recreate or e['seq']<=staged_deleted[key]):raise ValueError('stale_recreate')
                            declared=payload.get('schema') or baseline_schema
                            after_value=json.loads(after)
                            if declared in fields and isinstance(after_value,dict):
                                unknown=sorted(f for f in after_value if f not in fields[declared])
                                if unknown:raise ProjectionError(unknown[0],'schema_unknown_field')
                            staged_live[key]=(after,declared);staged_deleted.pop(key,None)
                            staged_prov[key]={f:(f,f'{declared}.{f}') for f in after_value} if isinstance(after_value,dict) else {}
                        else:raise ValueError('unknown_action')
                        staged_chains.setdefault(key,[]).append(e)
                    except ProjectionError as exc:
                        failure=(e,'schema_projection',str(exc));break
                    except (ValueError,KeyError) as exc:
                        failure=(e,str(exc),None);break
                before_state.update(txn_before)
                if failure:
                    bad.update(touched)
                    failed_event,code,extra=failure
                    detail=f'{tx}: {extra}; entire transaction rejected' if extra else f'{tx}: entire transaction rejected'
                    db.execute('INSERT INTO issue(code,key,detail,event_id) VALUES (?,?,?,?)',(code,failed_event['key'],detail,failed_event['id']))
                    continue
                live,deleted,alias,chains,prov=staged_live,staged_deleted,staged_alias,staged_chains,staged_prov
                for old,new,seq,eid in new_aliases:
                    db.execute('INSERT OR REPLACE INTO key_alias VALUES (?,?,?,?)',(old,new,seq,eid))
            all_keys=set(live)|set(deleted)|set(bad)
            for key in sorted(all_keys):
                final_prov=prov.get(key,{})
                if key in bad:
                    status='无法判定';value=None
                elif key in deleted:
                    status='已删除';value=None
                else:
                    raw,current=live[key]
                    if current==target:
                        status='存在';value=raw
                    else:
                        path_edges=find_path(graph,current,target)
                        if path_edges is None:
                            db.execute('INSERT INTO issue(code,key,detail) VALUES (?,?,?)',('schema_unprojectable',key,f'schema_no_path:{current}->{target}'))
                            bad.add(key);status='无法判定';value=None
                        else:
                            try:
                                projected,final_prov=project_row(json.loads(raw),path_edges,prov.get(key))
                                status='存在';value=json.dumps(projected,sort_keys=True)
                            except ProjectionError as exc:
                                db.execute('INSERT INTO issue(code,key,detail) VALUES (?,?,?)',('schema_unprojectable',key,str(exc)))
                                bad.add(key);status='无法判定';value=None
                db.execute('INSERT INTO visible_row VALUES (?,?,?,?)',(run,key,value,status))
                db.execute('INSERT INTO decision VALUES (?,?,?,?,?)',(run,key,status,value,STRATEGY_VERSION))
                db.execute('INSERT INTO key_schema VALUES (?,?,?,?,?,?)',(run,key,baseline_schema,live[key][1] if key in live else None,target,before_state.get(key,'unchecked')))
                if status=='存在':
                    for field,(source,chain) in sorted(final_prov.items()):
                        db.execute('INSERT INTO field_trace(run_id,key,field,source_field,chain) VALUES (?,?,?,?,?)',(run,key,field,source,chain))
                for e in chains.get(key,[]):db.execute('INSERT INTO evidence(run_id,key,event_id,source_id,detail) VALUES (?,?,?,?,?)',(run,key,e['id'],e['source_id'],'replay'))
                out[key]=status
            db.execute("UPDATE replay_run SET status=?,ended_at=CURRENT_TIMESTAMP WHERE id=?",('待核' if bad else '已判定',run))
            db.execute("INSERT INTO run_issue(run_id,issue_id) SELECT ?,id FROM issue WHERE id>? OR code IN ('identity_conflict','parse_error')",(run,previous_issue_id))
    db.close();return run,out,excluded
