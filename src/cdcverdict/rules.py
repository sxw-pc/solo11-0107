import hashlib
import json
from .store import connect
from .timeline import transactions
from .identity import resolve,add_alias
VERSION='1'
RETENTION_MODES=('full_history','retention_line')
EXPIRED='历史已过期'
def _canonical(obj):return json.dumps(obj,ensure_ascii=False,sort_keys=True,separators=(',',':'))
def _digest(text):return hashlib.sha256(text.encode('utf-8')).hexdigest()
def _contract(db):return db.execute('SELECT * FROM retention_contract WHERE id=1').fetchone()
def _latest_compaction(db):return db.execute('SELECT * FROM retention_compaction ORDER BY floor DESC,id DESC LIMIT 1').fetchone()
def configure_retention(path,mode):
    if mode not in RETENTION_MODES:raise ValueError('invalid_retention_mode')
    db=connect(path)
    with db:
        old=_contract(db)
        if old:
            if old['mode']!=mode:raise ValueError('retention_contract_changed')
        else:db.execute('INSERT INTO retention_contract(id,mode) VALUES (1,?)',(mode,))
    db.close();return mode
def _seed(db,comp):
    if not comp:
        live={r['key']:r['value'] for r in db.execute('SELECT * FROM snapshot_row')}
        return live,{}, {}, set()
    summary=json.loads(comp['summary'])
    live={k:v['value'] for k,v in summary['live'].items()}
    deleted={k:(t['deleted_seq'],t['before_digest']) for k,t in summary['tombstones'].items()}
    alias={}
    for r in db.execute('SELECT old_key,new_key,effective_seq FROM key_alias WHERE effective_seq<=? ORDER BY effective_seq',(comp['floor'],)):
        alias[r['old_key']]=(r['new_key'],r['effective_seq'])
    return live,deleted,alias,set(summary['blocked'])
def _fold(groups,state,bad,allow_recreate):
    live,deleted,alias=state
    chains={k:[] for k in live};new_aliases=[];rejected=[]
    for _,tx,group in groups:
        touched={e['key'] for e in group if e['key']}
        touched.update(json.loads(e['raw']).get('alias_to') for e in group if json.loads(e['raw']).get('alias_to'))
        if touched & bad:
            bad.update(touched);rejected.append((tx,None,'txn_blocked_by_prior_conflict'));continue
        staged_live=live.copy();staged_deleted=deleted.copy();staged_alias=alias.copy()
        staged_chains={key:items.copy() for key,items in chains.items()};aliases_added=[]
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
                    aliases_added.append((key,new,e['seq'],e['id']));key=new
                elif action=='update':
                    if key not in staged_live or staged_live[key]!=before:raise ValueError('before_mismatch')
                    staged_live[key]=after
                elif action=='delete':
                    if key not in staged_live or staged_live[key]!=before:raise ValueError('before_mismatch')
                    staged_live.pop(key);staged_deleted[key]=(e['seq'],_digest(before))
                elif action=='insert':
                    if key in staged_live:raise ValueError('duplicate_insert')
                    if key in staged_deleted and (not allow_recreate or e['seq']<=staged_deleted[key][0]):raise ValueError('stale_recreate')
                    staged_live[key]=after;staged_deleted.pop(key,None)
                else:raise ValueError('unknown_action')
                staged_chains.setdefault(key,[]).append(e)
            except (ValueError,KeyError) as exc:
                failure=(e,str(exc));break
        if failure:
            bad.update(touched);rejected.append((tx,)+failure);continue
        live,deleted,alias,chains=staged_live,staged_deleted,staged_alias,staged_chains
        new_aliases.extend(aliases_added)
    return (live,deleted,alias),chains,new_aliases,rejected
def _project(db,watermark,allow_recreate=True):
    comp=_latest_compaction(db)
    floor=comp['floor'] if comp else None
    live,deleted,alias,seeded=_seed(db,comp)
    events,bad,excluded=transactions(db,watermark)
    bad=set(bad)|seeded
    bad.update(r['key'] for r in db.execute("SELECT key FROM issue WHERE code='identity_conflict'") if r['key'])
    if floor is not None:events=[g for g in events if g[0]>floor]
    state,chains,new_aliases,rejected=_fold(events,(live,deleted,alias),bad,allow_recreate)
    return floor,state,chains,new_aliases,rejected,bad,excluded
def _expire_run(db,run,watermark,comp):
    summary=json.loads(comp['summary'])
    keys={r['key'] for r in db.execute('SELECT key FROM snapshot_row')}
    keys|=set(summary['live'])|set(summary['tombstones'])
    keys|={r['old_key'] for r in db.execute('SELECT old_key FROM key_alias')}
    keys|={r['new_key'] for r in db.execute('SELECT new_key FROM key_alias')}
    for key in sorted(keys):
        db.execute('INSERT INTO visible_row VALUES (?,?,?,?)',(run,key,None,EXPIRED))
        db.execute('INSERT INTO decision VALUES (?,?,?,?,?)',(run,key,EXPIRED,None,VERSION))
    issue=db.execute('INSERT INTO issue(code,detail) VALUES (?,?)',
      ('history_expired',f'watermark {watermark} below retention floor {comp["floor"]}')).lastrowid
    db.execute('INSERT INTO run_issue VALUES (?,?)',(run,issue))
    db.execute("UPDATE replay_run SET status=?,ended_at=CURRENT_TIMESTAMP WHERE id=?",(EXPIRED,run))
    return sorted(keys)
def replay(path,watermark,allow_recreate=True):
    db=connect(path);out={};excluded=[]
    with db:
        run=db.execute("INSERT INTO replay_run(watermark,status) VALUES (?,'待接收')",(watermark,)).lastrowid
        db.execute("UPDATE replay_run SET status='已校验' WHERE id=?",(run,))
        contract=_contract(db);comp=_latest_compaction(db)
        if contract and contract['mode']=='retention_line' and comp and watermark<comp['floor']:
            for key in _expire_run(db,run,watermark,comp):out[key]=EXPIRED
        else:
            previous_issue_id=db.execute('SELECT COALESCE(MAX(id),0) FROM issue').fetchone()[0]
            _,state,chains,new_aliases,rejected,bad,excluded=_project(db,watermark,allow_recreate)
            live,deleted,_=state
            for txn_id in excluded:db.execute('INSERT INTO run_exclusion VALUES (?,?)',(run,txn_id))
            for tx,event,code in rejected:
                if event is None:db.execute('INSERT INTO issue(code,detail) VALUES (?,?)',(code,tx))
                else:db.execute('INSERT INTO issue(code,key,detail,event_id) VALUES (?,?,?,?)',(code,event['key'],f'{tx}: entire transaction rejected',event['id']))
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
def compact(path,floor):
    if not isinstance(floor,int) or isinstance(floor,bool) or floor<0:raise ValueError('invalid_retention_floor')
    db=connect(path)
    with db:
        contract=_contract(db)
        if not contract:raise ValueError('retention_contract_missing')
        if contract['mode']!='retention_line':raise ValueError('retention_contract_forbids_compaction')
        previous=_latest_compaction(db)
        if previous and floor<=previous['floor']:raise ValueError('retention_floor_regression')
        _,state,_,_,_,bad,_=_project(db,floor)
        live,deleted,alias=state
        for old,(new,seq) in alias.items():
            db.execute('INSERT OR IGNORE INTO key_alias(old_key,new_key,effective_seq,source_event) VALUES (?,?,?,NULL)',(old,new,seq))
        runs=[]
        for r in db.execute('SELECT id,watermark,status FROM replay_run ORDER BY id'):
            scope={'decisions':[list(x) for x in db.execute('SELECT key,status,value FROM decision WHERE run_id=? ORDER BY key',(r['id'],))],
              'excluded':[x[0] for x in db.execute('SELECT txn_id FROM run_exclusion WHERE run_id=? ORDER BY txn_id',(r['id'],))]}
            runs.append({'id':r['id'],'watermark':r['watermark'],'status':r['status'],'digest':_digest(_canonical(scope))})
        summary={'floor':floor,
          'live':{k:{'value':v,'digest':_digest(v)} for k,v in sorted(live.items())},
          'tombstones':{k:{'deleted_seq':seq,'before_digest':mark} for k,(seq,mark) in sorted(deleted.items())},
          'aliases':[{'old_key':old,'new_key':new,'effective_seq':seq} for old,(new,seq) in sorted(alias.items())],
          'blocked':sorted(bad),
          'runs':runs}
        text=_canonical(summary);digest=_digest(text)
        db.execute('INSERT INTO retention_compaction(floor,summary,digest) VALUES (?,?,?)',(floor,text,digest))
        db.execute('''DELETE FROM change_event WHERE seq<? AND txn_id NOT IN (
          SELECT txn_id FROM change_event WHERE kind='commit' AND seq>=?) AND txn_id NOT IN (
          SELECT DISTINCT txn_id FROM change_event WHERE id IN (SELECT event_id FROM evidence WHERE event_id IS NOT NULL)
            OR id IN (SELECT event_id FROM issue WHERE event_id IS NOT NULL)
            OR id IN (SELECT source_event FROM key_alias WHERE source_event IS NOT NULL))''',(floor,floor))
    db.close();return floor,digest
