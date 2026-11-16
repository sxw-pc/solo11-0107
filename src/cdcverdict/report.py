"""可恢复的报告发布流程。

每次发布是一个 report_batch 生成批次：先在暂存目录写出 report.json/report.md/manifest.json，
再复制进 `<out>-run<N>-gen<G>` 公开目录，manifest 最后落盘，随后核对成员摘要才把批次记为
complete。文件写入与 SQLite 更新不共享事务，任一切点中断后，同摘要重试沿用同一代次并跳过
已存在的成员；内容变化另开代次，已完成的 bundle 不被覆盖。certified 要求成员齐全才公开；
preview 允许带 provisional 标记和缺件清单先行公开，缺件补齐且摘要核对一致后由 rules.archive
把运行置为已归档——已归档只指向完整且核验过的 bundle。
"""
import hashlib,json,os,shutil
from pathlib import Path
from .store import connect
from .rules import archive
CONTRACTS=('certified','preview')
MEMBERS=('report.json','report.md','manifest.json')
SEALED=('report.json','report.md')
def _sha(b):return hashlib.sha256(b).hexdigest()
def _payload(db,run):
    run_row=dict(db.execute('SELECT * FROM replay_run WHERE id=?',(run,)).fetchone())
    run_row.pop('status')
    data={'run':run_row,
      'decisions':[dict(x) for x in db.execute('SELECT * FROM decision WHERE run_id=? ORDER BY key',(run,))],
      'evidence':[dict(x) for x in db.execute('SELECT * FROM evidence WHERE run_id=? ORDER BY id',(run,))],
      'issues':[dict(x) for x in db.execute('SELECT i.* FROM issue i JOIN run_issue ri ON ri.issue_id=i.id WHERE ri.run_id=? ORDER BY i.id',(run,))],
      'sources':[dict(x) for x in db.execute('SELECT id,path,sha FROM source_file ORDER BY id')],
      'excluded_transactions':[r['txn_id'] for r in db.execute('SELECT txn_id FROM run_exclusion WHERE run_id=? ORDER BY txn_id',(run,))]}
    expected={r['key']:r['value'] for r in db.execute('SELECT * FROM expected')}
    actual={r['key']:r['value'] for r in data['decisions'] if r['status']=='存在'}
    data['expected_differences']=[k for k in sorted(set(expected)|set(actual)) if expected.get(k)!=actual.get(k)] if expected else []
    return data
def _sealed(data):
    return {'report.json':(json.dumps(data,ensure_ascii=False,indent=2)+'\n').encode(),
      'report.md':('# 回放结论\n\n'+'\n'.join(f"- {x['key']}: {x['status']}" for x in data['decisions'])+'\n').encode()}
def _manifest(data,contract,missing,generation,sealed):
    doc={'run_id':data['run']['id'],'generation':generation,'contract':contract,
      'provisional':bool(missing),'missing':list(missing),
      'members':[{'name':name,'path':name,'sha':_sha(sealed[name])} for name in SEALED]}
    return (json.dumps(doc,ensure_ascii=False,indent=2)+'\n').encode()
def _digest(sealed):return _sha(''.join(_sha(sealed[name]) for name in SEALED).encode())
def _write_atomic(path,content):
    tmp=path.with_name(path.name+'.tmp');tmp.write_bytes(content);os.replace(tmp,path)
def _staging_dir(out,run,generation):return Path(out).parent/f'.{Path(out).name}-run{run}-gen{generation}.staging'
def _publish_dir(out,run,generation):return Path(out).parent/f'{Path(out).name}-run{run}-gen{generation}'
def _verify(db,batch,missing_ok=False):
    missing=json.loads(batch['missing'])
    if missing and not missing_ok:return False
    if not batch['published_dir']:return False
    members={r['name']:r for r in db.execute('SELECT * FROM report_member WHERE batch_id=?',(batch['id'],))}
    if set(members)!={m for m in MEMBERS if m not in missing}:return False
    for m in members.values():
        p=Path(m['path'])
        if not p.is_file() or _sha(p.read_bytes())!=m['sha']:return False
    if missing:return True
    return _digest({name:Path(members[name]['path']).read_bytes() for name in SEALED})==batch['digest']
def _finalize(db,batch_id,run):
    batch=db.execute('SELECT * FROM report_batch WHERE id=?',(batch_id,)).fetchone()
    archive(db,run,batch['status']=='complete' and _verify(db,batch))
def _stage(db,batch_id,out,run,generation,files,missing):
    staged=_staging_dir(out,run,generation);staged.mkdir(parents=True,exist_ok=True)
    for name in MEMBERS:
        if name in missing:continue
        path=staged/name
        if not(path.exists() and path.is_file() and path.read_bytes()==files[name]):
            _write_atomic(path,files[name])
        db.execute("INSERT OR REPLACE INTO report_member VALUES (?,?,?,?,'staged')",(batch_id,name,str(path),_sha(files[name])))
    db.execute("UPDATE report_batch SET staged_dir=?,missing=? WHERE id=?",(str(staged),json.dumps(missing),batch_id))
def _publish(db,batch_id,out,run,generation,files):
    published=_publish_dir(out,run,generation)
    if published.exists():
        if not published.is_dir():raise OSError(f'publish_target_not_directory: {published}')
    else:published.mkdir(parents=True)
    for name in MEMBERS:
        row=db.execute('SELECT * FROM report_member WHERE batch_id=? AND name=?',(batch_id,name)).fetchone()
        if not row:continue
        target=published/name
        if not(target.exists() and target.is_file() and _sha(target.read_bytes())==row['sha']):
            _write_atomic(target,files[name])
        db.execute("UPDATE report_member SET status='published',path=? WHERE batch_id=? AND name=?",(str(target),batch_id,name))
    db.execute("UPDATE report_batch SET status='complete',published_dir=? WHERE id=?",(str(published),batch_id))
    shutil.rmtree(_staging_dir(out,run,generation),ignore_errors=True)
def write(path,run,out,contract='certified',missing=()):
    if contract not in CONTRACTS:raise ValueError('unknown_publish_contract')
    missing=sorted(set(missing))
    if any(m not in SEALED for m in missing):raise ValueError('unknown_report_member')
    if contract=='certified' and missing:raise ValueError('certified_requires_complete_bundle')
    db=connect(path)
    try:
        with db:
            if not db.execute('SELECT 1 FROM replay_run WHERE id=?',(run,)).fetchone():raise ValueError('unknown_run')
            data=_payload(db,run);sealed=_sealed(data);digest=_digest(sealed)
            batch=db.execute('SELECT * FROM report_batch WHERE run_id=? AND contract=? AND digest=? ORDER BY generation DESC LIMIT 1',(run,contract,digest)).fetchone()
            if batch and batch['status']=='complete' and json.loads(batch['missing'])==missing and _verify(db,batch,missing_ok=contract=='preview'):
                _finalize(db,batch['id'],run)
                return Path(batch['published_dir'])/'report.json'
            if batch:
                if not set(missing)<=set(json.loads(batch['missing'])):raise ValueError('missing_members_regress')
                batch_id,generation=batch['id'],batch['generation']
            else:
                generation=db.execute('SELECT COALESCE(MAX(generation),0) FROM report_batch WHERE run_id=?',(run,)).fetchone()[0]+1
                batch_id=db.execute("INSERT INTO report_batch(run_id,generation,contract,status,digest,missing) VALUES (?,?,?,'staging',?,?)",(run,generation,contract,digest,json.dumps(missing))).lastrowid
            files=dict(sealed);files['manifest.json']=_manifest(data,contract,missing,generation,sealed)
        try:
            with db:_stage(db,batch_id,out,run,generation,files,missing)
            with db:_publish(db,batch_id,out,run,generation,files)
            with db:
                batch=db.execute('SELECT * FROM report_batch WHERE id=?',(batch_id,)).fetchone()
                if not _verify(db,batch,missing_ok=contract=='preview'):raise OSError('report_bundle_verification_failed')
                _finalize(db,batch_id,run)
        except OSError:
            with db:db.execute("UPDATE replay_run SET status='待核' WHERE id=? AND status='已判定'",(run,))
            raise
        return Path(batch['published_dir'])/'report.json'
    finally:db.close()
