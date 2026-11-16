import hashlib,json
from pathlib import Path
from .store import connect
from .contract import validate_snapshot
def sha(b): return hashlib.sha256(b).hexdigest()
def source(db,path):
    raw=Path(path).read_bytes();h=sha(raw)
    row=db.execute('SELECT id FROM source_file WHERE sha=?',(h,)).fetchone()
    if row:return row['id'],True
    sid=db.execute('INSERT INTO source_file(path,sha,raw) VALUES (?,?,?)',(str(path),h,raw)).lastrowid
    return sid,False
def snapshot(path,db_path,watermark,schema='v1'):
    raw=Path(path).read_bytes();db=connect(db_path)
    records=[json.loads(s) for s in raw.decode('utf-8').splitlines() if s.strip()]
    validate_snapshot(records,watermark,schema)
    with db:
        frozen=db.execute('SELECT s.watermark,s.schema_version,f.sha FROM snapshot_row s JOIN source_file f ON f.id=s.source_id LIMIT 1').fetchone()
        if frozen:
            if watermark<frozen['watermark']:raise ValueError('watermark_regression')
            if watermark!=frozen['watermark'] or schema!=frozen['schema_version'] or sha(raw)!=frozen['sha']:
                raise ValueError('snapshot_contract_changed')
        else:
            sid,duplicate=source(db,path)
            if duplicate:raise ValueError('source_already_used')
            for r in records:
                db.execute('INSERT INTO snapshot_row VALUES (?,?,?,?,?)',(str(r['key']),json.dumps(r['value'],sort_keys=True),watermark,schema,sid))
    db.close();return len(records)
def ingest(path,db_path):
    db=connect(db_path);count=0
    raw=Path(path).read_bytes();lines=raw.decode('utf-8').splitlines()
    with db:
        sid,duplicate=source(db,path)
        if duplicate:return 0
        for n,line in enumerate(lines,1):
            if not line.strip():continue
            try:
                e=json.loads(line);kind=e['kind'];tx=str(e['txn']);seq=int(e['seq']);part=str(e.get('partition','p0'))
                if kind not in ('begin','change','commit'):raise ValueError('kind')
                h=sha(line.encode())
                old=db.execute('SELECT sha FROM change_event WHERE partition=? AND seq=? AND txn_id=?',(part,seq,tx)).fetchone()
                if old:
                    if old['sha']!=h:db.execute('INSERT INTO issue(code,key,detail) VALUES (?,?,?)',('identity_conflict',e.get('key'),f'{part}:{seq}:{tx}'))
                    continue
                db.execute('INSERT INTO change_event(source_id,line_no,partition,seq,txn_id,kind,key,before_value,after_value,alias_to,sha,raw) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                  (sid,n,part,seq,tx,kind,e.get('key'),json.dumps(e.get('before'),sort_keys=True),json.dumps(e.get('after'),sort_keys=True),e.get('alias_to'),h,line))
                count+=1
            except (ValueError,KeyError,TypeError,json.JSONDecodeError) as exc:
                db.execute('INSERT INTO issue(code,detail) VALUES (?,?)',('parse_error',f'line {n}: {exc}'))
    db.close();return count
