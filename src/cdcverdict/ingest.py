import hashlib,json
from pathlib import Path
from .store import connect
from .contract import validate_snapshot,compile_contract,load_contract,DEFAULT_TABLE
def sha(b): return hashlib.sha256(b).hexdigest()
def source(db,path):
    raw=Path(path).read_bytes();h=sha(raw)
    row=db.execute('SELECT id FROM source_file WHERE sha=?',(h,)).fetchone()
    if row:return row['id'],True
    sid=db.execute('INSERT INTO source_file(path,sha,raw) VALUES (?,?,?)',(str(path),h,raw)).lastrowid
    return sid,False
def register_contract(db,spec):
    """Compile and persist a contract; an identical contract is idempotent."""
    contract=compile_contract(spec)
    row=db.execute('SELECT id FROM contract_registry WHERE sha=?',(contract.sha,)).fetchone()
    if row:return contract
    existing=db.execute('SELECT sha FROM contract_registry ORDER BY id DESC LIMIT 1').fetchone()
    if existing:
        raise ValueError('contract_changed')
    cid=db.execute('INSERT INTO contract_registry(schema_version,raw,sha) VALUES (?,?,?)',
                   (contract.schema,contract.raw,contract.sha)).lastrowid
    db.executemany('INSERT INTO table_contract VALUES (?,?,?)',
                   [(cid,t.name,t.key_field) for t in contract.tables.values()])
    db.executemany('INSERT INTO reference_contract VALUES (?,?,?,?,?,?)',
                   [(cid,r.child_table,r.child_field,r.parent_table,r.parent_field,r.mode)
                    for r in contract.references])
    return contract
def snapshot(path,db_path,watermark,schema='v1',contract_spec=None):
    raw=Path(path).read_bytes();db=connect(db_path)
    records=[json.loads(s) for s in raw.decode('utf-8').splitlines() if s.strip()]
    with db:
        contract=register_contract(db,contract_spec) if contract_spec is not None else load_contract(db)
        validate_snapshot(records,watermark,schema,contract)
        frozen=db.execute('SELECT s.watermark,s.schema_version,f.sha FROM snapshot_row s JOIN source_file f ON f.id=s.source_id LIMIT 1').fetchone()
        if frozen:
            if watermark<frozen['watermark']:raise ValueError('watermark_regression')
            if watermark!=frozen['watermark'] or schema!=frozen['schema_version'] or sha(raw)!=frozen['sha']:
                raise ValueError('snapshot_contract_changed')
        else:
            sid,duplicate=source(db,path)
            if duplicate:raise ValueError('source_already_used')
            for r in records:
                table=r['table'] if contract is not None else DEFAULT_TABLE
                db.execute('INSERT INTO snapshot_row(table_name,key,value,watermark,schema_version,source_id) VALUES (?,?,?,?,?,?)',
                           (table,str(r['key']),json.dumps(r['value'],sort_keys=True),watermark,schema,sid))
    db.close();return len(records)
def ingest(path,db_path):
    db=connect(db_path);count=0
    raw=Path(path).read_bytes();lines=raw.decode('utf-8').splitlines()
    contract=load_contract(db)
    with db:
        sid,duplicate=source(db,path)
        if duplicate:return 0
        for n,line in enumerate(lines,1):
            if not line.strip():continue
            try:
                e=json.loads(line);kind=e['kind'];tx=str(e['txn']);seq=int(e['seq']);part=str(e.get('partition','p0'))
                if kind not in ('begin','change','commit'):raise ValueError('kind')
                if kind=='change':
                    table=str(e['table']) if 'table' in e else DEFAULT_TABLE
                    if contract is not None and table not in contract.table_names:
                        raise ValueError(f'unknown_table:{table}')
                else:
                    table=''
                h=sha(line.encode())
                old=db.execute('SELECT sha FROM change_event WHERE partition=? AND seq=? AND txn_id=? AND table_name=?',
                               (part,seq,tx,table)).fetchone()
                if old:
                    if old['sha']!=h:db.execute('INSERT INTO issue(code,table_name,key,detail) VALUES (?,?,?,?)',
                                                ('identity_conflict',table or None,e.get('key'),f'{part}:{seq}:{tx}'))
                    continue
                db.execute('INSERT INTO change_event(source_id,line_no,partition,seq,txn_id,table_name,kind,key,before_value,after_value,alias_to,sha,raw) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
                  (sid,n,part,seq,tx,table,kind,e.get('key'),json.dumps(e.get('before'),sort_keys=True),json.dumps(e.get('after'),sort_keys=True),e.get('alias_to'),h,line))
                count+=1
            except (ValueError,KeyError,TypeError,json.JSONDecodeError) as exc:
                db.execute('INSERT INTO issue(code,detail) VALUES (?,?)',('parse_error',f'line {n}: {exc}'))
    db.close();return count
