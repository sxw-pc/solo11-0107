"""Target consistency and critical-consumer release decisions.

These ledgers intentionally never overwrite a replay run: a later target snapshot or
acknowledgement produces a new decision while earlier evidence stays inspectable.
"""
import hashlib
import json
from pathlib import Path

from .store import connect


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _target_digest(db,target_id):
    rows=[tuple(r) for r in db.execute('SELECT key,value FROM target_row WHERE snapshot_id=? ORDER BY key',(target_id,))]
    return _sha(json.dumps(rows,sort_keys=True).encode())


def import_target(db_path, file, watermark):
    raw = Path(file).read_bytes()
    rows = [json.loads(line) for line in raw.decode('utf-8').splitlines() if line.strip()]
    values = {}
    for row in rows:
        key = str(row['key'])
        if not key or key in values:
            raise ValueError('target_duplicate_key')
        values[key] = json.dumps(row['value'], sort_keys=True, ensure_ascii=False)
    db = connect(db_path)
    try:
        with db:
            old = db.execute('SELECT id,watermark FROM target_snapshot WHERE source_sha=?', (_sha(raw),)).fetchone()
            if old:
                if old['watermark'] != watermark:
                    raise ValueError('target_snapshot_contract_changed')
                return old['id']
            target_id = db.execute('INSERT INTO target_snapshot(watermark,source_sha,raw) VALUES (?,?,?)',
                                   (watermark, _sha(raw), raw)).lastrowid
            db.executemany('INSERT INTO target_row VALUES (?,?,?)',
                           [(target_id, key, value) for key, value in sorted(values.items())])
            return target_id
    finally:
        db.close()


def audit_target(db_path, run_id, target_id):
    db = connect(db_path)
    try:
        with db:
            run = db.execute('SELECT * FROM replay_run WHERE id=?', (run_id,)).fetchone()
            target = db.execute('SELECT * FROM target_snapshot WHERE id=?', (target_id,)).fetchone()
            if not run or not target:
                raise ValueError('unknown_run_or_target')
            replay = {r['key']: r for r in db.execute('SELECT * FROM visible_row WHERE run_id=?', (run_id,))}
            observed = {r['key']: r['value'] for r in db.execute('SELECT * FROM target_row WHERE snapshot_id=?', (target_id,))}
            differences = []
            if target['watermark'] < run['watermark']:
                status, reason = 'lagging', 'target_watermark_behind_replay'
            elif target['watermark'] > run['watermark']:
                status, reason = 'unknown', 'target_ahead_without_comparable_replay'
            elif run['status'] not in {'已判定','已归档'} or any(r['status'] == '无法判定' for r in replay.values()):
                status, reason = 'unknown', 'replay_evidence_incomplete'
            else:
                for key in sorted(set(replay) | set(observed)):
                    row = replay.get(key)
                    expected = row['value'] if row and row['status'] == '存在' else None
                    actual = observed.get(key)
                    if expected != actual:
                        kind = 'extra' if expected is None and actual is not None else 'missing' if actual is None else 'value_mismatch'
                        differences.append((key, kind, expected, actual))
                status, reason = ('divergent', 'visible_rows_disagree') if differences else ('ready', 'same_visible_state')
            audit_id = db.execute('INSERT INTO cutover_audit(run_id,target_id,status,reason,target_digest) VALUES (?,?,?,?,?)',
                                  (run_id, target_id, status, reason,_target_digest(db,target_id))).lastrowid
            db.executemany('INSERT INTO cutover_difference VALUES (?,?,?,?,?)',
                           [(audit_id, *row) for row in differences])
            return audit_id, status, differences
    finally:
        db.close()


def configure_consumers(db_path, file):
    raw = Path(file).read_bytes()
    rows = json.loads(raw)
    if not isinstance(rows, list) or not rows:
        raise ValueError('empty_consumer_contract')
    ids = [str(row['id']) for row in rows]
    if any(not value for value in ids) or len(set(ids)) != len(ids):
        raise ValueError('duplicate_consumer')
    db = connect(db_path)
    try:
        with db:
            for row in rows:
                value = (str(row['id']), int(bool(row['critical'])), int(row['minimum_watermark']), _sha(raw))
                old = db.execute('SELECT * FROM consumer_contract WHERE id=?', (value[0],)).fetchone()
                if old and tuple(old[k] for k in ('id','critical','minimum_watermark','config_sha')) != value:
                    raise ValueError('consumer_contract_changed')
                db.execute('INSERT OR IGNORE INTO consumer_contract VALUES (?,?,?,?)', value)
    finally:
        db.close()


def acknowledge(db_path, consumer, run_id, sequence, status, source_digest):
    if status not in {'accepted', 'rejected'}:
        raise ValueError('invalid_ack_status')
    db = connect(db_path)
    try:
        with db:
            if not db.execute('SELECT 1 FROM consumer_contract WHERE id=?', (consumer,)).fetchone():
                raise ValueError('unknown_consumer')
            if not db.execute('SELECT 1 FROM replay_run WHERE id=?', (run_id,)).fetchone():
                raise ValueError('unknown_run')
            old = db.execute('SELECT * FROM consumer_ack WHERE consumer_id=? AND sequence=?', (consumer, sequence)).fetchone()
            if old:
                if (old['run_id'],old['status'],old['source_digest']) != (run_id,status,source_digest):
                    raise ValueError('ack_identity_conflict')
                return old['id']
            return db.execute('INSERT INTO consumer_ack(consumer_id,run_id,sequence,status,source_digest) VALUES (?,?,?,?,?)',
                              (consumer,run_id,sequence,status,source_digest)).lastrowid
    finally:
        db.close()


def release_decision(db_path, audit_id):
    db = connect(db_path)
    try:
        with db:
            audit = db.execute('SELECT * FROM cutover_audit WHERE id=?', (audit_id,)).fetchone()
            if not audit:
                raise ValueError('unknown_audit')
            run = db.execute('SELECT * FROM replay_run WHERE id=?', (audit['run_id'],)).fetchone()
            target_changed=_target_digest(db,audit['target_id'])!=audit['target_digest']
            members=[]
            for consumer in db.execute('SELECT * FROM consumer_contract ORDER BY id'):
                ack = db.execute('SELECT * FROM consumer_ack WHERE consumer_id=? AND run_id=? ORDER BY sequence DESC LIMIT 1',
                                 (consumer['id'], audit['run_id'])).fetchone()
                state = ('not_required' if not consumer['critical'] else
                         'watermark_short' if run['watermark'] < consumer['minimum_watermark'] else
                         'missing_ack' if not ack else
                         'rejected' if ack['status'] != 'accepted' else
                         'stale_ack' if ack['source_digest'] != db.execute('SELECT source_sha FROM target_snapshot WHERE id=?',
                           (audit['target_id'],)).fetchone()[0] else 'accepted')
                members.append((consumer['id'], state, ack['id'] if ack else None))
            if not members:
                raise ValueError('no_consumer_contract')
            status = 'ready' if audit['status'] == 'ready' and not target_changed and all(x[1] in {'accepted','not_required'} for x in members) else 'blocked'
            reason = 'target_changed_after_audit' if target_changed else audit['status'] if audit['status'] != 'ready' else ','.join(f'{name}:{state}' for name,state,_ in members if state not in {'accepted','not_required'}) or 'all_critical_accepted'
            gate_id = db.execute('INSERT INTO release_gate(audit_id,run_id,status,reason) VALUES (?,?,?,?)',
                                 (audit_id,audit['run_id'],status,reason)).lastrowid
            db.executemany('INSERT INTO release_member VALUES (?,?,?,?)',[(gate_id,*x) for x in members])
            return gate_id,status,reason
    finally:
        db.close()


def audit_repair_merge(db_path, merge_id):
    """Re-audit a merged repair snapshot; the merged snapshot is never a release target itself."""
    from .repair import repair_merge_report
    report = repair_merge_report(db_path, merge_id)
    merge = report['merge']
    if merge['status'] != 'merged':
        raise ValueError('repair_merge_conflict')
    if merge['output_target_id'] is None:
        raise ValueError('repair_merge_has_no_snapshot')
    db = connect(db_path)
    try:
        # Bind to the audit created for this merged snapshot; never reuse an
        # audit of an intermediate branch snapshot.
        existing = db.execute('SELECT id,status FROM cutover_audit WHERE run_id=? AND target_id=? '
                              'AND target_digest=? ORDER BY id DESC LIMIT 1',
                              (merge['run_id'], merge['output_target_id'], merge['output_digest'])).fetchone()
        if existing:
            return existing['id'], existing['status'], None
        run_id = merge['run_id']
    finally:
        db.close()
    return audit_target(db_path, run_id, merge['output_target_id'])


def release_repair_merge(db_path, merge_id):
    """Release gate bound to the new audit of a merged repair snapshot.

    A blocked or partial merge never yields a ready gate, and the gate always
    references a fresh audit row rather than the frozen pre-merge audit.
    """
    from .repair import repair_merge_report
    report = repair_merge_report(db_path, merge_id)
    merge = report['merge']
    if merge['status'] != 'merged':
        raise ValueError('repair_merge_conflict')
    unresolved = [row['key'] for row in report['keys'] if row['outcome'] in {'unresolved', 'conflict', 'deferred'}]
    if unresolved:
        raise ValueError('repair_merge_incomplete')
    audit_id, status, _ = audit_repair_merge(db_path, merge_id)
    _, gate_status, reason = release_decision(db_path, audit_id)
    if status != 'ready' and gate_status == 'ready':
        raise ValueError('repair_merge_audit_not_ready')
    return audit_id, gate_status, reason

