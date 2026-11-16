"""Copy-on-write repair of a divergent target against a frozen replay verdict.

Decisions are explicit per business key. A repair creates a target snapshot rather
than changing the audited input, so a later audit and consumer acknowledgement
must refer to the new snapshot.
"""
import hashlib
import json
from pathlib import Path

from .cutover import _target_digest
from .store import connect


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def plan_repair(path, audit_id, manifest_file):
    raw = Path(manifest_file).read_bytes()
    choices = json.loads(raw.decode('utf-8'))
    if not isinstance(choices, list) or not choices:
        raise ValueError('empty_repair_manifest')
    db = connect(path)
    try:
        with db:
            audit = db.execute('SELECT * FROM cutover_audit WHERE id=?', (audit_id,)).fetchone()
            if not audit or audit['status'] != 'divergent':
                raise ValueError('audit_not_divergent')
            if _target_digest(db, audit['target_id']) != audit['target_digest']:
                raise ValueError('audited_target_changed')
            differences = {row['key']: row for row in db.execute('SELECT * FROM cutover_difference WHERE audit_id=?', (audit_id,))}
            keys = [str(row['key']) for row in choices]
            if len(set(keys)) != len(keys) or set(keys) != set(differences):
                raise ValueError('repair_manifest_incomplete')
            for row in choices:
                if row['decision'] not in {'restore_replay', 'accept_target', 'defer'} or not str(row.get('reason', '')).strip():
                    raise ValueError('invalid_repair_choice')
            digest = _sha(raw)
            previous = db.execute('SELECT id FROM repair_plan WHERE audit_id=? AND manifest_sha=?',
                                  (audit_id, digest)).fetchone()
            if previous:
                return previous['id']
            plan = db.execute("INSERT INTO repair_plan(audit_id,manifest_sha,raw,status) VALUES (?,?,?,'planned')",
                              (audit_id, digest, raw)).lastrowid
            db.executemany('INSERT INTO repair_choice VALUES (?,?,?,?,?,?)',
                           [(plan, row['key'], row['decision'], row['reason'],
                             differences[row['key']]['replay_value'], differences[row['key']]['target_value'])
                            for row in choices])
            return plan
    finally:
        db.close()


def apply_repair(path, plan_id):
    db = connect(path)
    try:
        with db:
            plan = db.execute('SELECT * FROM repair_plan WHERE id=?', (plan_id,)).fetchone()
            if not plan:
                raise ValueError('unknown_repair_plan')
            if plan['status'] == 'applied':
                return plan['output_target_id'], plan['result']
            audit = db.execute('SELECT * FROM cutover_audit WHERE id=?', (plan['audit_id'],)).fetchone()
            if _target_digest(db, audit['target_id']) != audit['target_digest']:
                raise ValueError('audited_target_changed')
            target = db.execute('SELECT * FROM target_snapshot WHERE id=?', (audit['target_id'],)).fetchone()
            rows = {row['key']: row['value'] for row in db.execute('SELECT * FROM target_row WHERE snapshot_id=?',
                                                                    (audit['target_id'],))}
            restored = 0
            accepted = 0
            deferred = 0
            for choice in db.execute('SELECT * FROM repair_choice WHERE plan_id=? ORDER BY key', (plan_id,)):
                if rows.get(choice['key']) != choice['target_value']:
                    raise ValueError('repair_target_conflict')
                if choice['decision'] == 'restore_replay':
                    if choice['replay_value'] is None:
                        rows.pop(choice['key'], None)
                    else:
                        rows[choice['key']] = choice['replay_value']
                    restored += 1
                elif choice['decision'] == 'accept_target':
                    accepted += 1
                else:
                    deferred += 1
            if not restored:
                output = target['id']
            else:
                raw = ('\n'.join(json.dumps({'key': key, 'value': json.loads(value)}, sort_keys=True, ensure_ascii=False)
                                 for key, value in sorted(rows.items()))).encode('utf-8')
                digest = _sha(raw)
                existing = db.execute('SELECT id,watermark FROM target_snapshot WHERE source_sha=?', (digest,)).fetchone()
                if existing and existing['watermark'] != target['watermark']:
                    raise ValueError('repair_snapshot_watermark_conflict')
                if existing:
                    actual = {row['key']: row['value'] for row in db.execute('SELECT key,value FROM target_row WHERE snapshot_id=?',
                                                                              (existing['id'],))}
                    if actual != rows:
                        raise ValueError('repair_snapshot_conflict')
                    output = existing['id']
                else:
                    output = db.execute('INSERT INTO target_snapshot(watermark,source_sha,raw) VALUES (?,?,?)',
                                        (target['watermark'], digest, raw)).lastrowid
                    db.executemany('INSERT INTO target_row VALUES (?,?,?)',
                                   [(output, key, value) for key, value in sorted(rows.items())])
            result = 'repaired' if restored and not accepted and not deferred else 'partial' if restored else 'accepted_deviation' if accepted else 'deferred'
            db.execute("UPDATE repair_plan SET status='applied',output_target_id=?,result=? WHERE id=?",
                       (output, result, plan_id))
            return output, result
    finally:
        db.close()


def repair_report(path, plan_id):
    db = connect(path)
    try:
        plan = db.execute('SELECT id,audit_id,manifest_sha,status,output_target_id,result FROM repair_plan WHERE id=?',
                          (plan_id,)).fetchone()
        if not plan:
            raise ValueError('unknown_repair_plan')
        return {'plan': dict(plan),
                'choices': [dict(row) for row in db.execute('SELECT key,decision,reason,replay_value,target_value FROM repair_choice WHERE plan_id=? ORDER BY key', (plan_id,))]}
    finally:
        db.close()
