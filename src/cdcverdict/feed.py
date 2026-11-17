"""Transaction-level consumer change feed with recoverable JSONL file delivery.

Two consumer semantics are deliberately distinct, not a strict/partial switch on
one contract:

* ``ledger`` consumers must receive the full transaction envelope (every event,
  ``complete=true``). If the authorized key scope does not cover every change
  member of a transaction, delivery is blocked and a scope gap is recorded; the
  consumer must never receive content that could be mistaken for a complete txn.
* ``key_alert`` consumers receive only the projected change events whose keys are
  authorized, always marked ``complete=false`` with the original member count,
  projected count and hidden count. A projection can drive a local alert but can
  never be used to conclude anything about the whole transaction.

The feedable transaction set is derived from the selected run's own evidence:
``timeline.transactions`` decides accepted/excluded, ``rules.replay`` writes
run-scoped evidence only for transactions that survived replay, and
``run_exclusion`` holds the watermark-excluded txn ids. Excluded, failed or
evidence-less transactions are never delivered.

Delivery is at-least-once. Each delivery first persists its envelope, payload
bytes and idempotency key (consumer/run/txn) in SQLite, then writes the same
bytes to a temp file and atomically renames it to the JSONL sink, and only an
explicit ``ack`` advances the per-consumer cursor. A crash after the file is
published but before the ack makes ``recover`` re-publish the identical bytes
under the same idempotency key; exactly-once without external cooperation is not
promised. Cursors are independent per consumer and delivery order follows
commit_seq monotonically, never passing an un-ACKed earlier transaction.
"""
import hashlib
import json
import os
from pathlib import Path

from .store import connect


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def register_subscription(db_path, file):
    """Freeze per-consumer subscription contracts, authorized keys and purpose."""
    raw = Path(file).read_bytes()
    rows = json.loads(raw)
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list) or not rows:
        raise ValueError('empty_subscription')
    db = connect(db_path)
    try:
        with db:
            seen = set()
            normalized = []
            for row in rows:
                consumer = str(row['id'])
                kind = str(row['kind'])
                if not consumer or consumer in seen:
                    raise ValueError('duplicate_subscription')
                if kind not in {'ledger', 'key_alert'}:
                    raise ValueError('invalid_subscription_kind')
                purpose = str(row.get('purpose') or '')
                if not purpose:
                    raise ValueError('subscription_purpose_required')
                keys = sorted({str(k) for k in row.get('keys', ()) if str(k)})
                if not keys:
                    raise ValueError('subscription_scope_empty')
                sink_dir = row.get('sink_dir')
                normalized.append((consumer, kind, purpose, sink_dir, keys))
                seen.add(consumer)
            for consumer, kind, purpose, sink_dir, keys in normalized:
                old = db.execute('SELECT * FROM txn_subscription WHERE consumer_id=?', (consumer,)).fetchone()
                if old:
                    if old['kind'] != kind or old['purpose'] != purpose or old['sink_dir'] != sink_dir:
                        raise ValueError('subscription_contract_changed')
                    old_keys = {r['key'] for r in db.execute(
                        'SELECT key FROM subscription_key WHERE consumer_id=?', (consumer,))}
                    if not old_keys.issubset(set(keys)):
                        raise ValueError('subscription_scope_shrunk')
                    db.execute('UPDATE txn_subscription SET config_sha=? WHERE consumer_id=?',
                               (_sha(raw), consumer))
                else:
                    db.execute('INSERT INTO txn_subscription VALUES (?,?,?,?,?,CURRENT_TIMESTAMP)',
                               (consumer, kind, purpose, sink_dir, _sha(raw)))
                db.executemany('INSERT OR IGNORE INTO subscription_key VALUES (?,?)',
                               [(consumer, k) for k in keys])
        return [n[0] for n in normalized]
    finally:
        db.close()


def _subscription(db, consumer):
    sub = db.execute('SELECT * FROM txn_subscription WHERE consumer_id=?', (consumer,)).fetchone()
    if not sub:
        raise ValueError('unknown_subscription')
    return sub


def _scope_keys(db, consumer):
    return {r['key'] for r in db.execute(
        'SELECT key FROM subscription_key WHERE consumer_id=? ORDER BY key', (consumer,))}


def _accepted_run_txns(db, run_id):
    """Accepted transactions backed by the selected run's actual evidence.

    Joins run-scoped evidence to the validated txn table, filters out
    watermark exclusions, and groups per txn in commit order. The evidence
    count must equal the number of change members: a mismatch means the run
    evidence is incomplete, so the txn cannot be offered as a full delivery.
    """
    out = []
    rows = db.execute(
        '''SELECT t.id AS txn_id,t.commit_seq,
                  COUNT(DISTINCT e.event_id) AS evidence_events
           FROM evidence e
           JOIN change_event ce ON ce.id=e.event_id
           JOIN txn t ON t.id=ce.txn_id
           WHERE e.run_id=? AND t.valid=1 AND t.committed=1 AND t.commit_seq IS NOT NULL
             AND NOT EXISTS (SELECT 1 FROM run_exclusion x
                             WHERE x.run_id=e.run_id AND x.txn_id=t.id)
           GROUP BY t.id,t.commit_seq
           HAVING evidence_events=(
             SELECT COUNT(*) FROM change_event c2
             WHERE c2.txn_id=t.id AND c2.kind='change')
           ORDER BY t.commit_seq,t.id''', (run_id,)).fetchall()
    for r in rows:
        out.append((r['txn_id'], r['commit_seq'], r['evidence_events']))
    return out


def _txn_events(db, txn_id):
    return [dict(r) for r in db.execute(
        'SELECT * FROM change_event WHERE txn_id=? ORDER BY seq,id', (txn_id,))]


def _change_members(events):
    return [e for e in events if e['kind'] == 'change']


def _serialize(payload):
    return (json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':')) + '\n').encode('utf-8')


def _build_envelope(db, sub, scope, run_id, txn_id, commit_seq):
    """Return (payload, blocking missing keys). Missing keys is non-empty only for ledgers."""
    events = _txn_events(db, txn_id)
    members = _change_members(events)
    member_keys = sorted({e['key'] for e in members if e['key']})
    missing = [k for k in member_keys if k not in scope]
    if sub['kind'] == 'ledger':
        if missing:
            return None, missing
        payload = {
            'consumer_id': sub['consumer_id'],
            'run_id': run_id,
            'txn_id': txn_id,
            'commit_seq': commit_seq,
            'delivery_kind': 'ledger',
            'purpose': sub['purpose'],
            'complete': True,
            'member_count': len(members),
            'events': [json.loads(e['raw']) for e in events],
        }
        return payload, []
    projected = [e for e in members if e['key'] and e['key'] in scope]
    payload = {
        'consumer_id': sub['consumer_id'],
        'run_id': run_id,
        'txn_id': txn_id,
        'commit_seq': commit_seq,
        'delivery_kind': 'key_alert',
        'purpose': sub['purpose'],
        'complete': False,
        'member_count': len(members),
        'projected_count': len(projected),
        'hidden_count': len(members) - len(projected),
        'events': [json.loads(e['raw']) for e in projected],
    }
    return payload, []


def _sink_dir(db, sub, sink_dir):
    directory = Path(sink_dir or sub['sink_dir'] or '')
    if not str(directory):
        raise ValueError('sink_dir_required')
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _atomic_publish(directory, name, blob):
    target = directory / name
    tmp = directory / ('.' + name + '.tmp')
    with open(tmp, 'wb') as handle:
        handle.write(blob)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, target)
    return target


def _pending_head(db, consumer):
    return db.execute(
        'SELECT * FROM txn_delivery WHERE consumer_id=? AND status!=? ORDER BY delivery_seq LIMIT 1',
        (consumer, 'acked')).fetchone()


def publish_feed(db_path, consumer, run_id, sink_dir=None):
    """Publish the next deliverable txn for one consumer, at most one per call.

    Ordering is enforced per consumer: an earlier un-ACKed delivery blocks all
    later ones. Ledger scope gaps block instead of emitting partial content.
    Returns a result dict describing what happened.
    """
    db = connect(db_path)
    try:
        sub = _subscription(db, consumer)
        run = db.execute('SELECT * FROM replay_run WHERE id=?', (run_id,)).fetchone()
        if not run:
            raise ValueError('unknown_run')
        head = _pending_head(db, consumer)
        if head:
            return {'consumer_id': consumer, 'action': 'blocked_pending_ack',
                    'delivery_id': head['id'], 'txn_id': head['txn_id'],
                    'commit_seq': head['commit_seq'], 'status': head['status']}
        scope = _scope_keys(db, consumer)
        cursor = db.execute('SELECT * FROM txn_feed_cursor WHERE consumer_id=?', (consumer,)).fetchone()
        # The cursor is run-scoped: a different replay run starts its own feed at
        # the beginning, even when its commit_seqs lie below an older run's cursor.
        same_run = cursor is not None and cursor['run_id'] == run_id
        last_seq = cursor['last_commit_seq'] if same_run else None
        last_txn = cursor['last_txn_id'] if same_run else None
        for txn_id, commit_seq, _evidence_count in _accepted_run_txns(db, run_id):
            if last_seq is not None and (commit_seq < last_seq or
                                         (commit_seq == last_seq and txn_id <= last_txn)):
                continue
            existing = db.execute(
                'SELECT * FROM txn_delivery WHERE consumer_id=? AND run_id=? AND txn_id=?',
                (consumer, run_id, txn_id)).fetchone()
            if existing:
                if existing['status'] == 'acked':
                    continue
                return {'consumer_id': consumer, 'action': 'blocked_pending_ack',
                        'delivery_id': existing['id'], 'txn_id': txn_id,
                        'commit_seq': commit_seq, 'status': existing['status']}
            payload, missing = _build_envelope(db, sub, scope, run_id, txn_id, commit_seq)
            if missing:
                with db:
                    db.execute(
                        '''INSERT INTO txn_scope_gap(consumer_id,run_id,txn_id,commit_seq,missing_keys)
                           VALUES (?,?,?,?,?)
                           ON CONFLICT(consumer_id,run_id,txn_id) DO UPDATE SET
                             missing_keys=excluded.missing_keys,
                             status=CASE WHEN txn_scope_gap.status='closed'
                                         THEN txn_scope_gap.status ELSE 'open' END''',
                        (consumer, run_id, txn_id, commit_seq, json.dumps(missing)))
                return {'consumer_id': consumer, 'action': 'scope_gap', 'txn_id': txn_id,
                        'commit_seq': commit_seq, 'missing_keys': missing}
            if sub['kind'] == 'key_alert' and not payload['events']:
                # Nothing authorized inside this txn: skip without creating a delivery.
                continue
            blob = _serialize(payload)
            digest = _sha(blob)
            with db:
                delivery_seq = db.execute(
                    'SELECT COALESCE(MAX(delivery_seq),0)+1 FROM txn_delivery WHERE consumer_id=?',
                    (consumer,)).fetchone()[0]
                delivery_id = db.execute(
                    '''INSERT INTO txn_delivery(consumer_id,run_id,txn_id,commit_seq,delivery_seq,
                          envelope_json,payload_blob,payload_sha,status)
                       VALUES (?,?,?,?,?,?,?,?,'pending')''',
                    (consumer, run_id, txn_id, commit_seq, delivery_seq,
                     json.dumps(payload, ensure_ascii=False, sort_keys=True), blob, digest)).lastrowid
            directory = _sink_dir(db, sub, sink_dir)
            name = f'{consumer}-{delivery_seq:08d}-run{run_id}-{txn_id}.jsonl'
            target = _atomic_publish(directory, name, blob)
            with db:
                db.execute(
                    "UPDATE txn_delivery SET status='published',sink_path=?,published_at=CURRENT_TIMESTAMP WHERE id=?",
                    (str(target), delivery_id))
            return {'consumer_id': consumer, 'action': 'published', 'delivery_id': delivery_id,
                    'delivery_seq': delivery_seq, 'run_id': run_id, 'txn_id': txn_id,
                    'commit_seq': commit_seq, 'sink_path': str(target), 'payload_sha': digest,
                    'complete': payload['complete']}
        return {'consumer_id': consumer, 'action': 'caught_up', 'run_id': run_id}
    finally:
        db.close()


def recover_feed(db_path, consumer, sink_dir=None):
    """Re-publish the identical bytes of the un-ACKed head under the same idempotency key.

    A crash between file publication and the local ACK leaves a ``published`` or
    ``pending`` delivery with no cursor advancement. Recovery never skips it,
    never creates a second logical delivery, and never passes it.
    """
    db = connect(db_path)
    try:
        sub = _subscription(db, consumer)
        head = _pending_head(db, consumer)
        if not head:
            return {'consumer_id': consumer, 'action': 'nothing_to_recover'}
        blob = head['payload_blob']
        if _sha(blob) != head['payload_sha']:
            raise ValueError('delivery_payload_corrupt')
        directory = _sink_dir(db, sub, sink_dir)
        name = f"{consumer}-{head['delivery_seq']:08d}-run{head['run_id']}-{head['txn_id']}.jsonl"
        target = _atomic_publish(directory, name, blob)
        if head['status'] != 'published' or head['sink_path'] != str(target):
            with db:
                db.execute(
                    "UPDATE txn_delivery SET status='published',sink_path=?,published_at=COALESCE(published_at,CURRENT_TIMESTAMP) WHERE id=?",
                    (str(target), head['id']))
        return {'consumer_id': consumer, 'action': 'recovered', 'delivery_id': head['id'],
                'delivery_seq': head['delivery_seq'], 'run_id': head['run_id'],
                'txn_id': head['txn_id'], 'commit_seq': head['commit_seq'],
                'sink_path': str(target), 'payload_sha': head['payload_sha']}
    finally:
        db.close()


def acknowledge_feed(db_path, consumer, run_id, txn_id):
    """Record the external ACK and advance only this consumer's cursor."""
    db = connect(db_path)
    try:
        _subscription(db, consumer)
        with db:
            delivery = db.execute(
                'SELECT * FROM txn_delivery WHERE consumer_id=? AND run_id=? AND txn_id=?',
                (consumer, run_id, txn_id)).fetchone()
            if not delivery:
                raise ValueError('unknown_delivery')
            if delivery['status'] == 'acked':
                return {'consumer_id': consumer, 'action': 'already_acked',
                        'delivery_id': delivery['id']}
            if delivery['status'] != 'published':
                raise ValueError('delivery_not_published')
            earlier = db.execute(
                'SELECT 1 FROM txn_delivery WHERE consumer_id=? AND delivery_seq<? AND status!=? LIMIT 1',
                (consumer, delivery['delivery_seq'], 'acked')).fetchone()
            if earlier:
                raise ValueError('ack_out_of_order')
            db.execute(
                "UPDATE txn_delivery SET status='acked',acked_at=CURRENT_TIMESTAMP WHERE id=?",
                (delivery['id'],))
            db.execute(
                '''INSERT INTO txn_feed_cursor(consumer_id,run_id,last_commit_seq,last_txn_id,
                      last_delivery_seq)
                   VALUES (?,?,?,?,?)
                   ON CONFLICT(consumer_id) DO UPDATE SET
                     run_id=excluded.run_id,last_commit_seq=excluded.last_commit_seq,
                     last_txn_id=excluded.last_txn_id,
                     last_delivery_seq=excluded.last_delivery_seq''',
                (consumer, run_id, delivery['commit_seq'], txn_id, delivery['delivery_seq']))
            gap = db.execute(
                'SELECT * FROM txn_scope_gap WHERE consumer_id=? AND run_id=? AND txn_id=?',
                (consumer, run_id, txn_id)).fetchone()
            if gap and gap['status'] == 'open':
                db.execute(
                    "UPDATE txn_scope_gap SET status='closed',closed_at=CURRENT_TIMESTAMP WHERE id=?",
                    (gap['id'],))
        return {'consumer_id': consumer, 'action': 'acked', 'delivery_id': delivery['id'],
                'delivery_seq': delivery['delivery_seq'], 'txn_id': txn_id,
                'commit_seq': delivery['commit_seq']}
    finally:
        db.close()


def feed_report(db_path, consumer=None):
    db = connect(db_path)
    try:
        subs = [dict(r) for r in db.execute(
            'SELECT * FROM txn_subscription WHERE ? IS NOT NULL OR 1=1 ORDER BY consumer_id',
            (consumer,)).fetchall()]
        if consumer and not subs:
            raise ValueError('unknown_subscription')
        report = []
        for sub in subs:
            cid = sub['consumer_id']
            sub['authorized_keys'] = [r['key'] for r in db.execute(
                'SELECT key FROM subscription_key WHERE consumer_id=? ORDER BY key', (cid,))]
            sub['cursor'] = db.execute(
                'SELECT * FROM txn_feed_cursor WHERE consumer_id=?', (cid,)).fetchone()
            sub['cursor'] = dict(sub['cursor']) if sub['cursor'] else None
            sub['deliveries'] = [dict(r) for r in db.execute(
                'SELECT id,run_id,txn_id,commit_seq,delivery_seq,status,sink_path,payload_sha,published_at,acked_at FROM txn_delivery WHERE consumer_id=? ORDER BY delivery_seq',
                (cid,))]
            sub['scope_gaps'] = [dict(r) for r in db.execute(
                'SELECT * FROM txn_scope_gap WHERE consumer_id=? ORDER BY id', (cid,))]
            report.append(sub)
        return report
    finally:
        db.close()
