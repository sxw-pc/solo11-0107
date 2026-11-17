import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from cdcverdict.feed import (register_subscription, publish_feed, recover_feed,
                             acknowledge_feed, feed_report)
from cdcverdict.ingest import snapshot, ingest
from cdcverdict.rules import replay
from cdcverdict.store import connect


def write_jsonl(path, rows):
    path.write_text('\n'.join(json.dumps(x, sort_keys=True) for x in rows), encoding='utf-8')
    return path


def seed(tmp_path, watermark=10):
    """Fixed scenario: T1 moves 100 from A to B, T2 updates C, T3 excluded, T4 fails."""
    db = tmp_path / 'data.db'
    write_jsonl(tmp_path / 'base.jsonl', [
        {'key': 'A', 'value': {'n': 100}},
        {'key': 'B', 'value': {'n': 0}},
        {'key': 'C', 'value': {'n': 1}},
    ])
    snapshot(tmp_path / 'base.jsonl', db, 0)
    events = [
        {'kind': 'begin', 'txn': 'T1', 'seq': 1},
        {'kind': 'change', 'txn': 'T1', 'seq': 2, 'key': 'A', 'action': 'update',
         'before': {'n': 100}, 'after': {'n': 0}},
        {'kind': 'change', 'txn': 'T1', 'seq': 3, 'key': 'B', 'action': 'update',
         'before': {'n': 0}, 'after': {'n': 100}},
        {'kind': 'commit', 'txn': 'T1', 'seq': 4},
        {'kind': 'begin', 'txn': 'T2', 'seq': 5},
        {'kind': 'change', 'txn': 'T2', 'seq': 6, 'key': 'C', 'action': 'update',
         'before': {'n': 1}, 'after': {'n': 2}},
        {'kind': 'commit', 'txn': 'T2', 'seq': 7},
        # T4 fails inside the watermark: an unsatisfied before condition rejects the
        # whole txn, so it stays structurally valid but gets no run-scoped evidence.
        {'kind': 'begin', 'txn': 'T4', 'seq': 8},
        {'kind': 'change', 'txn': 'T4', 'seq': 9, 'key': 'A', 'action': 'update',
         'before': {'n': 999}, 'after': {'n': 7}},
        {'kind': 'commit', 'txn': 'T4', 'seq': 10},
        # T3 commits beyond the watermark -> run_exclusion, never feedable.
        {'kind': 'begin', 'txn': 'T3', 'seq': 11},
        {'kind': 'change', 'txn': 'T3', 'seq': 12, 'key': 'C', 'action': 'update',
         'before': {'n': 2}, 'after': {'n': 3}},
        {'kind': 'commit', 'txn': 'T3', 'seq': 13},
    ]
    write_jsonl(tmp_path / 'events.jsonl', events)
    ingest(tmp_path / 'events.jsonl', db)
    run, out, excluded = replay(db, watermark)
    return db, run, out, excluded


def subscribe(tmp_path, db, rows):
    path = tmp_path / f'sub-{rows[0]["id"]}.json'
    path.write_text(json.dumps(rows), encoding='utf-8')
    return register_subscription(db, path)


def read_sink(result):
    lines = Path(result['sink_path']).read_text(encoding='utf-8').splitlines()
    assert len(lines) == 1
    return json.loads(lines[0])


def test_run_evidence_anchors_feedable_transaction_set(tmp_path):
    db, run, out, excluded = seed(tmp_path)
    assert excluded == ['T3']
    con = connect(db)
    # Only T1 and T2 carry run-scoped evidence; failed T4 has none.
    evidenced = {r['txn_id'] for r in con.execute(
        '''SELECT DISTINCT ce.txn_id FROM evidence e
           JOIN change_event ce ON ce.id=e.event_id WHERE e.run_id=?''', (run,))}
    assert evidenced == {'T1', 'T2'}
    # T4 is structurally complete (so txn.valid=1) but replay rejected it;
    # evidence-anchored derivation must not treat the valid flag as sufficient.
    assert con.execute("SELECT valid FROM txn WHERE id='T4'").fetchone()[0] == 1
    assert [r[0] for r in con.execute(
        'SELECT txn_id FROM run_exclusion WHERE run_id=?', (run,))] == ['T3']
    # T4's failure poisons A (it stays 无法判定); C was only touched by accepted txns.
    assert out['A'] == '无法判定' and out['C'] == '存在'


def test_ledger_receives_complete_two_leg_envelope(tmp_path):
    db, run, _, _ = seed(tmp_path)
    subscribe(tmp_path, db, [{'id': 'ledger', 'kind': 'ledger', 'purpose': 'reconciliation',
                              'keys': ['A', 'B', 'C'], 'sink_dir': str(tmp_path / 'sink-ledger')}])
    r1 = publish_feed(db, 'ledger', run)
    assert r1['action'] == 'published' and r1['txn_id'] == 'T1' and r1['complete'] is True
    env = read_sink(r1)
    assert env['complete'] is True and env['delivery_kind'] == 'ledger'
    assert env['member_count'] == 2
    kinds = [e['kind'] for e in env['events']]
    assert kinds == ['begin', 'change', 'change', 'commit']
    assert {e['key'] for e in env['events'] if e['kind'] == 'change'} == {'A', 'B'}
    assert env['run_id'] == run and env['txn_id'] == 'T1' and env['commit_seq'] == 4
    # Cursor must not move before the external ACK.
    assert connect(db).execute(
        'SELECT 1 FROM txn_feed_cursor WHERE consumer_id=?', ('ledger',)).fetchone() is None
    ack = acknowledge_feed(db, 'ledger', run, 'T1')
    assert ack['action'] == 'acked'
    cursor = connect(db).execute(
        'SELECT * FROM txn_feed_cursor WHERE consumer_id=?', ('ledger',)).fetchone()
    assert cursor['last_txn_id'] == 'T1' and cursor['last_commit_seq'] == 4


def test_key_alert_gets_projection_only_and_cannot_claim_completeness(tmp_path):
    db, run, _, _ = seed(tmp_path)
    subscribe(tmp_path, db, [{'id': 'key_alert', 'kind': 'key_alert', 'purpose': 'fraud_alert',
                              'keys': ['A'], 'sink_dir': str(tmp_path / 'sink-alert')}])
    r1 = publish_feed(db, 'key_alert', run)
    assert r1['action'] == 'published' and r1['txn_id'] == 'T1' and r1['complete'] is False
    env = read_sink(r1)
    assert env['complete'] is False and env['delivery_kind'] == 'key_alert'
    assert env['member_count'] == 2 and env['projected_count'] == 1 and env['hidden_count'] == 1
    assert [e['key'] for e in env['events']] == ['A']
    assert env['txn_id'] == 'T1' and env['commit_seq'] == 4
    # The B leg must not leak into the alert consumer's bytes.
    raw = Path(r1['sink_path']).read_text(encoding='utf-8')
    assert all(e['key'] != 'B' for e in env['events'])
    assert 'B' not in raw
    # The projected A leg still carries its own before/after values legitimately.
    assert env['events'][0]['after'] == {'n': 0}
    acknowledge_feed(db, 'key_alert', run, 'T1')
    r2 = publish_feed(db, 'key_alert', run)
    # T2 only touches C, which the alert scope does not cover -> skipped, feed catches up.
    assert r2['action'] == 'caught_up'


def test_ledger_scope_gap_blocks_instead_of_partial_delivery(tmp_path):
    db, run, _, _ = seed(tmp_path)
    subscribe(tmp_path, db, [{'id': 'partial_ledger', 'kind': 'ledger', 'purpose': 'reconciliation',
                              'keys': ['A'], 'sink_dir': str(tmp_path / 'sink-gap')}])
    r1 = publish_feed(db, 'partial_ledger', run)
    assert r1['action'] == 'scope_gap' and r1['txn_id'] == 'T1'
    assert r1['missing_keys'] == ['B']
    con = connect(db)
    gap = con.execute(
        "SELECT * FROM txn_scope_gap WHERE consumer_id='partial_ledger'").fetchone()
    assert gap['status'] == 'open' and json.loads(gap['missing_keys']) == ['B']
    # No delivery row, no file, no cursor: nothing that can impersonate a full txn.
    assert con.execute(
        "SELECT COUNT(*) FROM txn_delivery WHERE consumer_id='partial_ledger'").fetchone()[0] == 0
    assert not list((tmp_path / 'sink-gap').glob('*.jsonl'))
    # Repeated feed keeps blocking on the same gap rather than jumping to T2.
    assert publish_feed(db, 'partial_ledger', run)['action'] == 'scope_gap'
    # Expanding the authorization scope clears the blocker on the next feed.
    subscribe(tmp_path, db, [{'id': 'partial_ledger', 'kind': 'ledger', 'purpose': 'reconciliation',
                              'keys': ['A', 'B', 'C'], 'sink_dir': str(tmp_path / 'sink-gap')}])
    r2 = publish_feed(db, 'partial_ledger', run)
    assert r2['action'] == 'published' and r2['txn_id'] == 'T1'
    env = read_sink(r2)
    assert env['complete'] is True and env['member_count'] == 2
    acknowledge_feed(db, 'partial_ledger', run, 'T1')
    assert connect(db).execute(
        "SELECT status FROM txn_scope_gap WHERE consumer_id='partial_ledger'").fetchone()[0] == 'closed'


def test_subscription_contract_and_scope_shrink_are_rejected(tmp_path):
    db, run, _, _ = seed(tmp_path)
    subscribe(tmp_path, db, [{'id': 'ledger', 'kind': 'ledger', 'purpose': 'reconciliation',
                              'keys': ['A', 'B'], 'sink_dir': str(tmp_path / 's')}])
    with pytest.raises(ValueError, match='subscription_contract_changed'):
        subscribe(tmp_path, db, [{'id': 'ledger', 'kind': 'key_alert', 'purpose': 'reconciliation',
                                  'keys': ['A', 'B'], 'sink_dir': str(tmp_path / 's')}])
    with pytest.raises(ValueError, match='subscription_scope_shrunk'):
        subscribe(tmp_path, db, [{'id': 'ledger', 'kind': 'ledger', 'purpose': 'reconciliation',
                                  'keys': ['A'], 'sink_dir': str(tmp_path / 's')}])
    with pytest.raises(ValueError, match='invalid_subscription_kind'):
        subscribe(tmp_path, db, [{'id': 'x', 'kind': 'strict', 'purpose': 'p', 'keys': ['A']}])


def test_excluded_and_failed_transactions_are_never_delivered(tmp_path):
    db, run, _, _ = seed(tmp_path)
    subscribe(tmp_path, db, [{'id': 'ledger', 'kind': 'ledger', 'purpose': 'reconciliation',
                              'keys': ['A', 'B', 'C'], 'sink_dir': str(tmp_path / 'sink-ledger')}])
    r1 = publish_feed(db, 'ledger', run)
    acknowledge_feed(db, 'ledger', run, r1['txn_id'])
    r2 = publish_feed(db, 'ledger', run)
    assert r2['txn_id'] == 'T2'
    acknowledge_feed(db, 'ledger', run, 'T2')
    assert publish_feed(db, 'ledger', run)['action'] == 'caught_up'
    delivered = {r['txn_id'] for r in connect(db).execute(
        'SELECT txn_id FROM txn_delivery')}
    assert delivered == {'T1', 'T2'}
    # A replay at the later watermark is a different run; its run-scoped feed
    # starts independently even though the consumer cursor already advanced.
    run2, _, excluded2 = replay(db, 13)
    assert not excluded2
    r3 = publish_feed(db, 'ledger', run2)
    assert r3['txn_id'] == 'T1' and r3['run_id'] == run2
    # T3 is now inside the watermark and evidenced, so it becomes feedable in run2.
    run2_txns = {r['txn_id'] for r in connect(db).execute(
        '''SELECT DISTINCT ce.txn_id FROM evidence e
           JOIN change_event ce ON ce.id=e.event_id WHERE e.run_id=?''', (run2,))}
    assert run2_txns == {'T1', 'T2', 'T3'}
    # The failed T4 still has no evidence and never appears in either run's feed.
    delivered = {r['txn_id'] for r in connect(db).execute('SELECT txn_id FROM txn_delivery')}
    assert 'T4' not in delivered


def test_crash_between_publish_and_ack_republishes_identical_bytes(tmp_path):
    db, run, _, _ = seed(tmp_path)
    sink = tmp_path / 'sink-ledger'
    subscribe(tmp_path, db, [{'id': 'ledger', 'kind': 'ledger', 'purpose': 'reconciliation',
                              'keys': ['A', 'B', 'C'], 'sink_dir': str(sink)}])
    r1 = publish_feed(db, 'ledger', run)
    first_bytes = Path(r1['sink_path']).read_bytes()
    first_path = r1['sink_path']
    # Simulate crash: file externally removed while the local ACK never happened.
    Path(first_path).unlink()
    rec = recover_feed(db, 'ledger')
    assert rec['action'] == 'recovered'
    assert rec['delivery_id'] == r1['delivery_id']
    assert rec['payload_sha'] == r1['payload_sha']
    assert Path(rec['sink_path']).read_bytes() == first_bytes
    # Same idempotency key -> still one logical delivery, no second row.
    con = connect(db)
    assert con.execute(
        "SELECT COUNT(*) FROM txn_delivery WHERE consumer_id='ledger'").fetchone()[0] == 1
    assert con.execute(
        "SELECT status FROM txn_delivery WHERE id=?", (r1['delivery_id'],)).fetchone()[0] == 'published'
    # recover is itself idempotent.
    rec2 = recover_feed(db, 'ledger')
    assert rec2['delivery_id'] == r1['delivery_id']
    assert Path(rec2['sink_path']).read_bytes() == first_bytes
    # A new feed cannot skip the un-ACKed head.
    assert publish_feed(db, 'ledger', run)['action'] == 'blocked_pending_ack'
    acknowledge_feed(db, 'ledger', run, 'T1')
    assert recover_feed(db, 'ledger')['action'] == 'nothing_to_recover'
    assert publish_feed(db, 'ledger', run)['txn_id'] == 'T2'


def test_crash_before_file_publish_is_recovered_from_pending_outbox(tmp_path):
    db, run, _, _ = seed(tmp_path)
    sink = tmp_path / 'sink-ledger'
    subscribe(tmp_path, db, [{'id': 'ledger', 'kind': 'ledger', 'purpose': 'reconciliation',
                              'keys': ['A', 'B', 'C'], 'sink_dir': str(sink)}])
    # Outbox row is durable; force the status back to 'pending' as if the rename never happened.
    r1 = publish_feed(db, 'ledger', run)
    with connect(db) as con:
        con.execute("UPDATE txn_delivery SET status='pending',sink_path=NULL WHERE id=?",
                    (r1['delivery_id'],))
    Path(r1['sink_path']).unlink()
    rec = recover_feed(db, 'ledger')
    assert rec['action'] == 'recovered' and rec['delivery_id'] == r1['delivery_id']
    assert Path(rec['sink_path']).read_bytes()
    assert connect(db).execute(
        "SELECT status FROM txn_delivery WHERE id=?", (r1['delivery_id'],)).fetchone()[0] == 'published'


def test_per_consumer_cursors_and_ordering_are_independent(tmp_path):
    db, run, _, _ = seed(tmp_path)
    subscribe(tmp_path, db, [
        {'id': 'ledger', 'kind': 'ledger', 'purpose': 'reconciliation',
         'keys': ['A', 'B', 'C'], 'sink_dir': str(tmp_path / 'l')},
        {'id': 'key_alert', 'kind': 'key_alert', 'purpose': 'fraud_alert',
         'keys': ['A'], 'sink_dir': str(tmp_path / 'a')},
    ])
    lr = publish_feed(db, 'ledger', run)
    ar = publish_feed(db, 'key_alert', run)
    assert lr['txn_id'] == ar['txn_id'] == 'T1'
    # Alert ACKs immediately; the ledger stays blocked on its own un-ACKed head.
    acknowledge_feed(db, 'key_alert', run, 'T1')
    assert publish_feed(db, 'key_alert', run)['action'] == 'caught_up'
    assert publish_feed(db, 'ledger', run)['action'] == 'blocked_pending_ack'
    # Each consumer has its own delivery sequence starting at 1.
    assert lr['delivery_seq'] == ar['delivery_seq'] == 1
    report = {row['consumer_id']: row for row in feed_report(db)}
    assert report['key_alert']['cursor']['last_txn_id'] == 'T1'
    assert report['ledger']['cursor'] is None
    assert len(report['ledger']['deliveries']) == 1
    # Out-of-order / unknown ACKs are rejected.
    with pytest.raises(ValueError, match='unknown_delivery'):
        acknowledge_feed(db, 'ledger', run, 'nope')
    acknowledge_feed(db, 'ledger', run, 'T1')
    t2 = publish_feed(db, 'ledger', run)
    assert t2['txn_id'] == 'T2'
    # Re-ACK of an already acknowledged delivery is idempotent.
    again = acknowledge_feed(db, 'ledger', run, 'T1')
    assert again['action'] == 'already_acked'


def test_idempotency_key_prevents_second_logical_delivery(tmp_path):
    db, run, _, _ = seed(tmp_path)
    subscribe(tmp_path, db, [{'id': 'ledger', 'kind': 'ledger', 'purpose': 'reconciliation',
                              'keys': ['A', 'B', 'C'], 'sink_dir': str(tmp_path / 'l')}])
    r1 = publish_feed(db, 'ledger', run)
    # Direct duplicate insert with the same consumer/run/txn key must be rejected by the schema.
    with pytest.raises(Exception):
        with connect(db) as con:
            con.execute(
                'INSERT INTO txn_delivery(consumer_id,run_id,txn_id,commit_seq,delivery_seq,envelope_json,payload_blob,payload_sha) VALUES (?,?,?,?,?,?,?,?)',
                ('ledger', run, 'T1', 4, 99, '{}', b'x', 'h'))
    acknowledge_feed(db, 'ledger', run, 'T1')
    # After ACK, re-feeding the same run/txn does not create another delivery.
    assert publish_feed(db, 'ledger', run)['action'] in {'caught_up', 'published'}
    assert connect(db).execute(
        "SELECT COUNT(*) FROM txn_delivery WHERE consumer_id='ledger' AND txn_id='T1'").fetchone()[0] == 1


def test_cli_feed_recover_ack_roundtrip(tmp_path):
    db, run, _, _ = seed(tmp_path)
    sub = tmp_path / 'sub.json'
    sub.write_text(json.dumps([{'id': 'ledger', 'kind': 'ledger', 'purpose': 'reconciliation',
                                'keys': ['A', 'B', 'C'], 'sink_dir': str(tmp_path / 'l')}]),
                   encoding='utf-8')
    base = [sys.executable, '-m', 'cdcverdict']
    env = {**os.environ, 'PYTHONPATH': str(Path(__file__).parents[1] / 'src')}
    def run_cli(*args):
        proc = subprocess.run(base + list(args), cwd=Path(__file__).parents[1],
                              capture_output=True, text=True, check=True, env=env)
        return json.loads(proc.stdout)
    assert run_cli('feed-subscribe', str(sub), '--db', str(db)) == ['ledger']
    published = run_cli('feed', '--db', str(db), '--consumer', 'ledger', '--run', str(run))
    assert published['action'] == 'published'
    Path(published['sink_path']).unlink()
    recovered = run_cli('recover', '--db', str(db), '--consumer', 'ledger')
    assert recovered['action'] == 'recovered'
    acked = run_cli('ack', '--db', str(db), '--consumer', 'ledger', '--run', str(run),
                    '--txn', 'T1')
    assert acked['action'] == 'acked'
    report = run_cli('feed-report', '--db', str(db), '--consumer', 'ledger')
    assert report[0]['cursor']['last_txn_id'] == 'T1'
    assert report[0]['deliveries'][0]['status'] == 'acked'
