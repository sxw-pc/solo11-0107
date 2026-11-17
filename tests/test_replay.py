"""Replay-run evidence semantics used by the transaction consumer feed.

The feed treats run-scoped evidence plus run_exclusion as the authoritative
boundary of feedable transactions; these tests pin that boundary at the replay
layer: whole-transaction commit, watermark exclusion, and rejected transactions
leaving no evidence behind.
"""
import json

from cdcverdict.ingest import snapshot, ingest
from cdcverdict.rules import replay
from cdcverdict.store import connect


def _setup(tmp_path, rows, watermark, base=None):
    db = tmp_path / 'data.db'
    base_file = tmp_path / 'base.jsonl'
    base_file.write_text('\n'.join(json.dumps(x) for x in (base or [{'key': 'A', 'value': 1}])),
                         encoding='utf-8')
    snapshot(base_file, db, 0)
    events_file = tmp_path / 'events.jsonl'
    events_file.write_text('\n'.join(json.dumps(x) for x in rows), encoding='utf-8')
    ingest(events_file, db)
    return db


def _transfer(seq_start=1, txn='T1', bad_before=None):
    return [
        {'kind': 'begin', 'txn': txn, 'seq': seq_start},
        {'kind': 'change', 'txn': txn, 'seq': seq_start + 1, 'key': 'A', 'action': 'update',
         'before': bad_before if bad_before is not None else 1, 'after': 0},
        {'kind': 'change', 'txn': txn, 'seq': seq_start + 2, 'key': 'B', 'action': 'update',
         'before': 0, 'after': 1},
        {'kind': 'commit', 'txn': txn, 'seq': seq_start + 3},
    ]


def test_accepted_transaction_writes_run_scoped_evidence_for_every_change(tmp_path):
    rows = _transfer()
    db = _setup(tmp_path, rows, 4, base=[{'key': 'A', 'value': 1}, {'key': 'B', 'value': 0}])
    run, out, excluded = replay(db, 4)
    assert excluded == [] and out == {'A': '存在', 'B': '存在'}
    con = connect(db)
    evidenced = con.execute(
        'SELECT ce.txn_id,COUNT(*) FROM evidence e JOIN change_event ce ON ce.id=e.event_id '
        'WHERE e.run_id=? GROUP BY ce.txn_id', (run,)).fetchall()
    assert [(r[0], r[1]) for r in evidenced] == [('T1', 2)]
    assert tuple(con.execute("SELECT committed,valid FROM txn WHERE id='T1'").fetchone()) == (1, 1)


def test_watermark_excluded_transaction_has_no_run_evidence_but_is_listed(tmp_path):
    rows = _transfer()
    db = _setup(tmp_path, rows, 2, base=[{'key': 'A', 'value': 1}, {'key': 'B', 'value': 0}])
    run, _out, excluded = replay(db, 2)
    assert excluded == ['T1']
    con = connect(db)
    assert con.execute('SELECT COUNT(*) FROM evidence WHERE run_id=?', (run,)).fetchone()[0] == 0
    assert [r[0] for r in con.execute(
        'SELECT txn_id FROM run_exclusion WHERE run_id=?', (run,))] == ['T1']


def test_rejected_transaction_leaves_no_evidence_and_is_not_excluded(tmp_path):
    rows = _transfer(bad_before=999)
    db = _setup(tmp_path, rows, 4, base=[{'key': 'A', 'value': 1}, {'key': 'B', 'value': 0}])
    run, out, excluded = replay(db, 4)
    assert excluded == []
    assert out == {'A': '无法判定', 'B': '无法判定'}
    con = connect(db)
    assert con.execute('SELECT COUNT(*) FROM evidence WHERE run_id=?', (run,)).fetchone()[0] == 0
    # Structural validity alone does not imply acceptance: replay rejected the txn
    # for a before mismatch, which the evidence-less state records authoritatively.
    assert con.execute("SELECT valid FROM txn WHERE id='T1'").fetchone()[0] == 1
    assert con.execute("SELECT code FROM issue WHERE detail LIKE '%T1%'").fetchone()[0] == 'before_mismatch'


def test_later_run_re_includes_transaction_excluded_by_earlier_watermark(tmp_path):
    rows = _transfer()
    db = _setup(tmp_path, rows, 4, base=[{'key': 'A', 'value': 1}, {'key': 'B', 'value': 0}])
    early, _, early_excluded = replay(db, 2)
    later, _, later_excluded = replay(db, 4)
    assert early_excluded == ['T1'] and later_excluded == []
    con = connect(db)
    assert con.execute('SELECT COUNT(*) FROM evidence WHERE run_id=?', (early,)).fetchone()[0] == 0
    assert con.execute('SELECT COUNT(*) FROM evidence WHERE run_id=?', (later,)).fetchone()[0] == 2
