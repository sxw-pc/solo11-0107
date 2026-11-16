"""Copy-on-write repair of a divergent target against a frozen replay verdict.

Decisions are explicit per business key. A repair creates a target snapshot rather
than changing the audited input, so a later audit and consumer acknowledgement
must refer to the new snapshot.

A frozen divergent audit may be split into several scoped repair plans.  A plan
only speaks for the keys listed in its manifest; every other audited difference
stays unresolved.  Plans are bound to the audit's frozen target digest, so a
target change invalidates old plans instead of silently rebasing them.  Explicit
merging folds non-overlapping plans into one new immutable target snapshot;
overlapping decisions with the same outcome are de-duplicated by rule, while
conflicting outcomes stay recorded and block the merge.
"""
import hashlib
import json
from pathlib import Path

from .cutover import _target_digest
from .store import connect

# Outcomes that describe an actual per-key settlement.  ``unresolved`` and
# ``conflict`` are never written to a target snapshot.
RESOLVED = {'restore_replay', 'accept_target'}
_DECISIONS = {'restore_replay', 'accept_target', 'defer'}
# Rule used when several plans decide the same key the same way: the decision is
# kept once and every contributing plan remains as evidence.
AGREEMENT_RULE = 'same_outcome_single_value'
_CANONICAL = ('restore_replay', 'accept_target', 'defer')


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _audit_digest(audit):
    """Digest of the frozen audit facts a repair plan is bound to."""
    return _sha(json.dumps({'audit_id': audit['id'], 'run_id': audit['run_id'],
                            'target_id': audit['target_id'], 'target_digest': audit['target_digest'],
                            'status': audit['status'], 'reason': audit['reason']},
                           sort_keys=True).encode())


def _snapshot_digest(db, target_id):
    return _target_digest(db, target_id)


def _rows(db, target_id):
    return {row['key']: row['value'] for row in db.execute(
        'SELECT key,value FROM target_row WHERE snapshot_id=?', (target_id,))}


def _write_snapshot(db, watermark, rows, existing_error):
    """Insert an immutable target snapshot; reuse an identical existing one."""
    raw = ('\n'.join(json.dumps({'key': key, 'value': json.loads(value)}, sort_keys=True, ensure_ascii=False)
                     for key, value in sorted(rows.items()))).encode('utf-8')
    digest = _sha(raw)
    existing = db.execute('SELECT id,watermark FROM target_snapshot WHERE source_sha=?', (digest,)).fetchone()
    if existing:
        if existing['watermark'] != watermark:
            raise ValueError(existing_error)
        actual = _rows(db, existing['id'])
        if actual != rows:
            raise ValueError(existing_error)
        return existing['id'], digest
    output = db.execute('INSERT INTO target_snapshot(watermark,source_sha,raw) VALUES (?,?,?)',
                        (watermark, digest, raw)).lastrowid
    db.executemany('INSERT INTO target_row VALUES (?,?,?)',
                   [(output, key, value) for key, value in sorted(rows.items())])
    return output, digest


def _load_frozen_audit(db, audit_id):
    audit = db.execute('SELECT * FROM cutover_audit WHERE id=?', (audit_id,)).fetchone()
    if not audit:
        raise ValueError('unknown_audit')
    if audit['status'] != 'divergent':
        raise ValueError('audit_not_divergent')
    if _snapshot_digest(db, audit['target_id']) != audit['target_digest']:
        raise ValueError('audited_target_changed')
    differences = {row['key']: row for row in db.execute(
        'SELECT * FROM cutover_difference WHERE audit_id=?', (audit_id,))}
    if not differences:
        raise ValueError('audit_has_no_differences')
    return audit, differences


def plan_repair(path, audit_id, manifest_file, scope=None):
    """Plan a repair branch over explicit keys of one frozen audit.

    ``scope='full'`` keeps the original contract that a manifest must cover every
    audited difference; ``scope='partial'`` (or ``None`` when the manifest is
    itself a strict subset) plans only the declared keys.  Undeclared keys stay
    unresolved and are never treated as accepted or deferred.
    """
    if scope not in {None, 'full', 'partial'}:
        raise ValueError('invalid_repair_scope')
    raw = Path(manifest_file).read_bytes()
    choices = json.loads(raw.decode('utf-8'))
    if not isinstance(choices, list) or not choices:
        raise ValueError('empty_repair_manifest')
    keys = [str(row.get('key')) for row in choices if isinstance(row, dict)]
    if len(keys) != len(choices) or len(set(keys)) != len(keys) or any(not key for key in keys):
        raise ValueError('invalid_repair_choice')
    for row in choices:
        if (row.get('decision') not in _DECISIONS or not str(row.get('reason', '')).strip()
                or not isinstance(row.get('reason'), str)):
            raise ValueError('invalid_repair_choice')
    db = connect(path)
    try:
        with db:
            audit, differences = _load_frozen_audit(db, audit_id)
            if not set(keys) <= set(differences):
                raise ValueError('repair_key_not_audited')
            if scope == 'full' and set(keys) != set(differences):
                raise ValueError('repair_manifest_incomplete')
            digest = _sha(raw)
            previous = db.execute('SELECT id FROM repair_plan WHERE audit_id=? AND manifest_sha=?',
                                  (audit_id, digest)).fetchone()
            if previous:
                return previous['id']
            audit_digest = _audit_digest(audit)
            base_digest = _snapshot_digest(db, audit['target_id'])
            scope_kind = 'full' if set(keys) == set(differences) else 'partial'
            plan = db.execute(
                'INSERT INTO repair_plan(audit_id,manifest_sha,raw,status,scope_kind,audit_digest,'
                'base_target_id,base_target_digest) VALUES (?,?,?,?,?,?,?,?)',
                (audit_id, digest, raw, 'planned', scope_kind, audit_digest,
                 audit['target_id'], base_digest)).lastrowid
            db.executemany('INSERT INTO repair_choice VALUES (?,?,?,?,?,?)',
                           [(plan, row['key'], row['decision'], row['reason'],
                             differences[row['key']]['replay_value'], differences[row['key']]['target_value'])
                            for row in choices])
            return plan
    finally:
        db.close()


def _require_plan_base(db, plan):
    """Re-read the plan's frozen base; never silently rebase it onto new input."""
    audit = db.execute('SELECT * FROM cutover_audit WHERE id=?', (plan['audit_id'],)).fetchone()
    if not audit:
        raise ValueError('unknown_audit')
    if plan['audit_digest'] and plan['audit_digest'] != _audit_digest(audit):
        raise ValueError('audit_digest_changed')
    if plan['base_target_id'] is not None:
        target_digest = _snapshot_digest(db, plan['base_target_id'])
        if plan['base_target_digest'] and target_digest != plan['base_target_digest']:
            raise ValueError('audited_target_changed')
        return plan['base_target_id'], target_digest
    if _snapshot_digest(db, audit['target_id']) != audit['target_digest']:
        raise ValueError('audited_target_changed')
    return audit['target_id'], audit['target_digest']


def apply_repair(path, plan_id):
    db = connect(path)
    try:
        with db:
            plan = db.execute('SELECT * FROM repair_plan WHERE id=?', (plan_id,)).fetchone()
            if not plan:
                raise ValueError('unknown_repair_plan')
            if plan['status'] == 'applied':
                return plan['output_target_id'], plan['result']
            base_id, base_digest = _require_plan_base(db, plan)
            target = db.execute('SELECT * FROM target_snapshot WHERE id=?', (base_id,)).fetchone()
            rows = _rows(db, base_id)
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
                output, _ = _write_snapshot(db, target['watermark'], rows,
                                            'repair_snapshot_watermark_conflict')
            result = 'repaired' if restored and not accepted and not deferred else 'partial' if restored else 'accepted_deviation' if accepted else 'deferred'
            db.execute("UPDATE repair_plan SET status='applied',output_target_id=?,result=? WHERE id=?",
                       (output, result, plan_id))
            return output, result
    finally:
        db.close()


def _choice_value(decision, replay_value, target_value):
    """Raw target-row value a choice contributes; ``None`` means the row is absent."""
    if decision == 'restore_replay':
        return replay_value
    if decision == 'accept_target':
        return target_value
    return None


def _contribution(decision, replay_value, target_value):
    """Comparable value of a choice: raw JSON text, or an absence sentinel."""
    value = _choice_value(decision, replay_value, target_value)
    # SQL NULL is stored only for "no row"; a JSON null is the text 'null'.
    return ('absent',) if value is None else ('value', value)


def plan_repair_merge(path, plan_ids):
    """Merge applied scoped plans of one frozen audit into a new target snapshot.

    The operation is retryable: the same plan set maps to one merge record and
    re-invoking it returns the same snapshot, even after later plan changes.
    """
    ids = [int(plan_id) for plan_id in plan_ids]
    if not ids or len(set(ids)) != len(ids):
        raise ValueError('invalid_merge_scope')
    set_sha = _sha(json.dumps(sorted(ids)).encode('utf-8'))
    db = connect(path)
    try:
        with db:
            previous = db.execute('SELECT * FROM repair_merge WHERE plan_set_sha=?', (set_sha,)).fetchone()
            if previous:
                # Idempotent retry, but only while the frozen facts still match:
                # a changed target or audit must never be silently re-used.
                if _snapshot_digest(db, previous['base_target_id']) != previous['base_target_digest']:
                    raise ValueError('audited_target_changed')
                audit = db.execute('SELECT * FROM cutover_audit WHERE id=?',
                                   (previous['audit_id'],)).fetchone()
                if not audit or _audit_digest(audit) != previous['audit_digest']:
                    raise ValueError('audit_digest_changed')
                return previous['id'], previous['output_target_id'], previous['status']

            plans = []
            for plan_id in sorted(ids):
                plan = db.execute('SELECT * FROM repair_plan WHERE id=?', (plan_id,)).fetchone()
                if not plan:
                    raise ValueError('unknown_repair_plan')
                # A stale plan is reported as stale even if it was never
                # applied: the merge must not silently rebase it.
                audit = db.execute('SELECT * FROM cutover_audit WHERE id=?', (plan['audit_id'],)).fetchone()
                if not audit:
                    raise ValueError('unknown_audit')
                if plan['audit_digest'] and plan['audit_digest'] != _audit_digest(audit):
                    raise ValueError('audit_digest_changed')
                if _plan_base_digest(db, plan) != audit['target_digest']:
                    raise ValueError('audited_target_changed')
                if plan['status'] != 'applied':
                    raise ValueError('repair_plan_not_applied')
                plans.append(plan)
            audits = {plan['audit_id'] for plan in plans}
            if len(audits) != 1:
                raise ValueError('repair_merge_audit_mismatch')
            audit, differences = _load_frozen_audit(db, plans[0]['audit_id'])

            # Every branch must be pinned to the same original target snapshot.
            base_ids = {plan['base_target_id'] or audit['target_id'] for plan in plans}
            base_digests = {plan['base_target_digest'] or audit['target_digest'] for plan in plans}
            if len(base_ids) != 1 or len(base_digests) != 1:
                raise ValueError('repair_merge_base_mismatch')
            base_id = base_ids.pop()
            base_digest = base_digests.pop()
            if base_id != audit['target_id'] or base_digest != audit['target_digest']:
                raise ValueError('repair_merge_base_mismatch')
            if _snapshot_digest(db, base_id) != audit['target_digest']:
                raise ValueError('audited_target_changed')
            for plan in plans:
                if _plan_base_digest(db, plan) != audit['target_digest']:
                    raise ValueError('audited_target_changed')

            contributions = {}
            reasons = {}
            for plan in plans:
                for choice in db.execute('SELECT * FROM repair_choice WHERE plan_id=? ORDER BY key', (plan['id'],)):
                    key = choice['key']
                    value = _choice_value(choice['decision'], choice['replay_value'], choice['target_value'])
                    comparable = _contribution(choice['decision'], choice['replay_value'],
                                               choice['target_value'])
                    contributions.setdefault(key, []).append(
                        (plan['id'], choice['decision'], value, comparable))
                    reasons[(key, plan['id'])] = choice['reason']

            base_rows = _rows(db, base_id)
            rows = dict(base_rows)
            outcomes = {}
            conflicts = []
            for key in sorted(contributions):
                choices = contributions[key]
                decided = [item for item in choices if item[1] in RESOLVED]
                # A deferral does not assert a value, so it never conflicts with
                # a concrete decision; two concrete decisions must agree exactly.
                if len({comparable for *_, comparable in decided}) > 1:
                    outcomes[key] = ('conflict', None, None, None, choices)
                    conflicts.append(key)
                    continue
                if not decided:
                    # Only deferrals speak for this key: no new target value.
                    outcomes[key] = ('deferred', 'defer', None, None, choices)
                    continue
                primary = sorted(decided, key=_canonical_key)[0]
                _, decision, value, _ = primary
                if decision == 'restore_replay':
                    if value is None:
                        rows.pop(key, None)
                    else:
                        rows[key] = value
                else:  # accept_target keeps the original target value
                    value = base_rows.get(key)
                    if key in base_rows:
                        rows[key] = base_rows[key]
                    else:
                        rows.pop(key, None)
                agreed = len(decided) > 1
                outcomes[key] = ('agreed' if agreed else 'decided', decision, value, primary[0], choices)

            audit_digest = _audit_digest(audit)
            status = 'conflict' if conflicts else 'merged'
            output_target_id = None
            output_digest = None
            if status == 'merged' and rows != base_rows:
                output_target_id, _ = _write_snapshot(
                    db, db.execute('SELECT watermark FROM target_snapshot WHERE id=?', (base_id,)).fetchone()[0],
                    rows, 'repair_snapshot_watermark_conflict')
            elif status == 'merged':
                # No plan changed a target row (all accepts).  Reuse the base
                # snapshot, but keep ``resolved=False`` semantics in reports.
                output_target_id = base_id
            if output_target_id is not None:
                # Canonical content digest, the same value an audit records.
                output_digest = _snapshot_digest(db, output_target_id)

            reason = (','.join(conflicts) if conflicts else
                      'no_new_target_rows' if status == 'merged' and not conflicts and rows == base_rows else
                      'branches_merged')
            merge_id = db.execute(
                'INSERT INTO repair_merge(audit_id,run_id,base_target_id,base_target_digest,audit_digest,'
                'plan_set_sha,status,reason,output_target_id,output_digest) VALUES (?,?,?,?,?,?,?,?,?,?)',
                (audit['id'], audit['run_id'], base_id, base_digest, audit_digest, set_sha, status, reason,
                 output_target_id, output_digest)).lastrowid
            db.executemany('INSERT INTO repair_merge_member VALUES (?,?)',
                           [(merge_id, plan['id']) for plan in plans])
            for key, (outcome, decision, value, primary_plan_id, choices) in outcomes.items():
                db.execute('INSERT INTO repair_merge_key(merge_id,key,outcome,decision,value,replay_value,'
                           'target_value,primary_plan_id,source_count) VALUES (?,?,?,?,?,?,?,?,?)',
                           (merge_id, key, outcome, decision, value,
                            differences[key]['replay_value'], differences[key]['target_value'],
                            primary_plan_id, len(choices)))
                db.executemany('INSERT INTO repair_merge_source VALUES (?,?,?,?,?,?)',
                               [(merge_id, key, plan_id, choice_decision,
                                 choice_value,
                                 reasons[(key, plan_id)])
                                for plan_id, choice_decision, choice_value, _ in choices])
            return merge_id, output_target_id, status
    finally:
        db.close()


def _plan_base_digest(db, plan):
    """Digest of the plan's frozen base target; never re-read as a new base."""
    if plan['base_target_id'] is not None and plan['base_target_digest'] is not None:
        return plan['base_target_digest']
    audit = db.execute('SELECT target_digest FROM cutover_audit WHERE id=?', (plan['audit_id'],)).fetchone()
    return audit['target_digest'] if audit else None


def _canonical_key(item):
    """Deterministic primary-source choice: canonical order, then plan id."""
    plan_id, decision, _, _ = item
    return (_CANONICAL.index(decision), plan_id)


def _decode(value):
    return json.loads(value) if value is not None else None


def repair_merge_report(path, merge_id):
    db = connect(path)
    try:
        merge = db.execute('SELECT * FROM repair_merge WHERE id=?', (merge_id,)).fetchone()
        if not merge:
            raise ValueError('unknown_repair_merge')
        members = [row['plan_id'] for row in db.execute(
            'SELECT plan_id FROM repair_merge_member WHERE merge_id=? ORDER BY plan_id', (merge_id,))]
        keys = []
        for row in db.execute('SELECT * FROM repair_merge_key WHERE merge_id=? ORDER BY key', (merge_id,)):
            sources = db.execute(
                'SELECT plan_id,decision,value,reason FROM repair_merge_source WHERE merge_id=? AND key=? '
                'ORDER BY plan_id', (merge_id, row['key'])).fetchall()
            keys.append({'key': row['key'], 'outcome': row['outcome'], 'decision': row['decision'],
                         'value': _decode(row['value']),
                         'replay_value': _decode(row['replay_value']),
                         'target_value': _decode(row['target_value']),
                         'primary_plan_id': row['primary_plan_id'],
                         'sources': [dict(source, value=_decode(source['value'])) for source in sources]})
        # Audited differences no branch spoke for stay unresolved; they are
        # never reported as accepted or deferred.
        covered = {row['key'] for row in keys}
        for difference in db.execute('SELECT * FROM cutover_difference WHERE audit_id=? ORDER BY key',
                                     (merge['audit_id'],)):
            if difference['key'] in covered:
                continue
            keys.append({'key': difference['key'], 'outcome': 'unresolved', 'decision': None,
                         'value': None,
                         'replay_value': _decode(difference['replay_value']),
                         'target_value': _decode(difference['target_value']),
                         'primary_plan_id': None, 'sources': []})
        keys.sort(key=lambda row: row['key'])
        return {'merge': dict(merge), 'plan_ids': members, 'keys': keys,
                'agreement_rule': AGREEMENT_RULE}
    finally:
        db.close()


def repair_report(path, plan_id):
    db = connect(path)
    try:
        plan = db.execute('SELECT id,audit_id,manifest_sha,status,output_target_id,result,scope_kind,'
                          'audit_digest,base_target_id,base_target_digest FROM repair_plan WHERE id=?',
                          (plan_id,)).fetchone()
        if not plan:
            raise ValueError('unknown_repair_plan')
        return {'plan': dict(plan),
                'choices': [dict(row) for row in db.execute('SELECT key,decision,reason,replay_value,target_value FROM repair_choice WHERE plan_id=? ORDER BY key', (plan_id,))]}
    finally:
        db.close()
