"""Contract allowance allocation, immutable close and authorized restatement."""

import hashlib
import json
from contextlib import closing

from .period_store import connect
from .period_usage import _read, _time, chosen_contract, source_digest


def _effective_usage(db, tenant, period):
    rows = db.execute('SELECT * FROM period_usage WHERE tenant=? AND period=? ORDER BY id',
                      (tenant, period)).fetchall()
    effective = []
    for row in rows:
        correction = db.execute('SELECT id,corrected_units FROM period_correction WHERE usage_id=? '
                                'ORDER BY revision DESC LIMIT 1', (row['id'],)).fetchone()
        effective.append((row, correction['corrected_units'] if correction else row['units'],
                          correction['id'] if correction else None))
    return effective


def _allocate(contract, effective):
    prices = json.loads(contract['prices_json'])
    reserved = json.loads(contract['reserved_json'])
    priority = json.loads(contract['priority_json'])
    actual = {product: 0 for product in prices}
    for row, units, _ in effective:
        actual[row['product']] += units
    reserved_used = {product: min(actual[product], reserved.get(product, 0)) for product in prices}
    remaining = {product: actual[product] - reserved_used[product] for product in prices}
    shared_left = contract['shared_units']
    shared_used = {product: 0 for product in prices}
    for product in priority:
        granted = min(shared_left, remaining[product])
        shared_used[product] = granted
        shared_left -= granted
    lines = []
    for product in sorted(prices):
        billable = remaining[product] - shared_used[product]
        lines.append({'product': product, 'actual_units': actual[product],
                      'reserved_used': reserved_used[product], 'shared_used': shared_used[product],
                      'billable_units': billable, 'price_cents': prices[product],
                      'charge_cents': billable * prices[product]})
    return lines


def _freeze(db, contract, version, previous_id, actor):
    tenant, period = contract['tenant'], contract['period']
    effective = _effective_usage(db, tenant, period)
    lines = _allocate(contract, effective)
    total = sum(line['charge_cents'] for line in lines)
    digest = source_digest(db, tenant, period)
    close_id = db.execute('INSERT INTO period_close(tenant,period,version,previous_id,contract_id,source_sha,total_cents,actor) '
                          'VALUES(?,?,?,?,?,?,?,?)',
                          (tenant, period, version, previous_id, contract['id'], digest, total, actor)).lastrowid
    for line in lines:
        db.execute('INSERT INTO period_line VALUES(?,?,?,?,?,?,?,?)',
                   (close_id, line['product'], line['actual_units'], line['reserved_used'],
                    line['shared_used'], line['billable_units'], line['price_cents'], line['charge_cents']))
    for row, units, correction_id in effective:
        db.execute('INSERT INTO period_usage_snapshot VALUES(?,?,?,?)',
                   (close_id, row['id'], units, correction_id))
    db.execute('UPDATE period_state SET current_close_id=?,status=? WHERE tenant=? AND period=?',
               (close_id, 'closed', tenant, period))
    return {'close_id': close_id, 'version': version, 'total_cents': total,
            'source_sha': digest, 'lines': lines}


def close_period(db_path, tenant, period, as_of, actor):
    with closing(connect(db_path)) as db, db:
        db.execute('BEGIN IMMEDIATE')
        contract = chosen_contract(db, tenant, period)
        if not contract:
            raise ValueError('period has no signed contract')
        if actor not in json.loads(contract['closers_json']):
            raise ValueError('actor lacks period close authorization')
        if _time(as_of) < _time(contract['ends']):
            raise ValueError('period cannot close before its end')
        state = db.execute('SELECT * FROM period_state WHERE tenant=? AND period=?', (tenant, period)).fetchone()
        if state['current_close_id']:
            raise ValueError('period already closed; request authorized restatement')
        return _freeze(db, contract, 1, None, actor)


def request_restatement(db_path, path):
    item, raw, sha = _read(path)
    ident, tenant, period, requester, reason = (str(item[key]) for key in
                                                ('request_id', 'tenant', 'period', 'requester', 'reason'))
    previous_id = int(item['previous_close_id'])
    if not all((ident, tenant, period, requester, reason)):
        raise ValueError('restatement request requires identity and reason')
    with closing(connect(db_path)) as db, db:
        db.execute('BEGIN IMMEDIATE')
        old = db.execute('SELECT source_sha,status FROM period_restatement WHERE id=?', (ident,)).fetchone()
        if old:
            if old['source_sha'] != sha:
                raise ValueError('restatement request id reused with changed evidence')
            return {'request_id': ident, 'status': old['status'], 'idempotent': True}
        state = db.execute('SELECT * FROM period_state WHERE tenant=? AND period=?', (tenant, period)).fetchone()
        if not state or state['status'] != 'needs_restatement' or state['current_close_id'] != previous_id:
            raise ValueError('request must name current close with changed source evidence')
        previous = db.execute('SELECT source_sha FROM period_close WHERE id=?', (previous_id,)).fetchone()
        digest = source_digest(db, tenant, period)
        if digest == previous['source_sha']:
            raise ValueError('source evidence has not changed')
        db.execute('INSERT INTO period_restatement(id,tenant,period,previous_close_id,candidate_sha,requester,reason,status,source_sha,raw) '
                   'VALUES(?,?,?,?,?,?,?,?,?,?)',
                   (ident, tenant, period, previous_id, digest, requester, reason, 'pending', sha, raw))
    return {'request_id': ident, 'status': 'pending', 'candidate_sha': digest}


def approve_restatement(db_path, request_id, reviewer, proof_sha):
    if not reviewer or not proof_sha:
        raise ValueError('reviewer and approval proof required')
    with closing(connect(db_path)) as db, db:
        db.execute('BEGIN IMMEDIATE')
        request = db.execute('SELECT * FROM period_restatement WHERE id=?', (request_id,)).fetchone()
        if not request or request['status'] != 'pending':
            raise ValueError('restatement request missing or no longer pending')
        contract = chosen_contract(db, request['tenant'], request['period'])
        if reviewer not in json.loads(contract['approvers_json']) or reviewer == request['requester']:
            raise ValueError('independent authorized finance reviewer required')
        state = db.execute('SELECT * FROM period_state WHERE tenant=? AND period=?',
                           (request['tenant'], request['period'])).fetchone()
        if not state or state['current_close_id'] != request['previous_close_id'] or state['status'] != 'needs_restatement':
            raise ValueError('restatement base close changed')
        if source_digest(db, request['tenant'], request['period']) != request['candidate_sha']:
            raise ValueError('source changed after restatement request')
        previous = db.execute('SELECT * FROM period_close WHERE id=?', (request['previous_close_id'],)).fetchone()
        result = _freeze(db, contract, previous['version'] + 1, previous['id'], reviewer)
        delta = result['total_cents'] - previous['total_cents']
        db.execute('UPDATE period_restatement SET reviewer=?,review_proof_sha=?,status=?,new_close_id=?,delta_cents=? WHERE id=?',
                   (reviewer, proof_sha, 'approved', result['close_id'], delta, request_id))
    return {'request_id': request_id, 'status': 'approved', 'previous_close_id': previous['id'],
            'new_close_id': result['close_id'], 'old_total_cents': previous['total_cents'],
            'new_total_cents': result['total_cents'], 'delta_cents': delta,
            'lines': result['lines']}


def period_report(db_path, tenant, period):
    with closing(connect(db_path)) as db:
        state = db.execute('SELECT * FROM period_state WHERE tenant=? AND period=?', (tenant, period)).fetchone()
        if not state:
            raise ValueError('unknown period')
        choice = db.execute('SELECT contract_id,signer,proof_sha,source_sha FROM period_choice WHERE tenant=? AND period=?',
                            (tenant, period)).fetchone()
        closes = [dict(row) for row in db.execute('SELECT * FROM period_close WHERE tenant=? AND period=? ORDER BY version',
                                                  (tenant, period))]
        for close in closes:
            close['lines'] = [dict(row) for row in db.execute('SELECT * FROM period_line WHERE close_id=? ORDER BY product',
                                                              (close['id'],))]
            close['usage'] = [dict(row) for row in db.execute('SELECT * FROM period_usage_snapshot WHERE close_id=? ORDER BY usage_id',
                                                              (close['id'],))]
        return {'tenant': tenant, 'period': period, 'state': dict(state), 'choice': dict(choice),
                'source_sha': source_digest(db, tenant, period), 'closes': closes,
                'restatements': [dict(row) for row in db.execute(
                    'SELECT id,previous_close_id,candidate_sha,requester,reason,reviewer,review_proof_sha,status,new_close_id,delta_cents,source_sha '
                    'FROM period_restatement WHERE tenant=? AND period=? ORDER BY id', (tenant, period))]}
