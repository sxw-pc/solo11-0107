"""Contract allowance allocation, immutable close and authorized restatement."""

import hashlib
import json
from contextlib import closing

from .period_store import connect
from .period_usage import _read, _time, amendments, chosen_contract, source_digest


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


def _price_timeline(prices, amendment_rows):
    """Per-product signed price changes ordered by (effective_at, amendment id)."""
    timeline = {product: [] for product in prices}
    for row in amendment_rows:
        for product, price in json.loads(row['prices_json']).items():
            if product in timeline:
                timeline[product].append((_time(row['effective_at']), row['id'], row['clause'], price))
    for changes in timeline.values():
        changes.sort(key=lambda change: (change[0], change[1]))
    return timeline


def _price_at(base, changes, occurred):
    """Fold signed changes in order: retroactive reprices the whole period,
    prospective only events whose occurred_at reaches its effective_at."""
    price = base
    for effective_at, _, clause, amended in changes:
        if clause == 'retroactive' or occurred >= effective_at:
            price = amended
    return price


def _allocate(contract, effective, amendment_rows=()):
    prices = json.loads(contract['prices_json'])
    reserved = json.loads(contract['reserved_json'])
    priority = json.loads(contract['priority_json'])
    timeline = _price_timeline(prices, amendment_rows)
    by_product = {product: [] for product in prices}
    for row, units, correction_id in effective:
        by_product[row['product']].append((row, units, correction_id))
    for events in by_product.values():
        events.sort(key=lambda event: (_time(event[0]['occurred_at']), event[0]['id']))
    actual = {product: sum(units for _, units, _ in by_product[product]) for product in prices}
    reserved_used = {product: min(actual[product], reserved.get(product, 0)) for product in prices}
    remaining = {product: actual[product] - reserved_used[product] for product in prices}
    shared_left = contract['shared_units']
    shared_used = {product: 0 for product in prices}
    for product in priority:
        granted = min(shared_left, remaining[product])
        shared_used[product] = granted
        shared_left -= granted
    lines = []
    event_detail = {}
    for product in sorted(prices):
        reserved_left = reserved_used[product]
        shared_product_left = shared_used[product]
        billable = 0
        charge = 0
        billed_prices = set()
        for row, units, _ in by_product[product]:
            left = units
            covered = min(left, reserved_left)
            reserved_left -= covered
            left -= covered
            covered = min(left, shared_product_left)
            shared_product_left -= covered
            left -= covered
            price = _price_at(prices[product], timeline[product], _time(row['occurred_at']))
            event_detail[row['id']] = (left, price, left * price)
            billable += left
            charge += left * price
            if left:
                billed_prices.add(price)
        lines.append({'product': product, 'actual_units': actual[product],
                      'reserved_used': reserved_used[product], 'shared_used': shared_used[product],
                      'billable_units': billable,
                      'price_cents': billed_prices.pop() if len(billed_prices) == 1 else prices[product],
                      'charge_cents': charge})
    return lines, event_detail


def _freeze(db, contract, version, previous_id, actor):
    tenant, period = contract['tenant'], contract['period']
    effective = _effective_usage(db, tenant, period)
    amendment_rows = amendments(db, tenant, period)
    lines, event_detail = _allocate(contract, effective, amendment_rows)
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
        billable, price, charge = event_detail[row['id']]
        db.execute('INSERT INTO period_usage_snapshot VALUES(?,?,?,?,?,?,?)',
                   (close_id, row['id'], units, correction_id, billable, price, charge))
    for amendment in amendment_rows:
        db.execute('INSERT INTO period_close_amendment VALUES(?,?)', (close_id, amendment['id']))
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
        old_charges = {row['product']: row['charge_cents'] for row in db.execute(
            'SELECT product,charge_cents FROM period_line WHERE close_id=?', (previous['id'],))}
        new_charges = {line['product']: line['charge_cents'] for line in result['lines']}
        product_deltas = []
        for product in sorted(set(old_charges) | set(new_charges)):
            old_charge = old_charges.get(product, 0)
            new_charge = new_charges.get(product, 0)
            db.execute('INSERT INTO period_restatement_line VALUES(?,?,?,?,?)',
                       (request_id, product, old_charge, new_charge, new_charge - old_charge))
            product_deltas.append({'product': product, 'old_charge_cents': old_charge,
                                   'new_charge_cents': new_charge,
                                   'delta_cents': new_charge - old_charge})
        db.execute('UPDATE period_restatement SET reviewer=?,review_proof_sha=?,status=?,new_close_id=?,delta_cents=? WHERE id=?',
                   (reviewer, proof_sha, 'approved', result['close_id'], delta, request_id))
    return {'request_id': request_id, 'status': 'approved', 'previous_close_id': previous['id'],
            'new_close_id': result['close_id'], 'old_total_cents': previous['total_cents'],
            'new_total_cents': result['total_cents'], 'delta_cents': delta,
            'product_deltas': product_deltas, 'lines': result['lines']}


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
            close['amendments'] = [row['amendment_id'] for row in db.execute(
                'SELECT amendment_id FROM period_close_amendment WHERE close_id=? ORDER BY amendment_id', (close['id'],))]
        signed = []
        for row in db.execute('SELECT * FROM period_amendment WHERE tenant=? AND period=? ORDER BY effective_at,id',
                              (tenant, period)):
            signed.append({'id': row['id'], 'tenant': row['tenant'], 'period': row['period'],
                           'contract_id': row['contract_id'], 'contract_sha': row['contract_sha'],
                           'effective_at': row['effective_at'], 'clause': row['clause'],
                           'prices_cents': json.loads(row['prices_json']),
                           'signer': row['signer'], 'proof_sha': row['proof_sha'],
                           'source_sha': row['source_sha']})
        restatements = [dict(row) for row in db.execute(
            'SELECT id,previous_close_id,candidate_sha,requester,reason,reviewer,review_proof_sha,status,new_close_id,delta_cents,source_sha '
            'FROM period_restatement WHERE tenant=? AND period=? ORDER BY id', (tenant, period))]
        for restatement in restatements:
            restatement['lines'] = [dict(row) for row in db.execute(
                'SELECT product,old_charge_cents,new_charge_cents,delta_cents FROM period_restatement_line '
                'WHERE request_id=? ORDER BY product', (restatement['id'],))]
        return {'tenant': tenant, 'period': period, 'state': dict(state), 'choice': dict(choice),
                'source_sha': source_digest(db, tenant, period), 'amendments': signed,
                'closes': closes, 'restatements': restatements}
