"""Signed warehouse usage terms and immutable metering/correction evidence."""

import hashlib
import json
from contextlib import closing
from datetime import datetime
from pathlib import Path

from .period_store import connect


def _read(path):
    raw = Path(path).read_bytes()
    return json.loads(raw), raw, hashlib.sha256(raw).hexdigest()


def _canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')


def _time(value):
    stamp = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if stamp.tzinfo is None:
        raise ValueError('period timestamp must include a timezone')
    return stamp


def _terms(item):
    required = ('contract_id', 'tenant', 'period', 'starts', 'ends', 'shared_units',
                'reserved', 'prices_cents', 'priority', 'signers', 'closers', 'approvers')
    if any(key not in item for key in required):
        raise ValueError('incomplete period contract')
    if not all(str(item[key]).strip() for key in ('contract_id', 'tenant', 'period')):
        raise ValueError('empty period contract identity')
    if _time(item['starts']) >= _time(item['ends']):
        raise ValueError('invalid period window')
    prices = item['prices_cents']
    reserved = item['reserved']
    priority = item['priority']
    if not isinstance(prices, dict) or not prices or not isinstance(reserved, dict) or not isinstance(priority, list):
        raise ValueError('invalid contract allocation terms')
    if set(priority) != set(prices) or len(priority) != len(prices) or not set(reserved) <= set(prices):
        raise ValueError('priority and reserved products must match price catalog')
    if any(not isinstance(key, str) or not key for key in prices):
        raise ValueError('invalid product')
    if not isinstance(item['shared_units'], int) or isinstance(item['shared_units'], bool) or item['shared_units'] < 0:
        raise ValueError('invalid shared allowance')
    for source in (prices, reserved):
        if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in source.values()):
            raise ValueError('contract amounts must be nonnegative integers')
    for role in ('signers', 'closers', 'approvers'):
        values = item[role]
        if not isinstance(values, list) or not values or len(set(values)) != len(values) or any(not isinstance(x, str) or not x for x in values):
            raise ValueError(f'invalid {role} list')


def register_contract(db_path, path):
    item, raw, sha = _read(path)
    _terms(item)
    with closing(connect(db_path)) as db, db:
        old = db.execute('SELECT source_sha FROM period_contract WHERE id=?', (item['contract_id'],)).fetchone()
        if old:
            if old['source_sha'] != sha:
                raise ValueError('contract id reused with changed terms')
            return {'contract_id': item['contract_id'], 'idempotent': True}
        db.execute('INSERT INTO period_contract VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (item['contract_id'], item['tenant'], item['period'], item['starts'], item['ends'],
                    item['shared_units'], json.dumps(item['reserved'], sort_keys=True),
                    json.dumps(item['prices_cents'], sort_keys=True), json.dumps(item['priority']),
                    json.dumps(item['signers']), json.dumps(item['closers']), json.dumps(item['approvers']),
                    sha, raw))
    return {'contract_id': item['contract_id'], 'tenant': item['tenant'], 'period': item['period']}


def choose_contract(db_path, path):
    item, raw, sha = _read(path)
    ident, signer, proof = (str(item[key]) for key in ('contract_id', 'signer', 'proof_sha'))
    if not ident or not signer or not proof:
        raise ValueError('invalid period contract choice')
    with closing(connect(db_path)) as db, db:
        contract = db.execute('SELECT * FROM period_contract WHERE id=?', (ident,)).fetchone()
        if not contract or signer not in json.loads(contract['signers_json']):
            raise ValueError('unsigned or unknown period contract')
        old = db.execute('SELECT source_sha FROM period_choice WHERE tenant=? AND period=?',
                         (contract['tenant'], contract['period'])).fetchone()
        if old:
            if old['source_sha'] != sha:
                raise ValueError('period contract choice is frozen')
            return {'contract_id': ident, 'idempotent': True}
        for other in db.execute('SELECT c.starts,c.ends FROM period_choice ch JOIN period_contract c ON c.id=ch.contract_id '
                                'WHERE ch.tenant=?', (contract['tenant'],)):
            if _time(contract['starts']) < _time(other['ends']) and _time(other['starts']) < _time(contract['ends']):
                raise ValueError('chosen warehouse periods overlap for tenant')
        db.execute('INSERT INTO period_choice VALUES(?,?,?,?,?,?,?)',
                   (contract['tenant'], contract['period'], ident, signer, proof, sha, raw))
        db.execute('INSERT INTO period_state(tenant,period,status) VALUES(?,?,?)',
                   (contract['tenant'], contract['period'], 'open'))
    return {'contract_id': ident, 'tenant': contract['tenant'], 'period': contract['period']}


def chosen_contract(db, tenant, period):
    return db.execute('SELECT c.* FROM period_choice ch JOIN period_contract c ON c.id=ch.contract_id '
                      'WHERE ch.tenant=? AND ch.period=?', (tenant, period)).fetchone()


AMENDMENT_CLAUSES = ('prospective', 'retroactive')


def sign_amendment(db_path, path):
    item, raw, sha = _read(path)
    if not isinstance(item, dict):
        raise ValueError('period amendment must be a JSON object')
    required = ('amendment_id', 'tenant', 'period', 'contract_id', 'contract_sha',
                'effective_at', 'clause', 'prices_cents', 'signer', 'proof_sha')
    if any(key not in item for key in required):
        raise ValueError('incomplete period amendment')
    ident, tenant, period, contract_id, contract_sha, signer, proof = (
        str(item[key]) for key in ('amendment_id', 'tenant', 'period', 'contract_id',
                                   'contract_sha', 'signer', 'proof_sha'))
    if not all((ident.strip(), tenant.strip(), period.strip(), contract_id.strip(),
                contract_sha.strip(), signer.strip(), proof.strip())):
        raise ValueError('empty period amendment identity')
    if item['clause'] not in AMENDMENT_CLAUSES:
        raise ValueError('amendment clause must be prospective or retroactive')
    prices = item['prices_cents']
    if not isinstance(prices, dict) or not prices:
        raise ValueError('amendment must name at least one product price')
    if any(not isinstance(key, str) or not key for key in prices):
        raise ValueError('invalid amendment product')
    if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in prices.values()):
        raise ValueError('amendment prices must be nonnegative integer cents')
    effective = _time(item['effective_at'])
    with closing(connect(db_path)) as db, db:
        db.execute('BEGIN IMMEDIATE')
        old = db.execute('SELECT source_sha FROM period_amendment WHERE id=?', (ident,)).fetchone()
        if old:
            if old['source_sha'] != sha:
                raise ValueError('period amendment is immutable')
            return {'amendment_id': ident, 'idempotent': True}
        contract = chosen_contract(db, tenant, period)
        if not contract or contract['id'] != contract_id:
            raise ValueError('amendment targets no chosen period contract')
        if contract['source_sha'] != contract_sha:
            raise ValueError('amendment does not bind the registered contract digest')
        if signer not in json.loads(contract['signers_json']):
            raise ValueError('amendment lacks an authorized contract signer')
        if not _time(contract['starts']) <= effective < _time(contract['ends']):
            raise ValueError('amendment effective_at falls outside the contract period')
        catalog = json.loads(contract['prices_json'])
        if not set(prices) <= set(catalog):
            raise ValueError('amendment may only reprice contracted products')
        db.execute('INSERT INTO period_amendment VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                   (ident, tenant, period, contract_id, contract_sha, item['effective_at'],
                    item['clause'], json.dumps(prices, sort_keys=True), signer, proof, sha, raw))
        state = db.execute('SELECT current_close_id FROM period_state WHERE tenant=? AND period=?',
                           (tenant, period)).fetchone()
        if state and state['current_close_id']:
            db.execute("UPDATE period_state SET status='needs_restatement' WHERE tenant=? AND period=?",
                       (tenant, period))
    return {'amendment_id': ident, 'tenant': tenant, 'period': period, 'clause': item['clause']}


def amendments(db, tenant, period):
    return db.execute('SELECT * FROM period_amendment WHERE tenant=? AND period=? '
                      'ORDER BY effective_at,id', (tenant, period)).fetchall()


def source_digest(db, tenant, period):
    contract = chosen_contract(db, tenant, period)
    if not contract:
        raise ValueError('period has no signed contract')
    choice = db.execute('SELECT source_sha FROM period_choice WHERE tenant=? AND period=?', (tenant, period)).fetchone()
    pieces = [contract['source_sha'], choice['source_sha']]
    pieces += [row['source_sha'] for row in db.execute(
        'SELECT source_sha FROM period_amendment WHERE tenant=? AND period=? ORDER BY id', (tenant, period))]
    pieces += [row['source_sha'] for row in db.execute(
        'SELECT source_sha FROM period_usage WHERE tenant=? AND period=? ORDER BY id', (tenant, period))]
    pieces += [row['source_sha'] for row in db.execute(
        'SELECT c.source_sha FROM period_correction c JOIN period_usage u ON u.id=c.usage_id '
        'WHERE u.tenant=? AND u.period=? ORDER BY c.usage_id,c.revision', (tenant, period))]
    return hashlib.sha256('|'.join(pieces).encode()).hexdigest()


def _record(db, table, kind, item, file_id):
    ident = str(item['event_id'])
    if not ident.strip():
        raise ValueError('period event id cannot be empty')
    raw = _canonical(item)
    sha = hashlib.sha256(raw).hexdigest()
    global_old = db.execute('SELECT kind,source_sha FROM period_event_identity WHERE id=?', (ident,)).fetchone()
    if global_old and (global_old['kind'], global_old['source_sha']) != (kind, sha):
        raise ValueError(f'period event id reused across kind or changed bytes: {ident}')
    old = db.execute(f'SELECT source_sha FROM {table} WHERE id=?', (ident,)).fetchone()
    if old:
        if old['source_sha'] != sha:
            raise ValueError(f'{kind} event id reused with changed bytes')
        return False, sha, raw
    db.execute('INSERT INTO period_event_identity VALUES(?,?,?)', (ident, kind, sha))
    return True, sha, raw


def import_file(db_path, path):
    item, raw, file_sha = _read(path)
    if not isinstance(item, dict) or not isinstance(item.get('usage', []), list) or not isinstance(item.get('corrections', []), list):
        raise ValueError('period source must contain usage and corrections arrays')
    touched = set()
    changed = 0
    with closing(connect(db_path)) as db, db:
        db.execute('INSERT OR IGNORE INTO period_source_file(source_sha,raw) VALUES(?,?)', (file_sha, raw))
        file_id = db.execute('SELECT id FROM period_source_file WHERE source_sha=?', (file_sha,)).fetchone()[0]
        for event in item.get('usage', []):
            tenant, period, product = (str(event[key]) for key in ('tenant', 'period', 'product'))
            contract = chosen_contract(db, tenant, period)
            if not contract or product not in json.loads(contract['prices_json']):
                raise ValueError('usage has no signed product contract')
            at = _time(event['occurred_at'])
            if not _time(contract['starts']) <= at < _time(contract['ends']):
                raise ValueError('usage falls outside chosen period')
            units = event['units']
            if not isinstance(units, int) or isinstance(units, bool) or units < 0:
                raise ValueError('usage units must be nonnegative integer')
            fresh, sha, event_raw = _record(db, 'period_usage', 'usage', event, file_id)
            if fresh:
                db.execute('INSERT INTO period_usage VALUES(?,?,?,?,?,?,?,?,?)',
                           (event['event_id'], tenant, period, product, event['occurred_at'], units,
                            sha, event_raw, file_id))
                changed += 1
                touched.add((tenant, period))
        for event in item.get('corrections', []):
            target = db.execute('SELECT * FROM period_usage WHERE id=?', (event['usage_id'],)).fetchone()
            if not target:
                raise ValueError('correction targets unknown usage')
            revision = event['revision']
            units = event['corrected_units']
            if (not isinstance(revision, int) or isinstance(revision, bool) or revision < 1 or
                    not isinstance(units, int) or isinstance(units, bool) or units < 0 or
                    not str(event.get('reason', '')).strip()):
                raise ValueError('invalid period correction')
            _time(event['observed_at'])
            if _time(event['observed_at']) < _time(target['occurred_at']):
                raise ValueError('correction cannot precede measured usage')
            fresh, sha, event_raw = _record(db, 'period_correction', 'correction', event, file_id)
            if fresh:
                head = db.execute('SELECT MAX(revision) FROM period_correction WHERE usage_id=?',
                                  (event['usage_id'],)).fetchone()[0] or 0
                if revision != head + 1:
                    raise ValueError('correction revision must advance by one')
                db.execute('INSERT INTO period_correction VALUES(?,?,?,?,?,?,?,?,?)',
                           (event['event_id'], event['usage_id'], revision, units, event['observed_at'],
                            event['reason'], sha, event_raw, file_id))
                changed += 1
                touched.add((target['tenant'], target['period']))
        for tenant, period in touched:
            state = db.execute('SELECT current_close_id FROM period_state WHERE tenant=? AND period=?',
                               (tenant, period)).fetchone()
            if state and state['current_close_id']:
                db.execute("UPDATE period_state SET status='needs_restatement' WHERE tenant=? AND period=?",
                           (tenant, period))
    return {'imported_events': changed, 'touched_periods': sorted([f'{a}/{b}' for a, b in touched]),
            'source_sha': file_sha, 'idempotent': changed == 0}
