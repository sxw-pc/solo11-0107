"""Independent quality product gate and reviewed fact repair arbitration."""

import hashlib
import json
import math
import os
import tempfile
from contextlib import closing
from decimal import Decimal
from pathlib import Path

from .store import connect


def _read(path):
    raw = Path(path).read_bytes()
    item = json.loads(raw)
    return item, raw, hashlib.sha256(raw).hexdigest()


def _write_lock(db):
    db.execute('BEGIN IMMEDIATE')


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _contract_order(contract):
    rules = contract.get('rules')
    products = contract.get('products')
    if (not isinstance(contract.get('contract_id'), str) or not contract['contract_id'] or
            not isinstance(rules, list) or not rules or not isinstance(products, list) or not products):
        raise ValueError('contract requires id, rules and products')
    by_id = {}
    kinds = {'present': 1, 'nonnegative': 1, 'equal': 2, 'sum_equal': 3}
    for rule in rules:
        if not isinstance(rule, dict):
            raise ValueError('invalid quality rule')
        name, kind = rule.get('id'), rule.get('kind')
        fields, deps = rule.get('fields'), rule.get('depends_on', [])
        if (not isinstance(name, str) or not name or name in by_id or
                not isinstance(kind, str) or kind not in kinds or not isinstance(fields, list) or
                not (len(fields) == kinds.get(kind) or kind == 'sum_equal' and len(fields) >= 3) or
                not all(isinstance(x, str) and x for x in fields) or
                not isinstance(deps, list) or not all(isinstance(x, str) and x for x in deps)):
            raise ValueError('invalid or duplicate quality rule')
        if len(set(fields)) != len(fields) or len(set(deps)) != len(deps):
            raise ValueError('duplicate quality rule field or dependency')
        by_id[name] = rule
    for rule in rules:
        if any(dep not in by_id or dep == rule['id'] for dep in rule.get('depends_on', [])):
            raise ValueError('unknown or self-dependent quality rule')
    order, visiting, visited = [], set(), set()

    def visit(name):
        if name in visiting:
            raise ValueError('cyclic quality rule dependency')
        if name in visited:
            return
        visiting.add(name)
        for dep in by_id[name].get('depends_on', []):
            visit(dep)
        visiting.remove(name)
        visited.add(name)
        order.append(by_id[name])

    for name in sorted(by_id):
        visit(name)
    product_ids = set()
    for product in products:
        if not isinstance(product, dict):
            raise ValueError('invalid quality product')
        name, required, delivery = product.get('id'), product.get('requires'), product.get('delivery', 'operational')
        if (not isinstance(name, str) or not name or name in product_ids or
                not isinstance(required, list) or not required or
                not all(isinstance(rule, str) and rule in by_id for rule in required) or
                len(set(required)) != len(required) or delivery not in ('operational', 'audit')):
            raise ValueError('invalid quality product dependencies')
        product_ids.add(name)
    return order


def register_quality_contract(db_path, path):
    item, raw, sha = _read(path)
    _contract_order(item)
    contract_id = str(item['contract_id'])
    with closing(connect(db_path)) as db, db:
        _write_lock(db)
        old = db.execute('SELECT source_sha FROM quality_contract WHERE id=?', (contract_id,)).fetchone()
        if old and old['source_sha'] != sha:
            raise ValueError('quality contract id reused with changed rules')
        db.execute('INSERT OR IGNORE INTO quality_contract VALUES(?,?,?)', (contract_id, sha, raw))
    return {'contract_id': contract_id, 'source_sha': sha, 'idempotent': bool(old)}


def _valid_fact(value):
    return (value is None or isinstance(value, str) or
            (isinstance(value, (int, float)) and not isinstance(value, bool) and
             (not isinstance(value, float) or math.isfinite(value))))


def import_quality_batch(db_path, path):
    item, raw, sha = _read(path)
    batch_id, contract_id = str(item.get('batch_id', '')), str(item.get('contract_id', ''))
    facts = item.get('facts')
    if (not batch_id or not contract_id or not isinstance(facts, dict) or
            any(not isinstance(key, str) or not key or not _valid_fact(value) for key, value in facts.items())):
        raise ValueError('invalid quality batch facts')
    with closing(connect(db_path)) as db, db:
        _write_lock(db)
        if not db.execute('SELECT 1 FROM quality_contract WHERE id=?', (contract_id,)).fetchone():
            raise ValueError('unknown quality contract')
        old = db.execute('SELECT contract_id,source_sha FROM quality_batch WHERE id=?', (batch_id,)).fetchone()
        if old and (old['contract_id'] != contract_id or old['source_sha'] != sha):
            raise ValueError('quality batch id reused with changed facts')
        db.execute('INSERT OR IGNORE INTO quality_batch(id,contract_id,source_sha,raw) VALUES(?,?,?,?)',
                   (batch_id, contract_id, sha, raw))
        assessment = _assess(db, batch_id)
    return {'batch_id': batch_id, 'revision': assessment['revision'],
            'products': assessment['products'], 'idempotent': bool(old)}


def _numeric(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not _valid_fact(value):
        raise ValueError('non numeric fact')
    return Decimal(str(value))


def _check(rule, facts):
    fields = rule['fields']
    if rule['kind'] == 'present':
        value = facts.get(fields[0])
        valid = value is not None and value != ''
        return valid, 'passed' if valid else f'{fields[0]} must be present'
    if any(field not in facts or facts[field] is None for field in fields):
        return False, 'missing fact: ' + ','.join(field for field in fields if field not in facts or facts[field] is None)
    try:
        if rule['kind'] == 'nonnegative':
            valid = _numeric(facts[fields[0]]) >= 0
        elif rule['kind'] == 'sum_equal':
            valid = _numeric(facts[fields[0]]) == sum((_numeric(facts[field]) for field in fields[1:]), Decimal(0))
        else:
            left, right = facts[fields[0]], facts[fields[1]]
            valid = _numeric(left) == _numeric(right) if isinstance(left, (int, float)) and isinstance(right, (int, float)) else left == right
    except ValueError:
        return False, 'non numeric fact in arithmetic rule'
    return valid, 'passed' if valid else f'{rule["kind"]} failed for {",".join(fields)}'


def _assess(db, batch_id):
    batch = db.execute('SELECT * FROM quality_batch WHERE id=?', (batch_id,)).fetchone()
    old = db.execute('SELECT id FROM quality_assessment WHERE batch_id=? AND revision=?',
                     (batch_id, batch['revision'])).fetchone()
    if old:
        return _assessment_report(db, old['id'])
    contract = json.loads(db.execute('SELECT raw FROM quality_contract WHERE id=?', (batch['contract_id'],)).fetchone()['raw'])
    facts = dict(json.loads(batch['raw'])['facts'])
    for correction in db.execute('SELECT fact_key,value_json FROM quality_correction WHERE batch_id=?', (batch_id,)):
        facts[correction['fact_key']] = json.loads(correction['value_json'])
    fact_sha = hashlib.sha256(_json(facts).encode('utf-8')).hexdigest()
    cursor = db.execute('INSERT INTO quality_assessment(batch_id,revision,fact_sha,facts_json) VALUES(?,?,?,?)',
                        (batch_id, batch['revision'], fact_sha, _json(facts)))
    assessment_id = cursor.lastrowid
    statuses = {}
    for rule in _contract_order(contract):
        blockers = [dep for dep in rule.get('depends_on', []) if statuses[dep] != 'passed']
        if blockers:
            status, detail = 'blocked', 'upstream: ' + ','.join(blockers)
        else:
            valid, detail = _check(rule, facts)
            status = 'passed' if valid else 'failed'
        statuses[rule['id']] = status
        db.execute('INSERT INTO quality_rule_result VALUES(?,?,?,?)',
                   (assessment_id, rule['id'], status, detail))
    for product in contract['products']:
        blockers = [name for name in product['requires'] if statuses[name] != 'passed']
        db.execute('INSERT INTO quality_product_result VALUES(?,?,?,?)',
                   (assessment_id, product['id'], 'blocked' if blockers else 'ready', _json(blockers)))
    return _assessment_report(db, assessment_id)


def _assessment_report(db, assessment_id):
    row = db.execute('SELECT * FROM quality_assessment WHERE id=?', (assessment_id,)).fetchone()
    return {'assessment_id': assessment_id, 'revision': row['revision'], 'fact_sha': row['fact_sha'],
            'facts': json.loads(row['facts_json']),
            'rules': {item['rule_id']: {'status': item['status'], 'detail': item['detail']}
                      for item in db.execute('SELECT * FROM quality_rule_result WHERE assessment_id=? ORDER BY rule_id', (assessment_id,))},
            'products': {item['product_id']: {'status': item['status'], 'blocked_by': json.loads(item['blocked_by'])}
                         for item in db.execute('SELECT * FROM quality_product_result WHERE assessment_id=? ORDER BY product_id', (assessment_id,))}}


def propose_quality_repair(db_path, path):
    item, raw, sha = _read(path)
    proposal, batch_id, key, proposer, proof = (str(item.get(name, '')) for name in
                                                ('proposal_id', 'batch_id', 'fact_key', 'proposer', 'evidence_sha'))
    try:
        expected_revision = int(item['expected_revision'])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError('repair proposal requires expected_revision') from exc
    value = item.get('value')
    if not all((proposal, batch_id, key, proposer, proof)) or not _valid_fact(value):
        raise ValueError('invalid quality repair proposal')
    with closing(connect(db_path)) as db, db:
        _write_lock(db)
        batch = db.execute('SELECT contract_id,revision FROM quality_batch WHERE id=?', (batch_id,)).fetchone()
        if not batch:
            raise ValueError('unknown quality batch')
        if expected_revision != batch['revision']:
            raise ValueError('stale repair proposal revision')
        contract = json.loads(db.execute('SELECT raw FROM quality_contract WHERE id=?', (batch['contract_id'],)).fetchone()['raw'])
        if key not in {field for rule in contract['rules'] for field in rule['fields']}:
            raise ValueError('repair fact is outside quality contract')
        old = db.execute('SELECT source_sha FROM quality_proposal WHERE id=?', (proposal,)).fetchone()
        if old and old['source_sha'] != sha:
            raise ValueError('proposal id reused with changed evidence')
        db.execute('INSERT OR IGNORE INTO quality_proposal(id,batch_id,expected_revision,fact_key,value_json,proposer,evidence_sha,source_sha,raw) '
                   'VALUES(?,?,?,?,?,?,?,?,?)', (proposal, batch_id, expected_revision, key, _json(value), proposer, proof, sha, raw))
    return {'proposal_id': proposal, 'idempotent': bool(old)}


def review_quality_repair(db_path, path):
    item, raw, sha = _read(path)
    proposal, reviewer, verdict, proof = (str(item.get(name, '')) for name in
                                          ('proposal_id', 'reviewer', 'verdict', 'proof_sha'))
    if not proposal or not reviewer or verdict not in ('approve', 'reject') or not proof:
        raise ValueError('invalid independent repair review')
    with closing(connect(db_path)) as db, db:
        _write_lock(db)
        candidate = db.execute('SELECT proposer,state FROM quality_proposal WHERE id=?', (proposal,)).fetchone()
        if not candidate or candidate['state'] != 'open' or candidate['proposer'] == reviewer:
            raise ValueError('review requires open proposal and independent reviewer')
        old = db.execute('SELECT source_sha FROM quality_review WHERE proposal_id=?', (proposal,)).fetchone()
        if old and old['source_sha'] != sha:
            raise ValueError('review already exists with different verdict')
        db.execute('INSERT OR IGNORE INTO quality_review VALUES(?,?,?,?,?,?)',
                   (proposal, reviewer, verdict, proof, sha, raw))
    return {'proposal_id': proposal, 'verdict': verdict, 'idempotent': bool(old)}


def withdraw_quality_repair(db_path, proposal_id, proposer, reason):
    if not reason or not reason.strip():
        raise ValueError('withdrawal requires reason')
    with closing(connect(db_path)) as db, db:
        _write_lock(db)
        row = db.execute('SELECT proposer,state FROM quality_proposal WHERE id=?', (proposal_id,)).fetchone()
        if not row or row['proposer'] != proposer or row['state'] not in ('open', 'withdrawn'):
            raise ValueError('only original proposer may withdraw an open proposal')
        db.execute("UPDATE quality_proposal SET state='withdrawn',withdraw_reason=? WHERE id=?",
                   (reason, proposal_id))
    return {'proposal_id': proposal_id, 'state': 'withdrawn'}


def apply_quality_repairs(db_path, batch_id):
    with closing(connect(db_path)) as db, db:
        _write_lock(db)
        batch = db.execute('SELECT revision FROM quality_batch WHERE id=?', (batch_id,)).fetchone()
        if not batch:
            raise ValueError('unknown quality batch')
        db.execute("UPDATE quality_proposal SET state='stale' WHERE batch_id=? AND state='open' AND expected_revision<>?",
                   (batch_id, batch['revision']))
        rows = db.execute("SELECT p.*,r.verdict FROM quality_proposal p LEFT JOIN quality_review r ON r.proposal_id=p.id "
                          "WHERE p.batch_id=? AND p.state='open' ORDER BY p.fact_key,p.id", (batch_id,)).fetchall()
        by_fact = {}
        for row in rows:
            by_fact.setdefault(row['fact_key'], []).append(row)
        outcomes, winners = {}, {}
        for key, candidates in by_fact.items():
            pending = [p['id'] for p in candidates if p['verdict'] is None]
            approvals = [p for p in candidates if p['verdict'] == 'approve']
            values = {p['value_json'] for p in approvals}
            if len(values) > 1 or (pending and len({p['value_json'] for p in candidates}) > 1):
                outcomes[key] = {'status': 'disputed', 'proposals': [p['id'] for p in candidates]}
            elif pending:
                outcomes[key] = {'status': 'pending_review', 'proposals': pending}
            elif not approvals:
                outcomes[key] = {'status': 'rejected', 'proposals': [p['id'] for p in candidates]}
            else:
                winner = approvals[0]
                winners[key] = winner
                outcomes[key] = {'status': 'approved', 'proposal': winner['id']}
        unresolved = [value['status'] for value in outcomes.values() if value['status'] != 'approved']
        if not outcomes:
            status = 'no_candidates'
        elif unresolved:
            status = 'disputed' if 'disputed' in unresolved else 'pending_review' if 'pending_review' in unresolved else 'rejected'
        else:
            status = 'applied'
            revision = batch['revision'] + 1
            for key, winner in winners.items():
                db.execute('INSERT INTO quality_correction VALUES(?,?,?,?,?) '
                           'ON CONFLICT(batch_id,fact_key) DO UPDATE SET value_json=excluded.value_json,proposal_id=excluded.proposal_id,revision=excluded.revision',
                           (batch_id, key, winner['value_json'], winner['id'], revision))
                db.execute("UPDATE quality_proposal SET state=CASE WHEN id=? THEN 'applied' ELSE 'closed' END "
                           "WHERE batch_id=? AND fact_key=? AND state='open'", (winner['id'], batch_id, key))
            db.execute('UPDATE quality_batch SET revision=? WHERE id=?', (revision, batch_id))
        assessment = _assess(db, batch_id)
        db.execute('INSERT INTO quality_arbitration(batch_id,status,details,assessment_id) VALUES(?,?,?,?)',
                   (batch_id, status, _json(outcomes), assessment['assessment_id']))
    return {'batch_id': batch_id, 'status': status, 'outcomes': outcomes, 'assessment': assessment}


def quality_report(db_path, batch_id):
    with closing(connect(db_path)) as db:
        batch = db.execute('SELECT * FROM quality_batch WHERE id=?', (batch_id,)).fetchone()
        if not batch:
            raise ValueError('unknown quality batch')
        original = json.loads(batch['raw'])['facts']
        corrections = [dict(x) for x in db.execute('SELECT * FROM quality_correction WHERE batch_id=? ORDER BY fact_key', (batch_id,))]
        effective = dict(original)
        for correction in corrections:
            effective[correction['fact_key']] = json.loads(correction['value_json'])
        assessment_ids = db.execute('SELECT id FROM quality_assessment WHERE batch_id=? ORDER BY revision', (batch_id,)).fetchall()
        return {'batch_id': batch_id, 'contract_id': batch['contract_id'], 'source_sha': batch['source_sha'],
                'original_facts': original, 'effective_facts': effective, 'corrections': corrections,
                'assessments': [_assessment_report(db, row['id']) for row in assessment_ids],
                'proposals': [dict(x) for x in db.execute('SELECT id,expected_revision,fact_key,value_json,proposer,evidence_sha,source_sha,state,withdraw_reason FROM quality_proposal WHERE batch_id=? ORDER BY id', (batch_id,))],
                'reviews': [dict(x) for x in db.execute('SELECT r.proposal_id,r.reviewer,r.verdict,r.proof_sha,r.source_sha FROM quality_review r JOIN quality_proposal p ON p.id=r.proposal_id WHERE p.batch_id=? ORDER BY r.proposal_id', (batch_id,))],
                'arbitrations': [dict(x) for x in db.execute('SELECT * FROM quality_arbitration WHERE batch_id=? ORDER BY id', (batch_id,))],
                'publications': [dict(x) | {'current': x['assessment_id'] == assessment_ids[-1]['id']}
                                 for x in db.execute('SELECT id,product_id,assessment_id,manifest_sha,out_path,created_at FROM quality_publication WHERE batch_id=? ORDER BY id', (batch_id,))]}


def _fact_correction_chains(db, batch_id, original_facts):
    """Reconstruct each repaired fact's chain from original proposal/review/arbitration rows.

    Chains are read from the immutable proposal, review and arbitration records and bound
    through the assessments created by each applied arbitration -- never inferred from the
    current quality_correction row. Returns ``(chains, errors)`` where errors maps a fact
    key to its broken-chain reasons (``None`` holds batch-level failures); a non-empty
    mapping means an audit product cannot be published.
    """
    def fail(key, message):
        errors.setdefault(key, []).append(message)

    chains, errors = {}, {}
    arbitrations = db.execute(
        "SELECT * FROM quality_arbitration WHERE batch_id=? AND status='applied' ORDER BY id",
        (batch_id,)).fetchall()
    proposals = {row['id']: row for row in
                 db.execute('SELECT * FROM quality_proposal WHERE batch_id=?', (batch_id,))}
    reviews = {row['proposal_id']: row for row in db.execute(
        'SELECT r.* FROM quality_review r JOIN quality_proposal p ON p.id=r.proposal_id WHERE p.batch_id=?',
        (batch_id,))}
    current = {row['fact_key']: row for row in
               db.execute('SELECT * FROM quality_correction WHERE batch_id=?', (batch_id,))}
    seen_keys = set()
    for arbitration in arbitrations:
        details = json.loads(arbitration['details'])
        assessment = db.execute('SELECT revision FROM quality_assessment WHERE id=?',
                                (arbitration['assessment_id'],)).fetchone()
        if not assessment:
            fail(None, 'arbitration %s has no assessment' % arbitration['id'])
            continue
        revision = assessment['revision']
        for key, outcome in details.items():
            if not isinstance(outcome, dict) or outcome.get('status') != 'approved':
                continue
            proposal_id = outcome.get('proposal')
            proposal = proposals.get(proposal_id)
            review = reviews.get(proposal_id)
            if proposal is None or proposal['fact_key'] != key or proposal['state'] != 'applied':
                fail(key, 'revision %s lacks an adopted proposal' % revision)
                continue
            if review is None or review['verdict'] != 'approve' or review['reviewer'] == proposal['proposer']:
                fail(key, 'revision %s lacks an independent approved review' % revision)
                continue
            new_value = json.loads(proposal['value_json'])
            previous = chains[key][-1]['new_value'] if key in chains else original_facts.get(key)
            prior_assessment = db.execute(
                'SELECT facts_json FROM quality_assessment WHERE batch_id=? AND revision=?',
                (batch_id, revision - 1)).fetchone()
            if prior_assessment is None:
                fail(key, 'revision %s has no predecessor assessment' % revision)
                continue
            old_value = json.loads(prior_assessment['facts_json']).get(key)
            if old_value != previous:
                fail(key, 'chain is not contiguous at revision %s' % revision)
            chains.setdefault(key, []).append(
                {'revision': revision, 'proposal_id': proposal_id,
                 'old_value': old_value, 'new_value': new_value,
                 'evidence_sha': proposal['evidence_sha'],
                 'reviewer': review['reviewer'], 'review_proof_sha': review['proof_sha'],
                 'superseded': False})
            seen_keys.add(key)
    for key, links in chains.items():
        revisions = [link['revision'] for link in links]
        if revisions != sorted(set(revisions)):
            fail(key, 'chain has non-unique revisions')
        for link in links[:-1]:
            link['superseded'] = True
        terminal = links[-1]
        correction = current.get(key)
        if correction is None:
            fail(key, 'current value has no correction record')
            continue
        if correction['revision'] != terminal['revision'] or correction['proposal_id'] != terminal['proposal_id'] \
                or json.loads(correction['value_json']) != terminal['new_value']:
            fail(key, 'current correction is not bound to its adopted chain')
    for key in set(current) - seen_keys:
        fail(key, 'current correction has no adopted reviewed chain')
    return chains, errors


def publish_quality_product(db_path, batch_id, product_id, out_path):
    """Write a product's current assessed facts and register its exact bytes."""
    target = Path(out_path).resolve()
    if target.exists():
        raise ValueError('publication target already exists')
    with closing(connect(db_path)) as db, db:
        _write_lock(db)
        if target.exists():
            raise ValueError('publication target already exists')
        batch = db.execute('SELECT contract_id,source_sha,revision FROM quality_batch WHERE id=?', (batch_id,)).fetchone()
        if not batch:
            raise ValueError('unknown quality batch')
        row = db.execute('SELECT id FROM quality_assessment WHERE batch_id=? AND revision=?',
                         (batch_id, batch['revision'])).fetchone()
        if not row:
            raise ValueError('current quality assessment missing')
        result = db.execute('SELECT status,blocked_by FROM quality_product_result WHERE assessment_id=? AND product_id=?',
                            (row['id'], product_id)).fetchone()
        if not result or result['status'] != 'ready':
            raise ValueError('quality product is blocked or unknown')
        assessment = _assessment_report(db, row['id'])
        contract_row = db.execute('SELECT source_sha,raw FROM quality_contract WHERE id=?',
                                  (batch['contract_id'],)).fetchone()
        contract_sha = contract_row['source_sha']
        contract = json.loads(contract_row['raw'])
        rules_by_id = {rule['id']: rule for rule in contract['rules']}
        required = next(product['requires'] for product in contract['products'] if product['id'] == product_id)
        relevant = set()

        def include(name):
            if name in relevant:
                return
            relevant.add(name)
            for dependency in rules_by_id[name].get('depends_on', []):
                include(dependency)

        for name in required:
            include(name)
        fields = {field for name in relevant for field in rules_by_id[name]['fields']}
        projected_facts = {key: value for key, value in assessment['facts'].items() if key in fields}
        product_contract = next(product for product in contract['products'] if product['id'] == product_id)
        delivery = product_contract.get('delivery', 'operational')
        manifest = {'batch_id': batch_id, 'product_id': product_id,
                    'assessment_id': row['id'], 'revision': batch['revision'],
                    'delivery': delivery,
                    'contract_sha': contract_sha, 'source_sha': batch['source_sha'],
                    'fact_sha': assessment['fact_sha'],
                    'product_fact_sha': hashlib.sha256(_json(projected_facts).encode('utf-8')).hexdigest(),
                    'facts': projected_facts,
                    'rules': {name: assessment['rules'][name] for name in sorted(relevant)}}
        if delivery == 'audit':
            original_facts = json.loads(db.execute('SELECT raw FROM quality_batch WHERE id=?',
                                                   (batch_id,)).fetchone()['raw'])['facts']
            chains, chain_errors = _fact_correction_chains(db, batch_id, original_facts)
            projected_chains = {key: chains.get(key, []) for key in sorted(fields)}
            relevant_errors = ['%s' % reason for reason in chain_errors.get(None, [])]
            for key in fields:
                if key in chain_errors:
                    relevant_errors.extend('%s: %s' % (key, reason) for reason in chain_errors[key])
                elif chains.get(key) and chains[key][-1]['new_value'] != projected_facts.get(key):
                    relevant_errors.append('%s: current value is not bound to its reviewed chain' % key)
                elif not chains.get(key) and db.execute(
                        'SELECT 1 FROM quality_correction WHERE batch_id=? AND fact_key=?',
                        (batch_id, key)).fetchone():
                    relevant_errors.append('%s: current value has no unique adopted reviewed chain' % key)
            if relevant_errors:
                raise ValueError('audit product rejected: ' + '; '.join(sorted(set(relevant_errors))))
            manifest['correction_chains'] = projected_chains
        raw = (_json(manifest) + '\n').encode('utf-8')
        sha = hashlib.sha256(raw).hexdigest()
        target.parent.mkdir(parents=True, exist_ok=True)
        handle, temp_name = tempfile.mkstemp(prefix='.quality-', dir=target.parent)
        try:
            with os.fdopen(handle, 'wb') as stream:
                stream.write(raw)
            os.replace(temp_name, target)
            cursor = db.execute('INSERT INTO quality_publication(batch_id,product_id,assessment_id,manifest_sha,raw,out_path) '
                                'VALUES(?,?,?,?,?,?)', (batch_id, product_id, row['id'], sha, raw, str(target)))
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)
    return {'publication_id': cursor.lastrowid, 'batch_id': batch_id, 'product_id': product_id,
            'assessment_id': row['id'], 'manifest_sha': sha, 'out_path': str(target)}
