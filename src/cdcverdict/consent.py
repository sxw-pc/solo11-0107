"""Purpose-limited consent over frozen replay values and recall of prior exports.

Consent versions describe a subject's authority; they do not alter replay, target,
repair, or report ledgers. A late withdrawal creates a new export recall instead
of rewriting the old exported bytes.

On top of the original owner/consent-event semantics this module keeps a
purpose-level *processing contract*.  A contract is either ``direct_only`` (only
processors explicitly declared by the consent source may receive the value) or
``delegated_chain`` (an allowed processor may delegate a narrowed processing
right to a sub-processor).  Every export freezes the concrete processor chain
that authorised the recipient, and a late revocation is chased along that frozen
chain so every processor that could have received a copy gets its own recall.
"""

import hashlib
import json
from pathlib import Path

from .store import connect


DIRECT = 'direct_only'
DELEGATED = 'delegated_chain'


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _json_sha(value):
    return _sha(json.dumps(value, ensure_ascii=False, sort_keys=True,
                           separators=(',', ':')).encode('utf-8'))


def _json_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':')).encode('utf-8')


# ---------------------------------------------------------------------------
# Evidence import
# ---------------------------------------------------------------------------

def _apply_processors(db, items, source_id=None):
    for item in items:
        if isinstance(item, dict):
            subject = str(item.get('subject', item.get('subject_id', '')))
            purpose = str(item.get('purpose', ''))
            declared = item.get('processor', item.get('id', ''))
        else:
            subject, purpose, declared = '', '', str(item)
        processors = declared if isinstance(declared, (list, tuple)) else [declared]
        if not subject or not purpose.strip() or not processors:
            raise ValueError('invalid_processor_declaration')
        if not db.execute('SELECT 1 FROM data_subject WHERE id=?', (subject,)).fetchone():
            raise ValueError('unknown_subject')
        for processor in processors:
            processor = str(processor)
            if not processor.strip():
                raise ValueError('invalid_processor_declaration')
            db.execute('INSERT OR IGNORE INTO consent_processor(subject_id,purpose,processor,source_id) '
                       'VALUES(?,?,?,?)', (subject, purpose, processor, source_id))


def _normalize_contract(spec, fallback_id=None):
    if not isinstance(spec, dict):
        raise ValueError('invalid_processing_contract')
    cid = str(spec.get('id', spec.get('contract_id', fallback_id or '')))
    purpose = str(spec.get('purpose', ''))
    mode = str(spec.get('mode', spec.get('kind', '')))
    mode = {'direct': DIRECT, 'direct_only': DIRECT,
            'delegated': DELEGATED, 'delegated_chain': DELEGATED}.get(mode, mode)
    if not cid or not purpose.strip() or mode not in (DIRECT, DELEGATED):
        raise ValueError('invalid_processing_contract')
    return cid, purpose, mode, spec


def _apply_contracts(db, items):
    if isinstance(items, dict):
        items = [dict(spec, id=name) if isinstance(spec, dict) else {'id': name, 'mode': spec}
                 for name, spec in items.items()]
    for spec in items:
        cid, purpose, mode, raw_spec = _normalize_contract(spec)
        digest = _json_sha(raw_spec)
        old = db.execute('SELECT * FROM processing_contract WHERE id=?', (cid,)).fetchone()
        if old:
            if (old['purpose'], old['mode'], old['source_sha']) != (purpose, mode, digest):
                raise ValueError('processing_contract_conflict')
            continue
        db.execute('INSERT INTO processing_contract(id,purpose,mode,source_sha,raw) '
                   'VALUES(?,?,?,?,?)', (cid, purpose, mode, digest, _json_bytes(raw_spec)))


def _delegation_values(item):
    parent_id = item.get('parent_id')
    parent_revision = item.get('parent_revision')
    return {
        'id': str(item.get('id', '')),
        'revision': int(item.get('revision', 0)),
        'contract_id': str(item.get('contract', item.get('contract_id', ''))),
        'parent_id': (None if parent_id is None else str(parent_id)),
        'parent_revision': (None if parent_revision is None else int(parent_revision)),
        'subject_id': str(item.get('subject', '')),
        'purpose': str(item.get('purpose', '')),
        'delegator': str(item.get('delegator', '')),
        'delegate': str(item.get('delegate', '')),
        'action': str(item.get('action', '')),
        'effective_watermark': int(item.get('effective_watermark', -1)),
        'expires_watermark': (None if item.get('expires_watermark') is None
                              else int(item['expires_watermark'])),
        'proof_sha': str(item.get('proof_sha', '')),
    }


def _exact_delegation(db, delegation_id, revision):
    return db.execute('SELECT * FROM processing_delegation WHERE id=? AND revision=?',
                      (delegation_id, revision)).fetchone()


def _delegation_definition(row):
    keys = ('contract_id', 'subject_id', 'purpose', 'delegator', 'delegate',
            'action', 'effective_watermark', 'expires_watermark', 'proof_sha',
            'parent_id', 'parent_revision')
    return tuple(row[key] for key in keys)


def _latest_delegation(db, delegation_id, watermark):
    return db.execute('SELECT * FROM processing_delegation WHERE id=? AND effective_watermark<=? '
                      'ORDER BY revision DESC LIMIT 1', (delegation_id, watermark)).fetchone()


def _parent_of(db, row):
    if row['parent_id'] is None:
        return None
    return _exact_delegation(db, row['parent_id'], row['parent_revision'])


def _ancestor_rows(db, row):
    """Return the parent chain of *row* oldest-last, rejecting broken links."""
    chain, seen, current = [], set(), _parent_of(db, row)
    while current is not None:
        token = (current['id'], current['revision'])
        if token in seen:
            raise ValueError('delegation_cycle')
        seen.add(token)
        chain.append(current)
        current = _parent_of(db, current)
    chain.reverse()
    return chain


def _delegation_window(db, row):
    """Intersection of all ancestor validity intervals for a delegation."""
    rows = _ancestor_rows(db, row) + [row]
    lower = max(item['effective_watermark'] for item in rows)
    expiries = [item['expires_watermark'] for item in rows
                if item['expires_watermark'] is not None]
    return lower, (min(expiries) if expiries else None)


def _chain_expired(db, leaf, watermark):
    """Whether a chain's intersected window has expired, even if it is invalid."""
    try:
        _, upper = _delegation_window(db, leaf)
    except ValueError:
        return False
    return upper is not None and watermark > upper


def _is_direct(db, subject, purpose, processor):
    return bool(db.execute('SELECT 1 FROM consent_processor '
                           'WHERE subject_id=? AND purpose=? AND processor=?',
                           (subject, purpose, processor)).fetchone())


def _owner(db, subject):
    row = db.execute('SELECT owner FROM data_subject WHERE id=?', (subject,)).fetchone()
    return row['owner'] if row else None


def _apply_delegations(db, items, source_sha):
    pending = list(items)

    def insert(item):
        if not isinstance(item, dict):
            raise ValueError('invalid_processing_delegation')
        value = _delegation_values(item)
        if (not value['id'] or not value['contract_id'] or not value['subject_id'] or
                not value['purpose'].strip() or not value['delegator'].strip() or
                not value['delegate'].strip() or value['delegator'] == value['delegate'] or
                value['revision'] < 1 or value['effective_watermark'] < 0 or
                not value['proof_sha'] or value['action'] not in ('delegate', 'revoke') or
                (value['expires_watermark'] is not None and
                 value['expires_watermark'] < value['effective_watermark'])):
            raise ValueError('invalid_processing_delegation')
        contract = db.execute('SELECT * FROM processing_contract WHERE id=?',
                              (value['contract_id'],)).fetchone()
        if not contract:
            raise ValueError('unknown_processing_contract')
        if contract['purpose'] != value['purpose']:
            raise ValueError('processing_delegation_purpose_mismatch')
        if contract['mode'] != DELEGATED:
            raise ValueError('delegation_not_allowed_for_direct_contract')
        if not db.execute('SELECT 1 FROM data_subject WHERE id=?',
                          (value['subject_id'],)).fetchone():
            raise ValueError('unknown_subject')
        prior = db.execute('SELECT * FROM processing_delegation WHERE id=? '
                           'ORDER BY revision DESC LIMIT 1', (value['id'],)).fetchone()
        exact = _exact_delegation(db, value['id'], value['revision'])
        if exact:
            if _delegation_definition(exact) != _delegation_definition(value):
                raise ValueError('processing_delegation_conflict')
            return False
        if prior:
            if value['revision'] != prior['revision'] + 1:
                raise ValueError('delegation_revision_gap')
            if value['effective_watermark'] < prior['effective_watermark']:
                raise ValueError('delegation_effective_regression')
            # Endpoints and scope of an existing delegation cannot change.
            for key in ('contract_id', 'subject_id', 'purpose', 'delegator', 'delegate'):
                if prior[key] != value[key]:
                    raise ValueError('processing_delegation_conflict')
            # A sub-delegation also may not silently re-root itself; dropping
            # the parent binding would escape every ancestor's narrowed window.
            if value['action'] == 'delegate' and (
                    prior['parent_id'], prior['parent_revision']) != (
                    value['parent_id'], value['parent_revision']):
                raise ValueError('processing_delegation_conflict')
        elif value['revision'] != 1:
            raise ValueError('delegation_revision_gap')
        parent = None
        parent_id = item.get('parent_id')
        parent_revision = item.get('parent_revision')
        if parent_id is not None or parent_revision is not None:
            if parent_id is None or parent_revision is None:
                raise ValueError('delegation_parent_required')
            parent = _exact_delegation(db, str(parent_id), int(parent_revision))
            if not parent:
                raise ValueError('unknown_parent_delegation')
            if parent['action'] != 'delegate':
                raise ValueError('delegation_parent_revoked')
            if (parent['contract_id'], parent['subject_id'], parent['purpose']) != (
                    value['contract_id'], value['subject_id'], value['purpose']):
                raise ValueError('delegation_scope_mismatch')
            if parent['delegate'] != value['delegator']:
                raise ValueError('delegation_parent_not_delegator')
            if value['action'] == 'revoke' and prior is None:
                raise ValueError('unknown_delegation')
        if value['action'] == 'revoke':
            if prior is None:
                raise ValueError('unknown_delegation')
            db.execute('INSERT INTO processing_delegation(id,revision,contract_id,parent_id,parent_revision,'
                       'subject_id,purpose,delegator,delegate,action,effective_watermark,expires_watermark,'
                       'proof_sha,source_sha,raw) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                       (value['id'], value['revision'], value['contract_id'], parent_id,
                        parent_revision, value['subject_id'], value['purpose'], value['delegator'],
                        value['delegate'], value['action'], value['effective_watermark'],
                        value['expires_watermark'], value['proof_sha'], source_sha,
                        _json_bytes(item)))
            return True
        if parent is None:
            if not (_is_direct(db, value['subject_id'], value['purpose'], value['delegator']) or
                    _owner(db, value['subject_id']) == value['delegator']):
                raise ValueError('delegation_root_not_authorized')
        else:
            lower, upper = _delegation_window(db, parent)
            if value['effective_watermark'] < lower:
                raise ValueError('delegation_effective_before_parent')
            if upper is not None and (value['expires_watermark'] is None or
                                      value['expires_watermark'] > upper):
                raise ValueError('delegation_out_of_parent_range')
            active_parent = _latest_delegation(db, parent['id'], value['effective_watermark'])
            if (not active_parent or active_parent['revision'] != parent['revision'] or
                    active_parent['action'] != 'delegate'):
                raise ValueError('delegation_parent_revoked')
            if not _active_chain(db, parent, value['effective_watermark']):
                raise ValueError('delegation_chain_invalid')
        # A delegation may not point back into its own ancestor chain.
        if parent is not None:
            seen_processors = {parent['delegator'], parent['delegate']}
            for ancestor in _ancestor_rows(db, parent):
                seen_processors.add(ancestor['delegator'])
                seen_processors.add(ancestor['delegate'])
            if value['delegate'] in seen_processors:
                raise ValueError('delegation_cycle')
        db.execute('INSERT INTO processing_delegation(id,revision,contract_id,parent_id,parent_revision,'
                   'subject_id,purpose,delegator,delegate,action,effective_watermark,expires_watermark,'
                   'proof_sha,source_sha,raw) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (value['id'], value['revision'], value['contract_id'], parent_id,
                    parent_revision, value['subject_id'], value['purpose'], value['delegator'],
                    value['delegate'], value['action'], value['effective_watermark'],
                    value['expires_watermark'], value['proof_sha'], source_sha,
                    _json_bytes(item)))
        return True

    deferred = None
    while pending:
        remaining = []
        progressed = False
        for item in pending:
            try:
                progressed = insert(item) or progressed
            except ValueError as exc:
                if str(exc) in ('unknown_parent_delegation', 'unknown_delegation'):
                    # The parent may be declared later in the same file.
                    remaining.append(item)
                    deferred = exc
                else:
                    raise
        if not remaining:
            return
        if not progressed:
            raise deferred
        pending = remaining



def _apply_processing(db, data, source_sha, source_id=None):
    processors = data.get('processors') or []
    contracts = data.get('contracts') or []
    delegations = data.get('delegations') or []
    processing = data.get('processing') or {}
    processors = list(processors) + list(processing.get('processors') or [])
    contracts = list(contracts) + list(processing.get('contracts') or [])
    delegations = list(delegations) + list(processing.get('delegations') or [])
    if processors:
        _apply_processors(db, processors, source_id)
    if contracts:
        _apply_contracts(db, contracts)
    if delegations:
        _apply_delegations(db, delegations, source_sha)


def import_processing(path, file):
    """Register processing contracts, processor declarations and delegations."""
    raw = Path(file).read_bytes()
    digest = _sha(raw)
    data = json.loads(raw.decode('utf-8'))
    db = connect(path)
    try:
        with db:
            before = db.execute('SELECT COUNT(*) FROM processing_delegation').fetchone()[0]
            _apply_processing(db, data, digest)
            after = db.execute('SELECT COUNT(*) FROM processing_delegation').fetchone()[0]
            return {'source_sha': digest, 'delegations': after, 'idempotent': before == after}
    finally:
        db.close()


def import_consent(path, file):
    raw = Path(file).read_bytes()
    digest = _sha(raw)
    data = json.loads(raw.decode('utf-8'))
    db = connect(path)
    try:
        try:
            with db:
                old = db.execute('SELECT * FROM consent_source WHERE sha=?', (digest,)).fetchone()
                if old and old['status'] == 'accepted':
                    return old['id']
                if old:
                    raise ValueError('previously_rejected_consent_source')
                source = db.execute("INSERT INTO consent_source(sha,raw,status) VALUES (?,?,'pending')",
                                    (digest, raw)).lastrowid
                for item in data.get('subjects', []):
                    sid = str(item['id'])
                    owner = str(item['owner'])
                    if not sid or not owner or not item['keys']:
                        raise ValueError('invalid_subject')
                    prior = db.execute('SELECT owner FROM data_subject WHERE id=?', (sid,)).fetchone()
                    if prior and prior['owner'] != owner:
                        raise ValueError('subject_owner_conflict')
                    db.execute('INSERT OR IGNORE INTO data_subject VALUES (?,?)', (sid, owner))
                    for key in item['keys']:
                        assigned = db.execute('SELECT subject_id FROM subject_key WHERE key=?',
                                              (key,)).fetchone()
                        if assigned and assigned['subject_id'] != sid:
                            raise ValueError('subject_key_conflict')
                        db.execute('INSERT OR IGNORE INTO subject_key VALUES (?,?)', (sid, str(key)))
                for e in data.get('events', []):
                    sid = str(e['subject'])
                    purpose = str(e['purpose'])
                    if (not purpose.strip() or e['action'] not in {'grant', 'revoke'} or
                            len(str(e['proof_sha'])) < 8):
                        raise ValueError('invalid_consent_evidence')
                    if not db.execute('SELECT 1 FROM data_subject WHERE id=?', (sid,)).fetchone():
                        raise ValueError('unknown_subject')
                    version = int(e['version'])
                    effective = int(e['effective_watermark'])
                    expires = e.get('expires_watermark')
                    if (version < 1 or effective < 0 or
                            (expires is not None and int(expires) < effective)):
                        raise ValueError('invalid_consent_version')
                    values = (sid, purpose, version, e['action'], effective, expires, str(e['proof_sha']))
                    prior = db.execute('SELECT * FROM consent_event WHERE id=? OR '
                                       '(subject_id=? AND purpose=? AND version=?)',
                                       (e['id'], sid, purpose, version)).fetchone()
                    if prior:
                        if prior['id'] != e['id'] or tuple(
                                prior[k] for k in ('subject_id', 'purpose', 'version', 'action',
                                                   'effective_watermark', 'expires_watermark',
                                                   'proof_sha')) != values:
                            raise ValueError('consent_version_conflict')
                        continue
                    last = db.execute('SELECT MAX(version) FROM consent_event WHERE subject_id=? '
                                      'AND purpose=?', (sid, purpose)).fetchone()[0]
                    if version != (last or 0) + 1:
                        raise ValueError('consent_version_gap')
                    db.execute('INSERT INTO consent_event VALUES (?,?,?,?,?,?,?,?,?)',
                               (e['id'], *values, source))
                _apply_processing(db, data, digest, source)
                # Withdrawal changes the licence of already materialized exports
                # in the same transaction as the new consent version.
                for export in db.execute('SELECT id FROM controlled_export').fetchall():
                    _reconcile_export(db, export['id'])
                db.execute("UPDATE consent_source SET status='accepted' WHERE id=?", (source,))
                return source
        except Exception as exc:
            with db:
                db.execute("INSERT OR IGNORE INTO consent_source(sha,raw,status,error) "
                           "VALUES (?,?,'rejected',?)", (digest, raw, str(exc)))
            raise
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Authority and the frozen process chain
# ---------------------------------------------------------------------------

def _authority(db, subject, purpose, watermark):
    row = db.execute('''SELECT * FROM consent_event WHERE subject_id=? AND purpose=?
      AND effective_watermark<=? ORDER BY version DESC LIMIT 1''',
                     (subject, purpose, watermark)).fetchone()
    if not row:
        return None, 'no_grant'
    if row['action'] == 'revoke':
        return row, 'withdrawn'
    if row['expires_watermark'] is not None and watermark > row['expires_watermark']:
        return row, 'expired'
    return row, 'authorized'


def _active_chain(db, leaf, watermark):
    """Resolve *leaf* to its full chain when every ancestor is active at watermark.

    A parent revision must still be the latest revision in force at the
    watermark; a newer revocation therefore invalidates children that pointed at
    it.  The chain validity interval is the intersection of all ancestor
    intervals, so a scope can only narrow as it is delegated.
    """
    rows, current, seen, processors = [], leaf, set(), []
    while current is not None:
        token = (current['id'], current['revision'])
        if token in seen:
            return None
        seen.add(token)
        if current['action'] != 'delegate':
            return None
        latest = _latest_delegation(db, current['id'], watermark)
        if latest is None or latest['revision'] != current['revision']:
            return None
        if current['effective_watermark'] > watermark:
            return None
        if (current['expires_watermark'] is not None and
                watermark > current['expires_watermark']):
            return None
        parent = _parent_of(db, current)
        if parent is not None and parent['delegate'] != current['delegator']:
            return None
        if current['delegate'] in processors:
            return None
        processors.append(current['delegate'])
        rows.append(current)
        current = parent
    rows.reverse()
    # Defensive: a persisted chain whose delegate repeats an earlier delegator
    # is cyclic and cannot authorise anything.
    if len(processors) != len(set(processors)) or rows[0]['delegator'] in processors:
        return None
    return rows


def _chain_members(rows):
    return [{'processor': rows[0]['delegator'], 'delegation_id': None,
             'delegation_revision': None}] + [
        {'processor': row['delegate'], 'delegation_id': row['id'],
         'delegation_revision': row['revision']} for row in rows]


def _chain_digest(mode, contract_id, members):
    return _json_sha({'mode': mode, 'contract_id': contract_id,
                      'members': [[member['processor'], member['delegation_id'],
                                   member['delegation_revision']] for member in members]})


def _resolution(mode, contract_id, members):
    return {'ok': True, 'mode': mode, 'contract_id': contract_id,
            'members': members, 'chain_sha': _chain_digest(mode, contract_id, members),
            'reason': None}


def _delegated_resolution(db, subject, purpose, recipient, watermark, contract):
    failure = 'no_delegation_chain'
    ids = [row['id'] for row in db.execute(
        'SELECT DISTINCT id FROM processing_delegation WHERE contract_id=? AND subject_id=? '
        'AND purpose=? ORDER BY id', (contract['id'], subject, purpose))]
    candidates = []
    for delegation_id in ids:
        latest = _latest_delegation(db, delegation_id, watermark)
        if latest is None:
            continue
        if latest['action'] == 'revoke':
            if latest['delegate'] == recipient:
                failure = 'delegation_chain_revoked'
            continue
        if latest['delegate'] != recipient:
            continue
        rows = _active_chain(db, latest, watermark)
        if not rows:
            # Distinguish an expired chain from one broken by revocation: the
            # effective window is the intersection of all ancestors, so a short
            # descendant can expire while an ancestor is still valid.
            if _chain_expired(db, latest, watermark):
                failure = 'delegation_chain_expired'
            else:
                failure = 'delegation_chain_invalid'
            continue
        root = rows[0]
        if not (_is_direct(db, subject, purpose, root['delegator']) or
                _owner(db, subject) == root['delegator']):
            failure = 'delegation_root_not_authorized'
            continue
        lower, upper = _delegation_window(db, latest)
        if watermark < lower:
            failure = 'delegation_not_effective'
            continue
        if upper is not None and watermark > upper:
            failure = 'delegation_chain_expired'
            continue
        members = _chain_members(rows)
        candidates.append(([member['processor'] for member in members],
                           [row['id'] for row in rows], members))
    if not candidates:
        return {'ok': False, 'reason': failure}
    candidates.sort(key=lambda item: (len(item[0]), item[0], item[1]))
    return _resolution(DELEGATED, contract['id'], candidates[0][2])


def _resolve_recipient(db, subject, purpose, recipient, watermark, contract):
    if contract['mode'] == DIRECT:
        if not _is_direct(db, subject, purpose, recipient):
            return {'ok': False, 'reason': 'recipient_not_direct_processor'}
        return _resolution(DIRECT, contract['id'],
                           [{'processor': recipient, 'delegation_id': None,
                             'delegation_revision': None}])
    # A declared processor may always receive directly; a sub-processor needs an
    # active delegation chain rooted in a declared processor.
    if _is_direct(db, subject, purpose, recipient):
        return _resolution(DELEGATED, contract['id'],
                           [{'processor': recipient, 'delegation_id': None,
                             'delegation_revision': None}])
    return _delegated_resolution(db, subject, purpose, recipient, watermark, contract)


def _resolve_for_export(db, subject, purpose, recipient, watermark, contracts):
    if not contracts:
        # Backward-compatible owner/consent-event semantics: without a
        # processing contract the recorded recipient is the only known holder.
        return _resolution('legacy', None,
                           [{'processor': recipient, 'delegation_id': None,
                             'delegation_revision': None}])
    # create_export() guarantees at most one contract per purpose here.
    return _resolve_recipient(db, subject, purpose, recipient, watermark, contracts[0])


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def create_export(path, run_id, purpose, recipient, contract_id=None, contract=None):
    if contract_id is None:
        contract_id = contract
    if not purpose.strip() or not recipient.strip():
        raise ValueError('purpose_and_recipient_required')
    db = connect(path)
    try:
        with db:
            run = db.execute('SELECT * FROM replay_run WHERE id=?', (run_id,)).fetchone()
            if not run or run['status'] not in {'已判定', '已归档'}:
                raise ValueError('run_not_final')
            if contract_id is None:
                contracts = db.execute('SELECT * FROM processing_contract WHERE purpose=? '
                                       'ORDER BY id', (purpose,)).fetchall()
                if len(contracts) > 1:
                    # Two policies for one purpose yield different legitimate
                    # answers for the same recipient; the caller must choose.
                    raise ValueError('processing_contract_required')
            else:
                contract = db.execute('SELECT * FROM processing_contract WHERE id=?',
                                      (contract_id,)).fetchone()
                if not contract:
                    raise ValueError('unknown_processing_contract')
                if contract['purpose'] != purpose:
                    raise ValueError('processing_contract_purpose_mismatch')
                contracts = [contract]
            candidates = db.execute('SELECT * FROM visible_row WHERE run_id=? ORDER BY key',
                                    (run_id,)).fetchall()
            # Resolve every subject before writing anything: an invalid or
            # expired chain must exclude the whole subject, never leave a parent
            # processor half-written ahead of a rejected child.
            rows, excluded, chains = [], [], {}
            for row in candidates:
                subject = db.execute('SELECT subject_id FROM subject_key WHERE key=?',
                                     (row['key'],)).fetchone()
                if row['status'] != '存在':
                    reason = 'replay_not_visible'
                elif not subject:
                    reason = 'unmapped_subject'
                else:
                    subject_id = subject['subject_id']
                    event, reason = _authority(db, subject_id, purpose, run['watermark'])
                    if reason == 'authorized':
                        key = (subject_id, purpose)
                        if key not in chains:
                            chains[key] = _resolve_for_export(
                                db, subject_id, purpose, recipient, run['watermark'], contracts)
                        resolved = chains[key]
                        if not resolved['ok']:
                            reason = resolved['reason']
                        else:
                            rows.append({'key': row['key'], 'subject_id': subject_id,
                                         'value': row['value'], 'consent_event_id': event['id'],
                                         'resolution': resolved})
                            continue
                excluded.append((row['key'], reason))
            content = _sha(json.dumps(
                [{key: item[key] for key in ('key', 'subject_id', 'value', 'consent_event_id')}
                 for item in rows], ensure_ascii=False, sort_keys=True).encode('utf-8'))
            subject_chains = {subject: resolution for (subject, _), resolution in chains.items()
                              if any(item['subject_id'] == subject for item in rows)}
            if len(subject_chains) == 1:
                export_chain_sha = next(iter(subject_chains.values()))['chain_sha']
            else:
                export_chain_sha = _json_sha({subject: resolution['chain_sha']
                                              for subject, resolution in sorted(subject_chains.items())})
            contract_ids = {resolution['contract_id'] for resolution in subject_chains.values()}
            stored_contract = (contract_id if contract_id is not None else
                               (next(iter(contract_ids)) if len(contract_ids) == 1 else None))
            status = 'complete' if rows and not excluded else 'partial' if rows else 'empty'
            eid = db.execute('INSERT INTO controlled_export(run_id,purpose,recipient,status,content_sha,'
                             'contract_id,chain_sha) VALUES (?,?,?,?,?,?,?)',
                             (run_id, purpose, recipient, status, content,
                              stored_contract if rows else None,
                              export_chain_sha if rows else _json_sha({}))).lastrowid
            for subject, resolution in sorted(subject_chains.items()):
                db.execute('INSERT INTO export_chain(export_id,subject_id,contract_id,mode,chain_sha) '
                           'VALUES(?,?,?,?,?)',
                           (eid, subject, resolution['contract_id'], resolution['mode'],
                            resolution['chain_sha']))
                for position, member in enumerate(resolution['members']):
                    db.execute('INSERT INTO export_chain_member(export_id,subject_id,position,'
                               'processor,delegation_id,delegation_revision) VALUES(?,?,?,?,?,?)',
                               (eid, subject, position, member['processor'],
                                member['delegation_id'], member['delegation_revision']))
            db.executemany('INSERT INTO export_row VALUES (?,?,?,?,?)',
                           [(eid, item['key'], item['subject_id'], item['value'],
                             item['consent_event_id']) for item in rows])
            db.executemany('INSERT INTO export_exclusion VALUES (?,?,?)',
                           [(eid, key, reason) for key, reason in excluded])
            return eid, status
    finally:
        db.close()


def _export_chain_rows(db, export_id, subject):
    return db.execute('SELECT * FROM export_chain_member WHERE export_id=? AND subject_id=? '
                      'ORDER BY position', (export_id, subject)).fetchall()


def _export_recipients(db, export_id, subject):
    members = _export_chain_rows(db, export_id, subject)
    if members:
        return [member['processor'] for member in members]
    export = db.execute('SELECT recipient FROM controlled_export WHERE id=?',
                        (export_id,)).fetchone()
    return [export['recipient']] if export else []


def _reconcile_export(db, export_id):
    export = db.execute('SELECT * FROM controlled_export WHERE id=?', (export_id,)).fetchone()
    if not export:
        raise ValueError('unknown_export')
    run = db.execute('SELECT watermark FROM replay_run WHERE id=?', (export['run_id'],)).fetchone()
    for row in db.execute('SELECT * FROM export_row WHERE export_id=?', (export_id,)).fetchall():
        original = db.execute('SELECT version FROM consent_event WHERE id=?',
                              (row['consent_event_id'],)).fetchone()
        if not original:
            continue
        revocations = db.execute('''SELECT * FROM consent_event WHERE subject_id=? AND purpose=?
          AND version>? AND action='revoke' AND effective_watermark<=? ORDER BY version''',
                                 (row['subject_id'], export['purpose'], original['version'],
                                  run['watermark'])).fetchall()
        for rev in revocations:
            # The frozen chain is the recall scope: every processor that could
            # have held this copy is chased separately.
            for holder in _export_recipients(db, export_id, row['subject_id']):
                db.execute("INSERT OR IGNORE INTO export_recall(export_id,key,revocation_id,recipient,"
                           "status,reason) VALUES (?,?,?,?,'pending','late_withdrawal')",
                           (export_id, row['key'], rev['id'], holder))
    return [r['id'] for r in db.execute('SELECT id FROM export_recall WHERE export_id=? '
                                        'ORDER BY id', (export_id,))]


def audit_withdrawals(path, export_id):
    db = connect(path)
    try:
        with db:
            return _reconcile_export(db, export_id)
    finally:
        db.close()


def acknowledge_recall(path, recall_id, recipient, proof_sha):
    if len(proof_sha) < 8:
        raise ValueError('recall_proof_required')
    db = connect(path)
    try:
        with db:
            recall = db.execute('SELECT * FROM export_recall WHERE id=?', (recall_id,)).fetchone()
            if not recall:
                raise ValueError('unknown_recall')
            row = db.execute('SELECT subject_id FROM export_row WHERE export_id=? AND key=?',
                             (recall['export_id'], recall['key'])).fetchone()
            if row:
                allowed = _export_recipients(db, recall['export_id'], row['subject_id'])
            else:
                export = db.execute('SELECT recipient FROM controlled_export WHERE id=?',
                                    (recall['export_id'],)).fetchone()
                allowed = [export['recipient']] if export else []
            if recipient != recall['recipient'] or recipient not in allowed:
                raise ValueError('wrong_export_recipient')
            old = db.execute('SELECT * FROM recall_receipt WHERE recall_id=? AND recipient=?',
                             (recall_id, recipient)).fetchone()
            if old:
                if old['proof_sha'] != proof_sha:
                    raise ValueError('recall_receipt_conflict')
                return old['id']
            receipt = db.execute('INSERT INTO recall_receipt(recall_id,recipient,proof_sha) '
                                 'VALUES (?,?,?)', (recall_id, recipient, proof_sha)).lastrowid
            # Only this copy is closed; a child processor's receipt never closes
            # the parent processor's separate copy.
            db.execute("UPDATE export_recall SET status='acknowledged' WHERE id=?", (recall_id,))
            return receipt
    finally:
        db.close()


def export_report(path, export_id):
    db = connect(path)
    try:
        export = db.execute('SELECT * FROM controlled_export WHERE id=?', (export_id,)).fetchone()
        if not export:
            raise ValueError('unknown_export')
        recalls = [dict(r) for r in db.execute('SELECT * FROM export_recall WHERE export_id=? '
                                               'ORDER BY id', (export_id,))]
        permission = ('recall_pending' if any(r['status'] == 'pending' for r in recalls)
                      else 'recalled' if recalls else 'valid')
        chains = []
        for chain in db.execute('SELECT * FROM export_chain WHERE export_id=? ORDER BY subject_id',
                                (export_id,)):
            chains.append({'subject_id': chain['subject_id'], 'mode': chain['mode'],
                           'contract_id': chain['contract_id'], 'chain_sha': chain['chain_sha'],
                           'members': [dict(member) for member in _export_chain_rows(
                               db, export_id, chain['subject_id'])]})
        return {'export': dict(export), 'current_permission': permission,
                'rows': [dict(r) for r in db.execute('SELECT * FROM export_row WHERE export_id=? '
                                                     'ORDER BY key', (export_id,))],
                'excluded': [dict(r) for r in db.execute('SELECT * FROM export_exclusion '
                                                         'WHERE export_id=? ORDER BY key',
                                                         (export_id,))],
                'chains': chains, 'recalls': recalls}
    finally:
        db.close()
