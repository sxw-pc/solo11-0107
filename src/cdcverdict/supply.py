"""Independent source-delivery arbitration and recovery-capacity ledger.

These decisions concern whether upstream data was delivered and which acquisition
work can run.  They do not inspect or mutate CDC replay runs.

A feed slot freezes one of two explicit business guarantees:

``terminal_origin``
    Two observations are independent when their attested paths end at different
    upstream origins.  Shared intermediate relays do not collapse votes.  This is
    the historical v3 contract.

``path_disjoint``
    Two observations are independent only when no node occurs in both complete
    attested paths.  At most one vote per producer and digest is counted.  The
    exact maximum set of pairwise path-disjoint producers is solved with a
    bitset branch-and-bound; an unproven maximum is ``indeterminate`` and never
    qualifies a delivery.
"""

import hashlib
import json
import os
from contextlib import closing
from pathlib import Path

from .store import connect

TERMINAL_ORIGIN = 'terminal_origin'
PATH_DISJOINT = 'path_disjoint'
INDEPENDENCE_BASES = (TERMINAL_ORIGIN, PATH_DISJOINT)

# Explicit fail-closed search bound for exact path-disjoint selection.  Exhausting
# it does not license a guess: the verdict becomes indeterminate.  The bound can
# be tightened through the CDEVERDICT_PATH_SEARCH_LIMIT environment variable.
PATH_SEARCH_NODE_LIMIT = int(os.environ.get('CDEVERDICT_PATH_SEARCH_LIMIT', '200000'))


class _SearchLimitExceeded(Exception):
    pass



def _load(path):
    raw = Path(path).read_bytes()
    return json.loads(raw), raw, hashlib.sha256(raw).hexdigest()


def register_slot(db_path, path, independence_basis=None):
    item, _, _ = _load(path)
    slot, partition = str(item['slot']), str(item['partition'])
    deadline, quorum = str(item['deadline']), int(item['quorum'])
    declared = item.get('independence_basis')
    declared = None if declared is None else str(declared)
    if independence_basis is not None:
        independence_basis = str(independence_basis)
        if declared is not None and declared != independence_basis:
            raise ValueError('slot declares conflicting independence basis')
    basis = independence_basis if independence_basis is not None else (declared or TERMINAL_ORIGIN)
    if basis not in INDEPENDENCE_BASES:
        raise ValueError('slot requires a known independence basis')
    if not slot or not partition or not deadline or quorum < 2:
        raise ValueError('slot requires partition, deadline and quorum >= 2')
    with closing(connect(db_path)) as db, db:
        old = db.execute('SELECT * FROM feed_slot WHERE id=?', (slot,)).fetchone()
        if old:
            if (old['partition'], old['deadline'], old['quorum'], old['independence_basis']) != (
                    partition, deadline, quorum, basis):
                raise ValueError('slot contract changed; register a new slot')
            return {'slot': slot, 'status': old['status'], 'revision': old['revision'],
                    'independence_basis': old['independence_basis']}
        db.execute('INSERT INTO feed_slot(id,partition,deadline,quorum,independence_basis) VALUES(?,?,?,?,?)',
                   (slot, partition, deadline, quorum, basis))
    return {'slot': slot, 'status': 'missing', 'revision': 0, 'independence_basis': basis}


def register_lineage(db_path, path):
    """Freeze an attested producer-to-origin path and reopen affected slots."""
    item, raw, source_sha = _load(path)
    event, producer, source_domain = (str(item[key]) for key in ('event_id', 'producer', 'source_domain'))
    version, ancestry = int(item['version']), item['ancestry']
    attested_by, proof_sha = str(item['attested_by']), str(item['proof_sha'])
    if (not all((event, producer, source_domain, attested_by, proof_sha)) or
            version < 1 or not isinstance(ancestry, list) or not ancestry or
            any(not isinstance(part, str) or not part for part in ancestry) or
            len(set(ancestry)) != len(ancestry) or ancestry[0] != source_domain or
            attested_by == producer):
        raise ValueError('invalid or cyclic producer lineage evidence')
    root_origin = ancestry[-1]
    with closing(connect(db_path)) as db, db:
        old = db.execute('SELECT source_sha FROM feed_lineage_event WHERE event_id=? OR (producer=? AND version=?)',
                         (event, producer, version)).fetchone()
        if old:
            if old['source_sha'] != source_sha:
                raise ValueError('lineage event/version reused with changed evidence')
            return {'producer': producer, 'version': version, 'root_origin': root_origin, 'idempotent': True}
        db.execute('INSERT INTO feed_lineage_event VALUES(?,?,?,?,?,?,?,?,?,?)',
                   (event, producer, version, source_domain, json.dumps(ancestry), root_origin,
                    attested_by, proof_sha, source_sha, raw))
        head = db.execute('SELECT version FROM feed_lineage_head WHERE producer=?', (producer,)).fetchone()
        reopened = []
        if not head or version > head['version']:
            db.execute('INSERT INTO feed_lineage_head VALUES(?,?,?,?) '
                       'ON CONFLICT(producer) DO UPDATE SET version=excluded.version,event_id=excluded.event_id,root_origin=excluded.root_origin',
                       (producer, version, event, root_origin))
            slots = db.execute('SELECT DISTINCT slot_id FROM feed_observation WHERE producer=? ORDER BY slot_id',
                               (producer,)).fetchall()
            for slot in slots:
                reopened.append(_arbitrate(db, slot['slot_id'])['slot'])
            _schedule_all(db)
    return {'producer': producer, 'version': version, 'root_origin': root_origin,
            'reopened_slots': reopened}


def _disjoint_set(paths, quorum, node_limit=None):
    """Exact maximum pairwise path-disjoint producer set, or None when unproven.

    ``paths`` maps producer -> ordered ancestry.  Bitsets encode which producers
    conflict with a given producer.  The search branches include/exclude, pruning
    on an optimistic remaining bound.  Producers are visited in sorted order and
    inclusion is explored first, so the same maximum set is returned regardless of
    observation arrival order.  A search that cannot finish inside ``node_limit``
    raises instead of falling back to a greedy answer.
    """
    producers = sorted(paths)
    count = len(producers)
    node_sets = {producer: frozenset(nodes) for producer, nodes in paths.items()}
    conflicts = [0] * count
    for index, producer in enumerate(producers):
        nodes = node_sets[producer]
        mask = 0
        for other, peer in enumerate(producers):
            if other != index and nodes & node_sets[peer]:
                mask |= 1 << other
        conflicts[index] = mask

    limit = int(PATH_SEARCH_NODE_LIMIT if node_limit is None else node_limit)
    explored = 0
    best_size = 0
    best_mask = 0

    def search(index, chosen_mask, available_mask, chosen):
        nonlocal explored, best_size, best_mask
        explored += 1
        if explored > limit:
            raise _SearchLimitExceeded('path-disjoint search exceeded its exactness bound')
        if chosen + available_mask.bit_count() <= best_size:
            return
        if index == count:
            best_size, best_mask = chosen, chosen_mask
            return
        if not (available_mask >> index) & 1:
            search(index + 1, chosen_mask, available_mask & ~(1 << index), chosen)
            return
        # Include first: the first maximum found is the lexicographically smallest.
        search(index + 1, chosen_mask | (1 << index),
               available_mask & ~conflicts[index] & ~(1 << index), chosen + 1)
        search(index + 1, chosen_mask, available_mask & ~(1 << index), chosen)

    search(0, 0, (1 << count) - 1, 0)
    if best_size < quorum:
        return None
    return {producers[index] for index in range(count) if best_mask & (1 << index)}


def _path_witness(db, selected_producers):
    """Complete attested paths and lineage versions for the selected producers."""
    witness = {}
    for producer in selected_producers:
        head = db.execute('SELECT h.version,h.event_id,h.root_origin,e.ancestry '
                          'FROM feed_lineage_head h JOIN feed_lineage_event e ON e.event_id=h.event_id '
                          'WHERE h.producer=?', (producer,)).fetchone()
        if not head:
            raise ValueError('selected producer has no lineage head')
        witness[producer] = {'version': head['version'], 'event_id': head['event_id'],
                             'root_origin': head['root_origin'],
                             'ancestry': json.loads(head['ancestry'])}
    return witness


def _arbitrate(db, slot, search_limit=None):
    row = db.execute('SELECT * FROM feed_slot WHERE id=?', (slot,)).fetchone()
    basis = row['independence_basis'] or TERMINAL_ORIGIN
    observations = db.execute(
        'SELECT o.producer,o.digest,o.event_id,h.root_origin,e.ancestry,h.version AS lineage_version '
        'FROM feed_observation o LEFT JOIN feed_lineage_head h ON h.producer=o.producer '
        'LEFT JOIN feed_lineage_event e ON e.event_id=h.event_id '
        'WHERE o.slot_id=? ORDER BY o.event_id',
        (slot,)).fetchall()
    by_digest = {}
    paths_by_digest = {}
    unverified = []
    lineage_evidence = {}
    for observation in observations:
        if not observation['root_origin'] or not observation['ancestry']:
            unverified.append(observation['event_id'])
            continue
        # One producer casts at most one vote per digest in either contract.
        lineage_evidence[observation['producer']] = {'root_origin': observation['root_origin'],
                                                     'version': observation['lineage_version']}
        by_digest.setdefault(observation['digest'], set()).add(observation['root_origin'])
        paths_by_digest.setdefault(observation['digest'], {})[observation['producer']] = list(
            json.loads(observation['ancestry']))

    selected_producers = []
    witness = {}
    indeterminate = None
    if len(by_digest) > 1:
        status, selected = 'disputed', None
    elif not by_digest:
        status, selected = ('unverified' if unverified else 'missing'), None
    else:
        digest = next(iter(by_digest))
        if basis == PATH_DISJOINT:
            candidates = paths_by_digest[digest]
            try:
                independent = _disjoint_set(candidates, row['quorum'], search_limit)
            except _SearchLimitExceeded:
                independent = None
                indeterminate = 'path-disjoint search limit exhausted before exact maximum'
            except RecursionError:
                # A pathologically deep candidate set must also fail closed.
                independent = None
                indeterminate = 'path-disjoint search limit exhausted before exact maximum'
            if indeterminate:
                status, selected = 'indeterminate', None
            elif independent is None:
                status, selected = 'collecting', None
            else:
                status, selected = 'accepted', digest
                selected_producers = sorted(independent)
                witness = _path_witness(db, selected_producers)
        else:
            producers = by_digest[digest]
            status, selected = ('accepted', digest) if len(producers) >= row['quorum'] else ('collecting', None)

    revision = row['revision'] + 1
    evidence = {'independence_basis': basis,
                'origins_by_digest': {digest: sorted(origins) for digest, origins in sorted(by_digest.items())},
                'producer_lineage': lineage_evidence, 'unverified_events': unverified,
                'candidate_paths_by_digest': {digest: {producer: list(nodes)
                                                       for producer, nodes in sorted(paths.items())}
                                              for digest, paths in sorted(paths_by_digest.items())},
                'selected_producers': selected_producers, 'path_witness': witness}
    if indeterminate:
        evidence['indeterminate_reason'] = indeterminate
    db.execute('UPDATE feed_slot SET revision=?,status=?,selected_digest=? WHERE id=?',
               (revision, status, selected, slot))
    db.execute('INSERT INTO feed_verdict(slot_id,revision,status,selected_digest,evidence) VALUES(?,?,?,?,?)',
               (slot, revision, status, selected, json.dumps(evidence, sort_keys=True)))
    return {'slot': slot, 'revision': revision, 'status': status, 'selected_digest': selected,
            'independence_basis': basis, 'independent_origins': evidence['origins_by_digest'],
            'lineage': lineage_evidence, 'selected_producers': selected_producers,
            'path_witness': witness, 'unverified_events': unverified}


def observe(db_path, path):
    item, raw, source_sha = _load(path)
    event = str(item['event_id'])
    slot, producer, digest = str(item['slot']), str(item['producer']), str(item['digest'])
    count, observed_at = int(item['row_count']), str(item['observed_at'])
    if not all((event, slot, producer, digest, observed_at)) or count < 0:
        raise ValueError('invalid feed observation')
    with closing(connect(db_path)) as db, db:
        if not db.execute('SELECT 1 FROM feed_slot WHERE id=?', (slot,)).fetchone():
            raise ValueError('unknown feed slot')
        old = db.execute('SELECT source_sha,slot_id FROM feed_observation WHERE event_id=?', (event,)).fetchone()
        if old:
            if old['source_sha'] != source_sha or old['slot_id'] != slot:
                raise ValueError('event id reused with different evidence')
            row = db.execute('SELECT revision,status,selected_digest FROM feed_slot WHERE id=?', (slot,)).fetchone()
            return {'slot': slot, **dict(row), 'idempotent': True}
        db.execute('INSERT INTO feed_observation(event_id,slot_id,producer,digest,row_count,observed_at,source_sha,raw) '
                   'VALUES(?,?,?,?,?,?,?,?)', (event, slot, producer, digest, count, observed_at, source_sha, raw))
        result = _arbitrate(db, slot)
        _schedule_all(db)
        return result


def register_worker(db_path, path):
    item, _, _ = _load(path)
    worker, partition, capacity = str(item['worker']), str(item['partition']), int(item['capacity'])
    if not worker or not partition or capacity < 1:
        raise ValueError('worker requires partition and positive capacity')
    with closing(connect(db_path)) as db, db:
        old = db.execute('SELECT partition,capacity FROM recovery_worker WHERE id=?', (worker,)).fetchone()
        if old and (old['partition'], old['capacity']) != (partition, capacity):
            raise ValueError('worker contract changed')
        db.execute('INSERT OR IGNORE INTO recovery_worker VALUES(?,?,?)', (worker, partition, capacity))
        _schedule_all(db)
    return {'worker': worker, 'partition': partition, 'capacity': capacity}


def _set_job(db, job_id, status, reason):
    current = db.execute('SELECT status FROM recovery_job WHERE id=?', (job_id,)).fetchone()[0]
    if current != status:
        db.execute('UPDATE recovery_job SET status=?,reason=? WHERE id=?', (status, reason, job_id))
        db.execute('INSERT INTO recovery_event(job_id,status,reason) VALUES(?,?,?)', (job_id, status, reason))


def _schedule_all(db):
    workers = db.execute('SELECT * FROM recovery_worker ORDER BY id').fetchall()
    for worker in workers:
        jobs = db.execute('SELECT j.*,s.revision AS current_revision,s.status AS slot_status '
                          'FROM recovery_job j JOIN feed_slot s ON s.id=j.slot_id '
                          'WHERE j.worker_id=? AND j.status NOT IN (\'completed\',\'stale\',\'closed\') '
                          'ORDER BY j.priority DESC,j.id', (worker['id'],)).fetchall()
        remaining = worker['capacity']
        for job in jobs:
            if job['slot_status'] == 'accepted':
                _set_job(db, job['id'], 'closed', 'source delivery accepted; recovery unnecessary')
            elif job['revision'] != job['current_revision']:
                _set_job(db, job['id'], 'stale', 'feed evidence revision changed; request again')
            elif job['units'] <= remaining:
                _set_job(db, job['id'], 'active', 'worker capacity reserved')
                remaining -= job['units']
            else:
                _set_job(db, job['id'], 'waiting', 'capacity held by higher-priority work')


def request_recovery(db_path, path):
    item, _, _ = _load(path)
    job, slot, worker = str(item['job_id']), str(item['slot']), str(item['worker'])
    priority, units, reason = int(item['priority']), int(item['units']), str(item['reason'])
    if not all((job, slot, worker, reason)) or units < 1:
        raise ValueError('invalid recovery request')
    with closing(connect(db_path)) as db, db:
        source = db.execute('SELECT partition,revision,status FROM feed_slot WHERE id=?', (slot,)).fetchone()
        capacity = db.execute('SELECT partition,capacity FROM recovery_worker WHERE id=?', (worker,)).fetchone()
        if not source or not capacity or source['partition'] != capacity['partition']:
            raise ValueError('unknown slot/worker or partition mismatch')
        if source['status'] == 'accepted' or units > capacity['capacity']:
            raise ValueError('accepted source or request exceeds worker capacity')
        old = db.execute('SELECT slot_id,worker_id,priority,units,revision,reason,status FROM recovery_job WHERE id=?',
                         (job,)).fetchone()
        if old:
            if tuple(old[key] for key in ('slot_id','worker_id','priority','units','revision')) != (
                    slot, worker, priority, units, source['revision']):
                raise ValueError('recovery job id reused with changed request')
            return {'job_id': job, 'status': old['status'], 'idempotent': True}
        db.execute('INSERT INTO recovery_job VALUES(?,?,?,?,?,?,?,?)',
                   (job, slot, worker, priority, units, source['revision'], 'waiting', reason))
        db.execute('INSERT INTO recovery_event(job_id,status,reason) VALUES(?,?,?)',
                   (job, 'waiting', 'request accepted'))
        _schedule_all(db)
        status = db.execute('SELECT status FROM recovery_job WHERE id=?', (job,)).fetchone()[0]
    return {'job_id': job, 'status': status, 'bound_revision': source['revision']}


def complete_recovery(db_path, job):
    with closing(connect(db_path)) as db, db:
        row = db.execute('SELECT status FROM recovery_job WHERE id=?', (job,)).fetchone()
        if not row or row['status'] != 'active':
            raise ValueError('only an active recovery job can complete')
        _set_job(db, job, 'completed', 'worker released after evidence acquisition')
        _schedule_all(db)
    return {'job_id': job, 'status': 'completed'}


def supply_report(db_path):
    with closing(connect(db_path)) as db, db:
        return {
            'slots': [dict(x) for x in db.execute('SELECT * FROM feed_slot ORDER BY id')],
            'observations': [dict(x) for x in db.execute('SELECT event_id,slot_id,producer,digest,row_count,observed_at,source_sha FROM feed_observation ORDER BY event_id')],
            'lineage_events': [dict(x) for x in db.execute('SELECT event_id,producer,version,source_domain,ancestry,root_origin,attested_by,proof_sha,source_sha FROM feed_lineage_event ORDER BY producer,version')],
            'lineage_heads': [dict(x) for x in db.execute('SELECT * FROM feed_lineage_head ORDER BY producer')],
            'verdicts': [dict(x) for x in db.execute('SELECT * FROM feed_verdict ORDER BY id')],
            'workers': [dict(x) for x in db.execute('SELECT * FROM recovery_worker ORDER BY id')],
            'jobs': [dict(x) for x in db.execute('SELECT * FROM recovery_job ORDER BY id')],
            'events': [dict(x) for x in db.execute('SELECT * FROM recovery_event ORDER BY id')],
        }
