"""Independent source-delivery arbitration and recovery-capacity ledger.

These decisions concern whether upstream data was delivered and which acquisition
work can run.  They do not inspect or mutate CDC replay runs.
"""

import hashlib
import json
from contextlib import closing
from pathlib import Path

from .store import connect


def _load(path):
    raw = Path(path).read_bytes()
    return json.loads(raw), raw, hashlib.sha256(raw).hexdigest()


def register_slot(db_path, path):
    item, _, _ = _load(path)
    slot, partition = str(item['slot']), str(item['partition'])
    deadline, quorum = str(item['deadline']), int(item['quorum'])
    if not slot or not partition or not deadline or quorum < 2:
        raise ValueError('slot requires partition, deadline and quorum >= 2')
    with closing(connect(db_path)) as db, db:
        old = db.execute('SELECT * FROM feed_slot WHERE id=?', (slot,)).fetchone()
        if old:
            if (old['partition'], old['deadline'], old['quorum']) != (partition, deadline, quorum):
                raise ValueError('slot contract changed; register a new slot')
            return {'slot': slot, 'status': old['status'], 'revision': old['revision']}
        db.execute('INSERT INTO feed_slot(id,partition,deadline,quorum) VALUES(?,?,?,?)',
                   (slot, partition, deadline, quorum))
    return {'slot': slot, 'status': 'missing', 'revision': 0}


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


def _arbitrate(db, slot):
    row = db.execute('SELECT * FROM feed_slot WHERE id=?', (slot,)).fetchone()
    observations = db.execute('SELECT o.producer,o.digest,o.event_id,l.root_origin,l.version AS lineage_version '
                              'FROM feed_observation o LEFT JOIN feed_lineage_head l ON l.producer=o.producer '
                              'WHERE o.slot_id=? ORDER BY o.event_id',
                              (slot,)).fetchall()
    by_digest = {}
    unverified = []
    lineage_evidence = {}
    for observation in observations:
        if not observation['root_origin']:
            unverified.append(observation['event_id'])
            continue
        by_digest.setdefault(observation['digest'], set()).add(observation['root_origin'])
        lineage_evidence[observation['producer']] = {'root_origin': observation['root_origin'],
                                                     'version': observation['lineage_version']}
    if len(by_digest) > 1:
        status, selected = 'disputed', None
    elif not by_digest:
        status, selected = ('unverified' if unverified else 'missing'), None
    else:
        digest, producers = next(iter(by_digest.items()))
        status, selected = ('accepted', digest) if len(producers) >= row['quorum'] else ('collecting', None)
    revision = row['revision'] + 1
    evidence = {'origins_by_digest': {digest: sorted(origins) for digest, origins in sorted(by_digest.items())},
                'producer_lineage': lineage_evidence, 'unverified_events': unverified}
    db.execute('UPDATE feed_slot SET revision=?,status=?,selected_digest=? WHERE id=?',
               (revision, status, selected, slot))
    db.execute('INSERT INTO feed_verdict(slot_id,revision,status,selected_digest,evidence) VALUES(?,?,?,?,?)',
               (slot, revision, status, selected, json.dumps(evidence, sort_keys=True)))
    return {'slot': slot, 'revision': revision, 'status': status, 'selected_digest': selected,
            'independent_origins': evidence['origins_by_digest'], 'lineage': lineage_evidence,
            'unverified_events': unverified}


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
