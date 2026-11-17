"""Durable execution ownership for recovery jobs, separate from feed arbitration.

All mutations take a SQLite write lock before reading the clock or lease. A token
is the task ID and monotonically increasing generation; it fences an old worker
even when a later attempt happens to return to the same worker.
"""

import json
import hashlib
from contextlib import closing
from pathlib import Path

from .store import connect
from .supply import _schedule_all, _set_job


def _transaction(db):
    db.execute('BEGIN IMMEDIATE')
    db.execute('INSERT OR IGNORE INTO dispatch_clock(id,tick) VALUES(1,0)')


def _clock(db, tick):
    tick = int(tick)
    previous = db.execute('SELECT tick FROM dispatch_clock WHERE id=1').fetchone()[0]
    if tick < previous:
        raise ValueError('dispatch clock cannot move backwards')
    db.execute('UPDATE dispatch_clock SET tick=? WHERE id=1', (tick,))
    return tick


def _event(db, task, tick, action, worker, generation, detail):
    db.execute('INSERT INTO dispatch_event(task_id,tick,action,worker_id,generation,detail) '
               'VALUES(?,?,?,?,?,?)', (task, tick, action, worker, generation, detail))


def register_dispatch_worker(db_path, path):
    item = json.loads(Path(path).read_text(encoding='utf-8'))
    worker, partition, capacity = str(item['worker']), str(item['partition']), int(item['capacity'])
    if not worker or not partition or capacity < 1:
        raise ValueError('dispatch worker requires partition and positive capacity')
    with closing(connect(db_path)) as db, db:
        _transaction(db)
        old = db.execute('SELECT partition,capacity FROM dispatch_worker WHERE id=?', (worker,)).fetchone()
        if old and (old['partition'], old['capacity']) != (partition, capacity):
            raise ValueError('dispatch worker contract changed')
        db.execute('INSERT OR IGNORE INTO dispatch_worker VALUES(?,?,?)', (worker, partition, capacity))
        db.execute('INSERT OR IGNORE INTO dispatch_cursor(partition) VALUES(?)', (partition,))
    return {'worker': worker, 'partition': partition, 'capacity': capacity}


def queue_dispatch(db_path, path):
    item = json.loads(Path(path).read_text(encoding='utf-8'))
    task, job, tenant = (str(item[key]) for key in ('task_id', 'job_id', 'tenant'))
    lease_ticks = int(item['lease_ticks'])
    if not task or not job or not tenant or lease_ticks < 1:
        raise ValueError('dispatch task requires job, tenant and positive lease_ticks')
    with closing(connect(db_path)) as db, db:
        _transaction(db)
        row = db.execute('SELECT j.*,s.partition,s.revision AS current_revision,s.status AS slot_status '
                         'FROM recovery_job j JOIN feed_slot s ON s.id=j.slot_id WHERE j.id=?', (job,)).fetchone()
        if not row or row['status'] not in ('active', 'waiting') or row['revision'] != row['current_revision'] or row['slot_status'] == 'accepted':
            raise ValueError('dispatch requires a live recovery job')
        old = db.execute('SELECT * FROM dispatch_task WHERE id=? OR job_id=?', (task, job)).fetchone()
        if old:
            if (old['id'], old['job_id'], old['tenant'], old['lease_ticks']) != (task, job, tenant, lease_ticks):
                raise ValueError('task or job already has a different dispatch contract')
            return {'task_id': task, 'status': old['status'], 'idempotent': True}
        db.execute('INSERT INTO dispatch_task(id,job_id,tenant,partition,lease_ticks) VALUES(?,?,?,?,?)',
                   (task, job, tenant, row['partition'], lease_ticks))
        tick = db.execute('SELECT tick FROM dispatch_clock WHERE id=1').fetchone()[0]
        _event(db, task, tick, 'queued', None, 0, 'recovery execution requested')
    return {'task_id': task, 'status': 'pending'}


def _live(db, task):
    job = db.execute('SELECT j.status,j.revision,s.revision AS current_revision,s.status AS slot_status '
                     'FROM dispatch_task t JOIN recovery_job j ON j.id=t.job_id '
                     'JOIN feed_slot s ON s.id=j.slot_id WHERE t.id=?', (task['id'],)).fetchone()
    return job and job['status'] in ('active', 'waiting') and job['revision'] == job['current_revision'] and job['slot_status'] != 'accepted'


def dispatch_tick(db_path, tick):
    with closing(connect(db_path)) as db, db:
        _transaction(db)
        tick = _clock(db, tick)
        for task in db.execute("SELECT * FROM dispatch_task WHERE status IN ('pending','leased') ORDER BY id").fetchall():
            if not _live(db, task):
                db.execute("UPDATE dispatch_task SET status='canceled',worker_id=NULL,expires_at=NULL WHERE id=?", (task['id'],))
                _event(db, task['id'], tick, 'canceled', task['worker_id'], task['generation'], 'recovery job no longer live')
            elif task['status'] == 'leased' and task['expires_at'] <= tick:
                db.execute("UPDATE dispatch_task SET status='pending',expires_at=NULL WHERE id=?", (task['id'],))
                _event(db, task['id'], tick, 'expired', task['worker_id'], task['generation'], 'lease expired; prior token fenced')
        assigned = []
        workers = db.execute('SELECT * FROM dispatch_worker ORDER BY partition,id').fetchall()
        for worker in workers:
            used = db.execute("SELECT COUNT(*) FROM dispatch_task WHERE status='leased' AND worker_id=?", (worker['id'],)).fetchone()[0]
            for _ in range(worker['capacity'] - used):
                candidates = db.execute("SELECT * FROM dispatch_task WHERE status='pending' AND partition=? ORDER BY id", (worker['partition'],)).fetchall()
                if not candidates:
                    break
                last = db.execute('SELECT last_tenant FROM dispatch_cursor WHERE partition=?', (worker['partition'],)).fetchone()[0]
                tenants = sorted({task['tenant'] for task in candidates})
                tenant = next((name for name in tenants if name > last), tenants[0])
                eligible = [task for task in candidates if task['tenant'] == tenant]
                # A timed-out attempt migrates when another worker has a free slot.
                task = next((task for task in eligible if task['worker_id'] != worker['id']), eligible[0])
                if task['worker_id'] == worker['id']:
                    other_free = any(
                        peer['id'] != worker['id'] and peer['partition'] == worker['partition'] and
                        db.execute("SELECT COUNT(*) FROM dispatch_task WHERE status='leased' AND worker_id=?", (peer['id'],)).fetchone()[0] < peer['capacity']
                        for peer in workers
                    )
                    if other_free:
                        break
                generation = task['generation'] + 1
                db.execute("UPDATE dispatch_task SET status='leased',generation=?,worker_id=?,expires_at=? WHERE id=?",
                           (generation, worker['id'], tick + task['lease_ticks'], task['id']))
                db.execute('UPDATE dispatch_cursor SET last_tenant=? WHERE partition=?', (tenant, worker['partition']))
                _event(db, task['id'], tick, 'leased', worker['id'], generation, f'expires at {tick + task["lease_ticks"]}')
                assigned.append({'task_id': task['id'], 'worker': worker['id'], 'generation': generation,
                                 'expires_at': tick + task['lease_ticks'], 'tenant': tenant})
    return assigned


def complete_dispatch(db_path, task_id, worker_id, generation, tick, artifact_path):
    raw = Path(artifact_path).read_bytes()
    if not raw:
        raise ValueError('completion requires nonempty artifact evidence')
    artifact_sha = hashlib.sha256(raw).hexdigest()
    with closing(connect(db_path)) as db, db:
        _transaction(db)
        tick = _clock(db, tick)
        task = db.execute('SELECT * FROM dispatch_task WHERE id=?', (task_id,)).fetchone()
        if (not task or task['status'] != 'leased' or task['worker_id'] != worker_id or
                task['generation'] != int(generation) or tick >= task['expires_at']):
            raise ValueError('stale or expired dispatch token')
        if not _live(db, task):
            raise ValueError('recovery job changed before completion')
        db.execute("UPDATE dispatch_task SET status='completed',artifact_sha=?,expires_at=NULL WHERE id=?",
                   (artifact_sha, task_id))
        db.execute('INSERT INTO dispatch_artifact(task_id,source_sha,raw) VALUES(?,?,?)',
                   (task_id, artifact_sha, raw))
        _event(db, task_id, tick, 'completed', worker_id, generation, artifact_sha)
        _set_job(db, task['job_id'], 'completed', f'dispatch artifact {artifact_sha}')
        _schedule_all(db)
    return {'task_id': task_id, 'job_id': task['job_id'], 'status': 'completed', 'artifact_sha': artifact_sha}


def renew_dispatch(db_path, task_id, worker_id, generation, tick):
    with closing(connect(db_path)) as db, db:
        _transaction(db)
        tick = _clock(db, tick)
        task = db.execute('SELECT * FROM dispatch_task WHERE id=?', (task_id,)).fetchone()
        if (not task or task['status'] != 'leased' or task['worker_id'] != worker_id or
                task['generation'] != int(generation) or tick >= task['expires_at']):
            raise ValueError('stale or expired dispatch token')
        if not _live(db, task):
            raise ValueError('recovery job changed before renewal')
        expires_at = tick + task['lease_ticks']
        db.execute('UPDATE dispatch_task SET expires_at=? WHERE id=?', (expires_at, task_id))
        _event(db, task_id, tick, 'renewed', worker_id, generation, f'expires at {expires_at}')
    return {'task_id': task_id, 'worker': worker_id, 'generation': generation, 'expires_at': expires_at}


def dispatch_report(db_path):
    with closing(connect(db_path)) as db:
        return {
            'workers': [dict(x) for x in db.execute('SELECT * FROM dispatch_worker ORDER BY id')],
            'tasks': [dict(x) for x in db.execute('SELECT * FROM dispatch_task ORDER BY id')],
            'events': [dict(x) for x in db.execute('SELECT * FROM dispatch_event ORDER BY id')],
            'artifacts': [dict(x) for x in db.execute('SELECT task_id,source_sha,length(raw) AS bytes FROM dispatch_artifact ORDER BY task_id')],
            'clock': dict(db.execute('SELECT * FROM dispatch_clock WHERE id=1').fetchone() or {}),
            'cursors': [dict(x) for x in db.execute('SELECT * FROM dispatch_cursor ORDER BY partition')],
        }
