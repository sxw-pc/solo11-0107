"""Purpose-limited consent over frozen replay values and recall of prior exports.

Consent versions describe a subject's authority; they do not alter replay, target,
repair, or report ledgers. A late withdrawal creates a new export recall instead
of rewriting the old exported bytes.
"""
import hashlib
import json
from pathlib import Path
from .store import connect


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def import_consent(path, file):
    raw=Path(file).read_bytes()
    digest=_sha(raw)
    data=json.loads(raw.decode('utf-8'))
    db=connect(path)
    try:
        try:
            with db:
                old=db.execute('SELECT * FROM consent_source WHERE sha=?',(digest,)).fetchone()
                if old and old['status']=='accepted':return old['id']
                if old:raise ValueError('previously_rejected_consent_source')
                source=db.execute("INSERT INTO consent_source(sha,raw,status) VALUES (?,?,'pending')",(digest,raw)).lastrowid
                for item in data.get('subjects',[]):
                    sid=str(item['id']);owner=str(item['owner'])
                    if not sid or not owner or not item['keys']:raise ValueError('invalid_subject')
                    prior=db.execute('SELECT owner FROM data_subject WHERE id=?',(sid,)).fetchone()
                    if prior and prior['owner']!=owner:raise ValueError('subject_owner_conflict')
                    db.execute('INSERT OR IGNORE INTO data_subject VALUES (?,?)',(sid,owner))
                    for key in item['keys']:
                        assigned=db.execute('SELECT subject_id FROM subject_key WHERE key=?',(key,)).fetchone()
                        if assigned and assigned['subject_id']!=sid:raise ValueError('subject_key_conflict')
                        db.execute('INSERT OR IGNORE INTO subject_key VALUES (?,?)',(sid,str(key)))
                for e in data.get('events',[]):
                    sid=str(e['subject']);purpose=str(e['purpose'])
                    if not purpose.strip() or e['action'] not in {'grant','revoke'} or len(str(e['proof_sha']))<8:
                        raise ValueError('invalid_consent_evidence')
                    if not db.execute('SELECT 1 FROM data_subject WHERE id=?',(sid,)).fetchone():raise ValueError('unknown_subject')
                    version=int(e['version']);effective=int(e['effective_watermark'])
                    expires=e.get('expires_watermark')
                    if version<1 or effective<0 or (expires is not None and int(expires)<effective):raise ValueError('invalid_consent_version')
                    values=(sid,purpose,version,e['action'],effective,expires,str(e['proof_sha']))
                    prior=db.execute('SELECT * FROM consent_event WHERE id=? OR (subject_id=? AND purpose=? AND version=?)',
                                     (e['id'],sid,purpose,version)).fetchone()
                    if prior:
                        if prior['id']!=e['id'] or tuple(prior[k] for k in ('subject_id','purpose','version','action','effective_watermark','expires_watermark','proof_sha'))!=values:
                            raise ValueError('consent_version_conflict')
                        continue
                    last=db.execute('SELECT MAX(version) FROM consent_event WHERE subject_id=? AND purpose=?',(sid,purpose)).fetchone()[0]
                    if version!=(last or 0)+1:raise ValueError('consent_version_gap')
                    db.execute('INSERT INTO consent_event VALUES (?,?,?,?,?,?,?,?,?)',(e['id'],*values,source))
                # Withdrawal changes the licence of already materialized exports
                # in the same transaction as the new consent version.
                for export in db.execute('SELECT id FROM controlled_export').fetchall():
                    _reconcile_export(db,export['id'])
                db.execute("UPDATE consent_source SET status='accepted' WHERE id=?",(source,))
                return source
        except Exception as exc:
            with db:
                db.execute("INSERT OR IGNORE INTO consent_source(sha,raw,status,error) VALUES (?,?,'rejected',?)",(digest,raw,str(exc)))
            raise
    finally:db.close()


def _authority(db,subject,purpose,watermark):
    row=db.execute('''SELECT * FROM consent_event WHERE subject_id=? AND purpose=?
      AND effective_watermark<=? ORDER BY version DESC LIMIT 1''',(subject,purpose,watermark)).fetchone()
    if not row:return None,'no_grant'
    if row['action']=='revoke':return row,'withdrawn'
    if row['expires_watermark'] is not None and watermark>row['expires_watermark']:return row,'expired'
    return row,'authorized'


def create_export(path,run_id,purpose,recipient):
    if not purpose.strip() or not recipient.strip():raise ValueError('purpose_and_recipient_required')
    db=connect(path)
    try:
        with db:
            run=db.execute('SELECT * FROM replay_run WHERE id=?',(run_id,)).fetchone()
            if not run or run['status'] not in {'已判定','已归档'}:raise ValueError('run_not_final')
            candidates=db.execute('SELECT * FROM visible_row WHERE run_id=? ORDER BY key',(run_id,)).fetchall()
            rows=[];excluded=[]
            for row in candidates:
                subject=db.execute('SELECT subject_id FROM subject_key WHERE key=?',(row['key'],)).fetchone()
                if row['status']!='存在':reason='replay_not_visible'
                elif not subject:reason='unmapped_subject'
                else:
                    event,reason=_authority(db,subject['subject_id'],purpose,run['watermark'])
                if reason=='authorized':rows.append((row['key'],subject['subject_id'],row['value'],event['id']))
                else:excluded.append((row['key'],reason))
            content=_sha(json.dumps(rows,ensure_ascii=False,sort_keys=True).encode('utf-8'))
            status='complete' if rows and not excluded else 'partial' if rows else 'empty'
            eid=db.execute('INSERT INTO controlled_export(run_id,purpose,recipient,status,content_sha) VALUES (?,?,?,?,?)',
                           (run_id,purpose,recipient,status,content)).lastrowid
            db.executemany('INSERT INTO export_row VALUES (?,?,?,?,?)',[(eid,*r) for r in rows])
            db.executemany('INSERT INTO export_exclusion VALUES (?,?,?)',[(eid,*r) for r in excluded])
            return eid,status
    finally:db.close()


def _reconcile_export(db,export_id):
    export=db.execute('SELECT * FROM controlled_export WHERE id=?',(export_id,)).fetchone()
    if not export:raise ValueError('unknown_export')
    run=db.execute('SELECT watermark FROM replay_run WHERE id=?',(export['run_id'],)).fetchone()
    for row in db.execute('SELECT * FROM export_row WHERE export_id=?',(export_id,)).fetchall():
        original=db.execute('SELECT version FROM consent_event WHERE id=?',(row['consent_event_id'],)).fetchone()
        revocations=db.execute('''SELECT * FROM consent_event WHERE subject_id=? AND purpose=? AND version>?
          AND action='revoke' AND effective_watermark<=? ORDER BY version''',
          (row['subject_id'],export['purpose'],original['version'],run['watermark'])).fetchall()
        for rev in revocations:
            db.execute("INSERT OR IGNORE INTO export_recall(export_id,key,revocation_id,status,reason) VALUES (?,?,?,'pending','late_withdrawal')",
                       (export_id,row['key'],rev['id']))
    return [r['id'] for r in db.execute('SELECT id FROM export_recall WHERE export_id=? ORDER BY id',(export_id,))]


def audit_withdrawals(path,export_id):
    db=connect(path)
    try:
        with db:return _reconcile_export(db,export_id)
    finally:db.close()


def acknowledge_recall(path,recall_id,recipient,proof_sha):
    if len(proof_sha)<8:raise ValueError('recall_proof_required')
    db=connect(path)
    try:
        with db:
            recall=db.execute('SELECT * FROM export_recall WHERE id=?',(recall_id,)).fetchone()
            if not recall:raise ValueError('unknown_recall')
            export=db.execute('SELECT recipient FROM controlled_export WHERE id=?',(recall['export_id'],)).fetchone()
            if recipient!=export['recipient']:raise ValueError('wrong_export_recipient')
            old=db.execute('SELECT * FROM recall_receipt WHERE recall_id=? AND recipient=?',(recall_id,recipient)).fetchone()
            if old:
                if old['proof_sha']!=proof_sha:raise ValueError('recall_receipt_conflict')
                return old['id']
            receipt=db.execute('INSERT INTO recall_receipt(recall_id,recipient,proof_sha) VALUES (?,?,?)',
                               (recall_id,recipient,proof_sha)).lastrowid
            db.execute("UPDATE export_recall SET status='acknowledged' WHERE id=?",(recall_id,))
            return receipt
    finally:db.close()


def export_report(path,export_id):
    db=connect(path)
    try:
        export=db.execute('SELECT * FROM controlled_export WHERE id=?',(export_id,)).fetchone()
        if not export:raise ValueError('unknown_export')
        recalls=[dict(r) for r in db.execute('SELECT * FROM export_recall WHERE export_id=? ORDER BY id',(export_id,))]
        permission='recall_pending' if any(r['status']=='pending' for r in recalls) else 'recalled' if recalls else 'valid'
        return {'export':dict(export),'current_permission':permission,
                'rows':[dict(r) for r in db.execute('SELECT * FROM export_row WHERE export_id=? ORDER BY key',(export_id,))],
                'excluded':[dict(r) for r in db.execute('SELECT * FROM export_exclusion WHERE export_id=? ORDER BY key',(export_id,))],
                'recalls':recalls}
    finally:db.close()
