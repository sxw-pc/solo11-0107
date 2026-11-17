import argparse,json,tempfile
from pathlib import Path
from .ingest import snapshot,ingest
from .rules import replay
from .report import write
from .cutover import import_target,audit_target,configure_consumers,acknowledge,release_decision
from .repair import plan_repair,apply_repair,repair_report
from .consent import import_consent,create_export,audit_withdrawals,acknowledge_recall,export_report
from .store import connect
def demo(workdir=None):
    root=Path(workdir or tempfile.mkdtemp(prefix='cdcverdict-'));root.mkdir(parents=True,exist_ok=True)
    base=root/'base.jsonl';base.write_text(json.dumps({'key':'A','value':{'n':1}}),encoding='utf-8')
    events=root/'events.jsonl';events.write_text('\n'.join(json.dumps(x) for x in [
      {'kind':'begin','txn':'T1','seq':1},
      {'kind':'change','txn':'T1','seq':2,'key':'A','action':'update','before':{'n':1},'after':{'n':2}},
      {'kind':'commit','txn':'T1','seq':3}]),encoding='utf-8')
    db=root/'data.db';snapshot(base,db,0);ingest(events,db);run,_,_=replay(db,3)
    return write(db,run,root/'report')
def decision_demo(workdir=None):
    root=Path(workdir or tempfile.mkdtemp(prefix='cdcverdict-decision-'));root.mkdir(parents=True,exist_ok=True)
    demo(root);db=root/'data.db'
    run=connect(db).execute('SELECT MAX(id) FROM replay_run').fetchone()[0]
    target=root/'target.jsonl';target.write_text(json.dumps({'key':'A','value':{'n':2}}),encoding='utf-8')
    tid=import_target(db,target,3);aid,status,_=audit_target(db,run,tid)
    consumers=root/'consumers.json';consumers.write_text(json.dumps([{'id':'ledger','critical':True,'minimum_watermark':3}]),encoding='utf-8')
    configure_consumers(db,consumers)
    sha=connect(db).execute('SELECT source_sha FROM target_snapshot WHERE id=?',(tid,)).fetchone()[0]
    acknowledge(db,'ledger',run,1,'accepted',sha)
    gate,release,_=release_decision(db,aid)
    return {'cutover_audit':aid,'target_status':status,'release_gate':gate,'release_status':release,'db':str(db)}
def repair_demo(workdir=None):
    root=Path(workdir or tempfile.mkdtemp(prefix='cdcverdict-repair-'));root.mkdir(parents=True,exist_ok=True)
    demo(root);db=root/'data.db';run=connect(db).execute('SELECT MAX(id) FROM replay_run').fetchone()[0]
    target=root/'divergent.jsonl';target.write_text('\n'.join(json.dumps(row) for row in [
        {'key':'A','value':{'n':9}}, {'key':'B','value':{'n':7}}]),encoding='utf-8')
    tid=import_target(db,target,3);audit,initial,_=audit_target(db,run,tid)
    first=root/'first-repair.json';first.write_text(json.dumps([
        {'key':'A','decision':'restore_replay','reason':'replay evidence wins'},
        {'key':'B','decision':'accept_target','reason':'await owner review'}]),encoding='utf-8')
    p1=plan_repair(db,audit,first);t1,result1=apply_repair(db,p1);a1,mid,_=audit_target(db,run,t1)
    second=root/'second-repair.json';second.write_text(json.dumps([
        {'key':'B','decision':'restore_replay','reason':'unowned target row'}]),encoding='utf-8')
    p2=plan_repair(db,a1,second);t2,result2=apply_repair(db,p2);a2,final,_=audit_target(db,run,t2)
    return {'initial':initial,'first_result':result1,'intermediate':mid,'second_result':result2,
            'final':final,'target':t2,'audit':a2,'db':str(db)}
def consent_demo(workdir=None):
    root=Path(workdir or tempfile.mkdtemp(prefix='cdcverdict-consent-'));root.mkdir(parents=True,exist_ok=True)
    demo(root);db=root/'data.db'
    run=connect(db).execute('SELECT MAX(id) FROM replay_run').fetchone()[0]
    grant=root/'grant.json';grant.write_text(json.dumps({'subjects':[{'id':'person-1','owner':'owner-1','keys':['A']}],
        'events':[{'id':'grant-1','subject':'person-1','purpose':'research','version':1,'action':'grant',
                   'effective_watermark':0,'proof_sha':'grant-proof-1'}]}),encoding='utf-8')
    import_consent(db,grant);first,status=create_export(db,run,'research','research-team')
    withdraw=root/'withdraw.json';withdraw.write_text(json.dumps({'events':[{'id':'withdraw-2','subject':'person-1',
        'purpose':'research','version':2,'action':'revoke','effective_watermark':3,'proof_sha':'withdraw-proof-2'}]}),encoding='utf-8')
    import_consent(db,withdraw);recalls=audit_withdrawals(db,first)
    second,status_after=create_export(db,run,'research','research-team')
    receipt=acknowledge_recall(db,recalls[0],'research-team','received-proof-1')
    return {'db':str(db),'run':run,'first_export':first,'initial_status':status,'recalls':recalls,
            'recall_receipt':receipt,'second_export':second,'after_status':status_after}
def main():
    p=argparse.ArgumentParser(description='离线 CDC 事务回放与可见性裁定');s=p.add_subparsers(dest='cmd',required=True)
    d=s.add_parser('demo');d.add_argument('--workdir')
    dd=s.add_parser('demo-decisions');dd.add_argument('--workdir')
    dr=s.add_parser('demo-repair');dr.add_argument('--workdir')
    dc=s.add_parser('demo-consent');dc.add_argument('--workdir')
    i=s.add_parser('init');i.add_argument('snapshot');i.add_argument('--db',required=True);i.add_argument('--watermark',type=int,required=True)
    g=s.add_parser('ingest');g.add_argument('events');g.add_argument('--db',required=True)
    v=s.add_parser('verdict');v.add_argument('--db',required=True);v.add_argument('--watermark',type=int,required=True)
    r=s.add_parser('report');r.add_argument('--db',required=True);r.add_argument('--run',type=int,required=True);r.add_argument('--out',required=True)
    t=s.add_parser('target-import');t.add_argument('file');t.add_argument('--db',required=True);t.add_argument('--watermark',type=int,required=True)
    ca=s.add_parser('cutover-audit');ca.add_argument('--db',required=True);ca.add_argument('--run',type=int,required=True);ca.add_argument('--target',type=int,required=True)
    cc=s.add_parser('consumer-contract');cc.add_argument('file');cc.add_argument('--db',required=True)
    ack=s.add_parser('consumer-ack');ack.add_argument('--db',required=True);ack.add_argument('--consumer',required=True);ack.add_argument('--run',type=int,required=True);ack.add_argument('--sequence',type=int,required=True);ack.add_argument('--status',required=True);ack.add_argument('--target-sha',required=True)
    gate=s.add_parser('release-gate');gate.add_argument('--db',required=True);gate.add_argument('--audit',type=int,required=True)
    rp=s.add_parser('repair-plan');rp.add_argument('manifest');rp.add_argument('--db',required=True);rp.add_argument('--audit',type=int,required=True)
    ra=s.add_parser('repair-apply');ra.add_argument('--db',required=True);ra.add_argument('--plan',type=int,required=True)
    rr=s.add_parser('repair-report');rr.add_argument('--db',required=True);rr.add_argument('--plan',type=int,required=True)
    ci=s.add_parser('consent-import');ci.add_argument('file');ci.add_argument('--db',required=True)
    ec=s.add_parser('export-create');ec.add_argument('--db',required=True);ec.add_argument('--run',type=int,required=True);ec.add_argument('--purpose',required=True);ec.add_argument('--recipient',required=True)
    ea=s.add_parser('export-audit');ea.add_argument('--db',required=True);ea.add_argument('--export',type=int,required=True)
    ackr=s.add_parser('recall-ack');ackr.add_argument('--db',required=True);ackr.add_argument('--recall',type=int,required=True);ackr.add_argument('--recipient',required=True);ackr.add_argument('--proof-sha',required=True)
    er=s.add_parser('export-report');er.add_argument('--db',required=True);er.add_argument('--export',type=int,required=True)
    a=p.parse_args()
    if a.cmd=='demo':print(demo(a.workdir))
    elif a.cmd=='demo-decisions':print(decision_demo(a.workdir))
    elif a.cmd=='demo-repair':print(repair_demo(a.workdir))
    elif a.cmd=='demo-consent':print(consent_demo(a.workdir))
    elif a.cmd=='init':print(snapshot(a.snapshot,a.db,a.watermark))
    elif a.cmd=='ingest':print(ingest(a.events,a.db))
    elif a.cmd=='verdict':print(replay(a.db,a.watermark))
    elif a.cmd=='report':print(write(a.db,a.run,a.out))
    elif a.cmd=='target-import':print(import_target(a.db,a.file,a.watermark))
    elif a.cmd=='cutover-audit':print(audit_target(a.db,a.run,a.target))
    elif a.cmd=='consumer-contract':print(configure_consumers(a.db,a.file))
    elif a.cmd=='consumer-ack':print(acknowledge(a.db,a.consumer,a.run,a.sequence,a.status,a.target_sha))
    elif a.cmd=='release-gate':print(release_decision(a.db,a.audit))
    elif a.cmd=='repair-plan':print(plan_repair(a.db,a.audit,a.manifest))
    elif a.cmd=='repair-apply':print(apply_repair(a.db,a.plan))
    elif a.cmd=='repair-report':print(json.dumps(repair_report(a.db,a.plan),ensure_ascii=False,indent=2))
    elif a.cmd=='consent-import':print(import_consent(a.db,a.file))
    elif a.cmd=='export-create':print(create_export(a.db,a.run,a.purpose,a.recipient))
    elif a.cmd=='export-audit':print(audit_withdrawals(a.db,a.export))
    elif a.cmd=='recall-ack':print(acknowledge_recall(a.db,a.recall,a.recipient,a.proof_sha))
    elif a.cmd=='export-report':print(json.dumps(export_report(a.db,a.export),ensure_ascii=False,indent=2))
if __name__=='__main__':main()
