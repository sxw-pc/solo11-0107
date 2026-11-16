import argparse,json,tempfile
from pathlib import Path
from .ingest import snapshot,ingest
from .rules import replay
from .report import write
from .cutover import import_target,audit_target,configure_consumers,acknowledge,release_decision
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
def main():
    p=argparse.ArgumentParser(description='离线 CDC 事务回放与可见性裁定');s=p.add_subparsers(dest='cmd',required=True)
    d=s.add_parser('demo');d.add_argument('--workdir')
    dd=s.add_parser('demo-decisions');dd.add_argument('--workdir')
    i=s.add_parser('init');i.add_argument('snapshot');i.add_argument('--db',required=True);i.add_argument('--watermark',type=int,required=True)
    g=s.add_parser('ingest');g.add_argument('events');g.add_argument('--db',required=True)
    v=s.add_parser('verdict');v.add_argument('--db',required=True);v.add_argument('--watermark',type=int,required=True)
    r=s.add_parser('report');r.add_argument('--db',required=True);r.add_argument('--run',type=int,required=True);r.add_argument('--out',required=True)
    t=s.add_parser('target-import');t.add_argument('file');t.add_argument('--db',required=True);t.add_argument('--watermark',type=int,required=True)
    ca=s.add_parser('cutover-audit');ca.add_argument('--db',required=True);ca.add_argument('--run',type=int,required=True);ca.add_argument('--target',type=int,required=True)
    cc=s.add_parser('consumer-contract');cc.add_argument('file');cc.add_argument('--db',required=True)
    ack=s.add_parser('consumer-ack');ack.add_argument('--db',required=True);ack.add_argument('--consumer',required=True);ack.add_argument('--run',type=int,required=True);ack.add_argument('--sequence',type=int,required=True);ack.add_argument('--status',required=True);ack.add_argument('--target-sha',required=True)
    gate=s.add_parser('release-gate');gate.add_argument('--db',required=True);gate.add_argument('--audit',type=int,required=True)
    a=p.parse_args()
    if a.cmd=='demo':print(demo(a.workdir))
    elif a.cmd=='demo-decisions':print(decision_demo(a.workdir))
    elif a.cmd=='init':print(snapshot(a.snapshot,a.db,a.watermark))
    elif a.cmd=='ingest':print(ingest(a.events,a.db))
    elif a.cmd=='verdict':print(replay(a.db,a.watermark))
    elif a.cmd=='report':print(write(a.db,a.run,a.out))
    elif a.cmd=='target-import':print(import_target(a.db,a.file,a.watermark))
    elif a.cmd=='cutover-audit':print(audit_target(a.db,a.run,a.target))
    elif a.cmd=='consumer-contract':print(configure_consumers(a.db,a.file))
    elif a.cmd=='consumer-ack':print(acknowledge(a.db,a.consumer,a.run,a.sequence,a.status,a.target_sha))
    elif a.cmd=='release-gate':print(release_decision(a.db,a.audit))
if __name__=='__main__':main()
