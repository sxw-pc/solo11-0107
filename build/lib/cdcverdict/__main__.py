import argparse,json,tempfile
from pathlib import Path
from .ingest import snapshot,ingest
from .rules import replay
from .report import write
def demo(workdir=None):
    root=Path(workdir or tempfile.mkdtemp(prefix='cdcverdict-'));root.mkdir(parents=True,exist_ok=True)
    base=root/'base.jsonl';base.write_text(json.dumps({'key':'A','value':{'n':1}}),encoding='utf-8')
    events=root/'events.jsonl';events.write_text('\n'.join(json.dumps(x) for x in [
      {'kind':'begin','txn':'T1','seq':1},
      {'kind':'change','txn':'T1','seq':2,'key':'A','action':'update','before':{'n':1},'after':{'n':2}},
      {'kind':'commit','txn':'T1','seq':3}]),encoding='utf-8')
    db=root/'data.db';snapshot(base,db,0);ingest(events,db);run,_,_=replay(db,3)
    return write(db,run,root/'report')
def main():
    p=argparse.ArgumentParser(description='离线 CDC 事务回放与可见性裁定');s=p.add_subparsers(dest='cmd',required=True)
    d=s.add_parser('demo');d.add_argument('--workdir')
    i=s.add_parser('init');i.add_argument('snapshot');i.add_argument('--db',required=True);i.add_argument('--watermark',type=int,required=True)
    g=s.add_parser('ingest');g.add_argument('events');g.add_argument('--db',required=True)
    v=s.add_parser('verdict');v.add_argument('--db',required=True);v.add_argument('--watermark',type=int,required=True)
    r=s.add_parser('report');r.add_argument('--db',required=True);r.add_argument('--run',type=int,required=True);r.add_argument('--out',required=True)
    a=p.parse_args()
    if a.cmd=='demo':print(demo(a.workdir))
    elif a.cmd=='init':print(snapshot(a.snapshot,a.db,a.watermark))
    elif a.cmd=='ingest':print(ingest(a.events,a.db))
    elif a.cmd=='verdict':print(replay(a.db,a.watermark))
    elif a.cmd=='report':print(write(a.db,a.run,a.out))
if __name__=='__main__':main()
