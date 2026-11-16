from collections import defaultdict
def transactions(db,watermark):
    events=[dict(x) for x in db.execute('SELECT * FROM change_event ORDER BY seq,id')]
    conflicted_txns={r['detail'].split(':',2)[-1] for r in db.execute("SELECT detail FROM issue WHERE code='identity_conflict' AND detail IS NOT NULL")}
    groups=defaultdict(list)
    for e in events:groups[e['txn_id']].append(e)
    accepted=[];excluded=[];bad=set()
    for tx,items in groups.items():
        begins=[e for e in items if e['kind']=='begin'];commits=[e for e in items if e['kind']=='commit']
        changes=[e for e in items if e['kind']=='change']
        valid=(tx not in conflicted_txns and len(begins)==len(commits)==1
               and len({(e['partition'],e['seq']) for e in items})==len(items)
               and all(begins[0]['seq']<e['seq']<commits[0]['seq'] for e in changes))
        commit_seq=commits[0]['seq'] if commits else None
        db.execute('INSERT OR REPLACE INTO txn VALUES (?,?,?,?,?)',(tx,begins[0]['seq'] if begins else None,commit_seq,int(bool(commits)),int(valid)))
        if not valid:
            db.execute('INSERT INTO issue(code,detail) VALUES (?,?)',('txn_incomplete',tx));bad.update(e['key'] for e in changes if e['key']);continue
        if commit_seq>watermark:excluded.append(tx);continue
        accepted.append((commit_seq,tx,sorted(changes,key=lambda e:e['seq'])))
    seqs=sorted({e['seq'] for e in events if e['seq']<=watermark})
    if seqs and any(b-a>1 for a,b in zip(seqs,seqs[1:])):
        bad.update(e['key'] for _,_,group in accepted for e in group if e['key'])
    return sorted(accepted,key=lambda item:(item[0],item[1])),bad,excluded
