from collections import defaultdict
def transactions(db,watermark):
    events=[dict(x) for x in db.execute('SELECT * FROM change_event ORDER BY seq,id')]
    groups=defaultdict(list)
    for e in events:groups[e['txn_id']].append(e)
    accepted=[];excluded=[];bad=set()
    for tx,items in groups.items():
        begins=[e for e in items if e['kind']=='begin'];commits=[e for e in items if e['kind']=='commit']
        changes=[e for e in items if e['kind']=='change']
        valid=len(begins)==len(commits)==1 and len({(e['partition'],e['seq']) for e in items})==len(items) and begins[0]['seq']<commits[0]['seq']
        commit_seq=commits[0]['seq'] if commits else None
        db.execute('INSERT OR REPLACE INTO txn VALUES (?,?,?,?,?)',(tx,begins[0]['seq'] if begins else None,commit_seq,int(bool(commits)),int(valid)))
        if not valid:
            db.execute('INSERT INTO issue(code,detail) VALUES (?,?)',('txn_incomplete',tx));bad.update(e['key'] for e in changes if e['key']);continue
        if commit_seq>watermark:excluded.append(tx);continue
        accepted.extend(sorted(changes,key=lambda e:e['seq']))
    seqs=sorted({e['seq'] for e in events if e['seq']<=watermark})
    if seqs and any(b-a>1 for a,b in zip(seqs,seqs[1:])):bad.update(e['key'] for e in accepted if e['key'])
    return sorted(accepted,key=lambda e:(e['seq'],e['id'])),bad,excluded
