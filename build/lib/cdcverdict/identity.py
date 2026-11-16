def resolve(alias,key,seq):
    seen=set()
    while key in alias and alias[key][1]<=seq:
        if key in seen:raise ValueError('alias_cycle')
        seen.add(key);key=alias[key][0]
    return key
def add_alias(alias,old,new,seq,live):
    if old==new or resolve(alias,new,seq)==old:raise ValueError('alias_cycle')
    if new in live and new!=old:raise ValueError('alias_merge')
    if any(v[0]==new and k!=old for k,v in alias.items()):raise ValueError('alias_merge')
    alias[old]=(new,seq)
