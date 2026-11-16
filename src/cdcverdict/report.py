import json
from pathlib import Path
from .store import connect
def write(path,run,out):
    db=connect(path)
    data={'run':dict(db.execute('SELECT * FROM replay_run WHERE id=?',(run,)).fetchone()),
      'decisions':[dict(x) for x in db.execute('SELECT * FROM decision WHERE run_id=? ORDER BY key',(run,))],
      'evidence':[dict(x) for x in db.execute('SELECT * FROM evidence WHERE run_id=? ORDER BY id',(run,))],
      'issues':[dict(x) for x in db.execute('SELECT i.* FROM issue i JOIN run_issue ri ON ri.issue_id=i.id WHERE ri.run_id=? ORDER BY i.id',(run,))],
      'sources':[dict(x) for x in db.execute('SELECT id,path,sha FROM source_file ORDER BY id')],
      'excluded_transactions':[r['txn_id'] for r in db.execute('SELECT txn_id FROM run_exclusion WHERE run_id=? ORDER BY txn_id',(run,))]}
    expected={r['key']:r['value'] for r in db.execute('SELECT * FROM expected')}
    actual={r['key']:r['value'] for r in data['decisions'] if r['status']=='存在'}
    data['expected_differences']=[k for k in sorted(set(expected)|set(actual)) if expected.get(k)!=actual.get(k)] if expected else []
    out=Path(out);out.parent.mkdir(parents=True,exist_ok=True)
    try:
        out.with_suffix('.json').write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
        out.with_suffix('.md').write_text('# 回放结论\n\n'+'\n'.join(f"- {x['key']}: {x['status']}" for x in data['decisions']),encoding='utf-8')
    except OSError:
        with db:db.execute("UPDATE replay_run SET status='待核' WHERE id=?",(run,))
        raise
    with db:db.execute("UPDATE replay_run SET status='已归档' WHERE id=? AND status='已判定'",(run,))
    db.close();return out.with_suffix('.json')


def write_repair_merge(path, merge_id, out):
    """Report one merge, key by key: source plan, decision and unresolved/conflict state.

    Read-only: the frozen audit, original target and earlier gates stay untouched.
    """
    from .repair import repair_merge_report
    data = repair_merge_report(path, merge_id)
    db = connect(path)
    try:
        differences = {row['key']: dict(row) for row in db.execute(
            'SELECT key,kind,replay_value,target_value FROM cutover_difference WHERE audit_id=?',
            (data['merge']['audit_id'],))}
    finally:
        db.close()
    keys = []
    for row in data['keys']:
        difference = differences.get(row['key'], {})
        # A deferral deliberately leaves the key unresolved for a later repair.
        unresolved = row['outcome'] in {'unresolved', 'conflict', 'deferred'}
        keys.append({
            'key': row['key'],
            'source_plans': [source['plan_id'] for source in row['sources']],
            'decisions': [{'plan_id': source['plan_id'], 'decision': source['decision'],
                           'value': source['value'], 'reason': source['reason']} for source in row['sources']],
            'decision': row['decision'],
            'outcome': row['outcome'],
            'unresolved': unresolved,
            'conflict': row['outcome'] == 'conflict',
            'value': row['value'],
            'replay_value': row['replay_value'],
            'target_value': row['target_value'],
            'difference_kind': difference.get('kind')})
    merged = data['merge']
    data = {'merge': merged,
            'agreement_rule': data['agreement_rule'],
            'keys': keys,
            'conflicts': [row['key'] for row in keys if row['conflict']],
            'unresolved': [row['key'] for row in keys if row['unresolved'] and not row['conflict']],
            'resolved': merged['status'] == 'merged' and not any(row['unresolved'] for row in keys)}
    out = Path(out); out.parent.mkdir(parents=True, exist_ok=True)
    out.with_suffix('.json').write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    lines = ['# 修复汇合', '',
             f"- 状态: {merged['status']}",
             f"- 合并结果快照: {merged['output_target_id']}",
             f"- 审计: {merged['audit_id']}",
             f"- 基线目标: {merged['base_target_id']}"]
    for row in keys:
        sources = ','.join(str(plan) for plan in row['source_plans']) or '-'
        state = row['outcome']
        lines.append(f"- {row['key']}: {row['decision'] or '-'} ({state}; plans={sources})")
    out.with_suffix('.md').write_text('\n'.join(lines), encoding='utf-8')
    return out.with_suffix('.json')

