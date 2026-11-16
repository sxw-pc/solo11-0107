"""按水位生效的 schema 版本图与字段级兼容投影。

每个字段映射声明来源版本、目标版本以及 rename/default/确定类型转换；
回放用该图校验跨 schema 事件并投影旧 schema 行。未声明映射的字段只阻断
受影响 key，不会把旧值或缺字段误判为 null/删除。
"""
import json
from collections import deque
from .store import connect


class ProjectionError(ValueError):
    def __init__(self, field, reason):
        super().__init__(f'{field}: {reason}')
        self.field = field
        self.reason = reason


def _canonical_int(v):
    try:
        return str(int(v)) == v
    except (ValueError, TypeError):
        return False


# 确定类型转换：apply 是确定性函数，lossless 谓词按值判定该次转换是否无损。
CONVERSIONS = {
    'int_to_str': (lambda v: str(v), lambda v: isinstance(v, int) and not isinstance(v, bool)),
    'str_to_int': (lambda v: int(v), lambda v: isinstance(v, str) and _canonical_int(v)),
    'float_to_str': (lambda v: str(v), lambda v: isinstance(v, (int, float)) and not isinstance(v, bool)),
}
KINDS = ('identity', 'rename', 'default', 'convert')


def validate_snapshot(records, watermark, schema):
    if not isinstance(watermark, int) or watermark < 0:
        raise ValueError('invalid_watermark')
    if not schema or not isinstance(schema, str):
        raise ValueError('invalid_schema')
    if not records:
        raise ValueError('empty_snapshot')
    keys = [str(r['key']) for r in records]
    if len(keys) != len(set(keys)):
        raise ValueError('snapshot_duplicate_key')
    return keys


def register_schema(db_path, version, effective_watermark, fields):
    """声明一个 schema 版本及其字段集，自 effective_watermark 起生效。

    fields 为字段名或 {'name','required'} 列表；重复注册必须完全一致。
    """
    if not version or not isinstance(version, str):
        raise ValueError('invalid_schema')
    if not isinstance(effective_watermark, int) or effective_watermark < 0:
        raise ValueError('invalid_watermark')
    norm = []
    for f in fields:
        if isinstance(f, str):
            norm.append((f, 1))
        else:
            norm.append((str(f['name']), 1 if f.get('required', True) else 0))
    if not norm or len({n for n, _ in norm}) != len(norm):
        raise ValueError('schema_duplicate_field')
    db = connect(db_path)
    try:
        with db:
            old = db.execute('SELECT effective_watermark FROM schema_version WHERE version=?', (version,)).fetchone()
            if old:
                if old['effective_watermark'] != effective_watermark:
                    raise ValueError('schema_contract_changed')
                existing = {(r['field'], r['required']) for r in
                            db.execute('SELECT field,required FROM schema_field WHERE version=?', (version,))}
                if existing != set(norm):
                    raise ValueError('schema_contract_changed')
                return version
            db.execute('INSERT INTO schema_version VALUES (?,?)', (version, effective_watermark))
            db.executemany('INSERT INTO schema_field VALUES (?,?,?)', [(version, n, r) for n, r in norm])
            return version
    finally:
        db.close()


def register_mapping(db_path, source_version, target_version, mappings):
    """声明 source_version→target_version 的字段映射（identity/rename/default/convert）。

    default 用 value 给常量；convert 用 value 指定 CONVERSIONS 中的确定转换。
    重复注册同一边必须完全一致，否则拒绝。
    """
    if not mappings:
        raise ValueError('empty_mapping')
    db = connect(db_path)
    try:
        with db:
            versions = {r['version'] for r in db.execute('SELECT version FROM schema_version')}
            if source_version not in versions or target_version not in versions:
                raise ValueError('unknown_schema_version')
            src_fields = {r['field'] for r in db.execute('SELECT field FROM schema_field WHERE version=?', (source_version,))}
            dst_fields = {r['field'] for r in db.execute('SELECT field FROM schema_field WHERE version=?', (target_version,))}
            norm = []
            for m in mappings:
                kind = m.get('kind')
                target = m.get('target')
                source = m.get('source')
                value = m.get('value')
                if kind not in KINDS:
                    raise ValueError('unknown_mapping_kind')
                if not target or target not in dst_fields:
                    raise ValueError('unknown_target_field')
                if kind == 'default':
                    if source is not None:
                        raise ValueError('default_has_source')
                    stored = json.dumps(value, ensure_ascii=False, sort_keys=True)
                else:
                    if kind == 'identity':
                        if source is None:
                            source = target
                        if source != target:
                            raise ValueError('identity_source_mismatch')
                    if source not in src_fields:
                        raise ValueError('unknown_source_field')
                    if kind == 'rename' and source == target:
                        raise ValueError('rename_to_self')
                    if kind == 'convert':
                        if value not in CONVERSIONS:
                            raise ValueError('unknown_conversion')
                        stored = value
                    else:
                        stored = None
                norm.append((source_version, target_version, source, target, kind, stored))
            if len({m[3] for m in norm}) != len(norm):
                raise ValueError('duplicate_target_field')
            old = {(r['source_field'], r['target_field'], r['kind'], r['value']) for r in db.execute(
                'SELECT source_field,target_field,kind,value FROM field_mapping WHERE source_version=? AND target_version=?',
                (source_version, target_version))}
            new = {(s, t, k, v) for _, _, s, t, k, v in norm}
            if old:
                if old != new:
                    raise ValueError('mapping_contract_changed')
                return
            db.executemany(
                'INSERT INTO field_mapping(source_version,target_version,source_field,target_field,kind,value) VALUES (?,?,?,?,?,?)',
                norm)
    finally:
        db.close()


def load_graph(db, watermark):
    """加载在该水位已生效的版本图：{源版本: {目标版本: [映射]}}。"""
    versions = {r['version'] for r in
                db.execute('SELECT version FROM schema_version WHERE effective_watermark<=?', (watermark,))}
    graph = {}
    for r in db.execute('SELECT source_version,target_version,source_field,target_field,kind,value FROM field_mapping'):
        if r['source_version'] in versions and r['target_version'] in versions:
            graph.setdefault(r['source_version'], {}).setdefault(r['target_version'], []).append(dict(r))
    return graph


def load_fields(db):
    out = {}
    for r in db.execute('SELECT version,field,required FROM schema_field'):
        out.setdefault(r['version'], {})[r['field']] = r['required']
    return out


def find_path(graph, source, target):
    """在版本图中找 source→target 的有向路径；同版本返回空路径（原样放行）。"""
    if source == target:
        return []
    if source is None or target is None:
        return None
    seen = {source}
    queue = deque([(source, [])])
    while queue:
        node, path = queue.popleft()
        for nxt, mappings in graph.get(node, {}).items():
            if nxt in seen:
                continue
            candidate = path + [(node, nxt, mappings)]
            if nxt == target:
                return candidate
            seen.add(nxt)
            queue.append((nxt, candidate))
    return None


def _label(kind, value):
    if kind == 'convert':
        return f'convert:{value}'
    if kind == 'default':
        return f'default={json.dumps(value, ensure_ascii=False)}'
    return kind


def _compose(path):
    """把多跳路径合成 源版本→目标版本 的有效映射，携带映射链（origin+suffix）。"""
    eff = None
    for src, dst, mappings in path:
        edge = []
        for m in mappings:
            kind = m['kind']
            value = json.loads(m['value']) if kind == 'default' and m['value'] is not None else m['value']
            edge.append({'source': m['source_field'], 'target': m['target_field'],
                         'steps': [{'kind': kind, 'value': value}],
                         'strict': kind != 'identity',
                         'origin': f"{src}.{m['source_field']}" if m['source_field'] is not None else None,
                         'suffix': f" -[{_label(kind, value)}]-> {dst}.{m['target_field']}"})
        if eff is None:
            eff = edge
            continue
        prev = {m['target']: m for m in eff}
        merged = []
        for m in edge:
            if m['source'] is not None and m['source'] in prev:
                p = prev[m['source']]
                merged.append({'source': p['source'], 'target': m['target'], 'steps': p['steps'] + m['steps'],
                               'strict': p['strict'] or m['strict'], 'origin': p['origin'],
                               'suffix': p['suffix'] + m['suffix']})
            elif m['source'] is None:
                merged.append(m)
        eff = merged
    return eff or []


def _apply_steps(steps, value, field):
    for s in steps:
        if s['kind'] == 'default':
            value = s['value']
        elif s['kind'] == 'convert':
            fn, lossless = CONVERSIONS[s['value']]
            if not lossless(value):
                raise ProjectionError(field, 'lossy_conversion')
            value = fn(value)
    return value


def project_row(row, path, base_prov=None):
    """把 row 沿版本路径全量投影：默认值物化，来源行中未被消费的字段或
    有损转换抛 ProjectionError。返回 (投影行, {目标字段: (来源字段, 映射链)})。"""
    if not isinstance(row, dict):
        raise ProjectionError('*', 'schema_row_not_object')
    if not path:
        return dict(row), dict(base_prov) if base_prov else {f: (f, None) for f in row}
    src = path[0][0]
    prov = base_prov if base_prov is not None else {f: (f, f'{src}.{f}') for f in row}
    out = {}
    new_prov = {}
    consumed = set()
    for m in _compose(path):
        target = m['target']
        if m['source'] is None:
            out[target] = _apply_steps(m['steps'], None, target)
            new_prov[target] = (None, '(default)' + m['suffix'])
        elif m['source'] in row:
            consumed.add(m['source'])
            out[target] = _apply_steps(m['steps'], row[m['source']], target)
            origin, chain = prov.get(m['source'], (m['source'], f"{src}.{m['source']}"))
            new_prov[target] = (origin, (chain or f"{src}.{m['source']}") + m['suffix'])
    leftover = sorted(set(row) - consumed)
    if leftover:
        raise ProjectionError(leftover[0], 'schema_unmapped_source')
    return out, new_prov


def validate_migration(before_row, after_row, path, fields_by_version):
    """校验跨 schema 事件的 after：每个 after 字段必须被声明的映射解释，
    每个 before 字段必须被消费；rename/default/convert 的值严格确定，
    仅 identity 映射的字段允许在同一事件里改值。返回 after 的字段来源链。"""
    if not isinstance(before_row, dict) or not isinstance(after_row, dict):
        raise ProjectionError('*', 'schema_row_not_object')
    dst = path[-1][1]
    target_fields = fields_by_version.get(dst, {})
    eff = _compose(path)
    by_target = {m['target']: m for m in eff}
    for f in sorted(after_row):
        if f not in target_fields:
            raise ProjectionError(f, 'schema_unknown_field')
        if f not in by_target:
            raise ProjectionError(f, 'schema_unmapped_field')
    consumable = {m['source'] for m in eff if m['source'] is not None}
    for g in sorted(before_row):
        if g not in consumable:
            raise ProjectionError(g, 'schema_unmapped_source')
    src = path[0][0]
    prov = {}
    for m in eff:
        f = m['target']
        if m['source'] is None:
            expected = _apply_steps(m['steps'], None, f)
            if f in after_row:
                if after_row[f] != expected:
                    raise ProjectionError(f, 'schema_default_violation')
                prov[f] = (None, '(default)' + m['suffix'])
        elif m['source'] in before_row:
            expected = _apply_steps(m['steps'], before_row[m['source']], f)
            if f not in after_row:
                raise ProjectionError(f, 'schema_field_dropped')
            if m['strict'] and after_row[f] != expected:
                raise ProjectionError(f, 'schema_mapping_violation')
            prov[f] = (m['source'], f"{src}.{m['source']}" + m['suffix'])
        elif f in after_row:
            if m['strict']:
                raise ProjectionError(f, 'schema_mapping_violation')
            prov[f] = (m['source'], f"{src}.{m['source']}" + m['suffix'])
    return prov
