"""Contract compilation for offline multi-table CDC replay.

A contract declares, per table, the primary-key field used to build the
(table, key) entity identity, plus non-null child.field -> parent.table.field
references and whether each reference is enforced immediately after every
event or only deferred to the post-commit staged view.
"""
DEFAULT_TABLE='default'
IMMEDIATE='immediate'
DEFERRED='deferred'
_MODES=(IMMEDIATE,DEFERRED)


class ContractError(ValueError):
    """Raised when a contract declaration cannot be compiled."""


class ReferenceViolation(Exception):
    """A non-null reference has no matching parent in the staged view."""

    def __init__(self,code,detail,event=None,entities=()):
        super().__init__(detail)
        self.code=code
        self.detail=detail
        self.event=event
        self.entities=frozenset(entities)


class Reference:
    __slots__=('child_table','child_field','parent_table','parent_field','mode')

    def __init__(self,child_table,child_field,parent_table,parent_field,mode):
        self.child_table=child_table
        self.child_field=child_field
        self.parent_table=parent_table
        self.parent_field=parent_field
        self.mode=mode

    def as_tuple(self):
        return (self.child_table,self.child_field,self.parent_table,self.parent_field,self.mode)


class TableContract:
    __slots__=('name','key_field')

    def __init__(self,name,key_field):
        self.name=name
        self.key_field=key_field


class CompiledContract:
    """Immutable, compiled view of a multi-table CDC contract."""

    def __init__(self,schema,tables,references,raw,sha):
        self.schema=schema
        self.tables=tables
        self.references=references
        self.raw=raw
        self.sha=sha
        self.table_names=set(tables)
        self.key_fields={name:tc.key_field for name,tc in tables.items()}
        self.immediate=[r for r in references if r.mode==IMMEDIATE]
        self.deferred=[r for r in references if r.mode==DEFERRED]

    def check_references(self,live,refs,event=None):
        """Validate the given references against a staged view.

        live maps (table,key) -> JSON-encoded row value. Only rows that
        currently exist in the staged view can satisfy a non-null reference.
        """
        for ref in refs:
            for (table,key),encoded in live.items():
                if table!=ref.child_table:
                    continue
                try:
                    row=json.loads(encoded)
                except (TypeError,ValueError):
                    raise ReferenceViolation('invalid_row_value',
                        f'{table}:{key}: row value cannot be decoded',event)
                if not isinstance(row,dict) or ref.child_field not in row:
                    raise ReferenceViolation('reference_field_missing',
                        f'{table}:{key}.{ref.child_field}: referenced field is missing',
                        event)
                target=row[ref.child_field]
                if target is None:
                    raise ReferenceViolation('reference_null',
                        f'{table}:{key}.{ref.child_field}: non-null reference is null',
                        event)
                if (ref.parent_table,str(target)) not in live:
                    label=(f'{ref.child_table}.{ref.child_field} -> '
                           f'{ref.parent_table}.{ref.parent_field} [{ref.mode}]')
                    child_identity=(table,key)
                    parent_identity=(ref.parent_table,str(target))
                    raise ReferenceViolation('fk_violation',
                        f'{label}: {ref.child_table}:{key}.{ref.child_field}={target!r} '
                        f'has no live parent {ref.parent_table}:{target}',event,
                        entities=(child_identity,parent_identity))


import hashlib
import json


def _require_name(value,prefix):
    if not isinstance(value,str) or not value.strip():
        raise ContractError(f'{prefix}_invalid_name')
    return value


def compile_contract(spec):
    """Compile a declarative contract spec into a CompiledContract.

    Spec shape::

        {"schema": "v2",
         "tables": {"orders": {"key_field": "id"}, ...},
         "references": [{"child": {"table": "items", "field": "order_id"},
                         "parent": {"table": "orders", "field": "id"},
                         "mode": "immediate|deferred"}, ...]}
    """
    if not isinstance(spec,dict):
        raise ContractError('contract_not_object')
    schema=spec.get('schema')
    if not schema or not isinstance(schema,str):
        raise ContractError('invalid_schema')
    table_specs=spec.get('tables')
    if not isinstance(table_specs,dict) or not table_specs:
        raise ContractError('contract_missing_tables')
    tables={}
    for name,decl in table_specs.items():
        _require_name(name,'table')
        if not isinstance(decl,dict):
            raise ContractError(f'table_{name}_invalid_declaration')
        key_field=decl.get('key_field')
        _require_name(key_field,f'table_{name}_key_field')
        if name in tables:
            raise ContractError(f'table_{name}_duplicate')
        tables[name]=TableContract(name,key_field)
    references=[]
    seen=set()
    for entry in spec.get('references',[]):
        if not isinstance(entry,dict):
            raise ContractError('reference_invalid_declaration')
        try:
            child=entry['child'];parent=entry['parent']
            child_table=_require_name(child.get('table'),'child_table')
            child_field=_require_name(child.get('field'),'child_field')
            parent_table=_require_name(parent.get('table'),'parent_table')
            parent_field=_require_name(parent.get('field'),'parent_field')
        except AttributeError:
            raise ContractError('reference_invalid_declaration')
        mode=entry.get('mode')
        if mode not in _MODES:
            raise ContractError('reference_invalid_mode')
        for table in (child_table,parent_table):
            if table not in tables:
                raise ContractError(f'reference_unknown_table:{table}')
        if parent_field!=tables[parent_table].key_field:
            raise ContractError('reference_parent_not_key')
        if child_field==tables[child_table].key_field:
            raise ContractError('reference_on_primary_key')
        identity=(child_table,child_field)
        if identity in seen:
            raise ContractError(f'reference_duplicate:{child_table}.{child_field}')
        seen.add(identity)
        references.append(Reference(child_table,child_field,parent_table,
                                    parent_field,mode))
    raw=json.dumps(spec,sort_keys=True,ensure_ascii=False)
    return CompiledContract(schema,tables,references,raw,hashlib.sha256(raw.encode()).hexdigest())


def load_contract(db):
    """Load the most recently registered compiled contract, if any."""
    row=db.execute('SELECT raw FROM contract_registry ORDER BY id DESC LIMIT 1').fetchone()
    return compile_contract(json.loads(row['raw'])) if row else None


def _row_identity(record,contract):
    table=record.get('table')
    if not table:
        raise ValueError('snapshot_missing_table')
    if table not in contract.table_names:
        raise ValueError(f'unknown_table:{table}')
    value=record.get('value')
    if not isinstance(value,dict):
        raise ValueError('snapshot_row_not_object')
    key_field=contract.key_fields[table]
    if key_field not in value or value[key_field] is None:
        raise ValueError(f'snapshot_missing_primary_key:{table}.{key_field}')
    key=str(value[key_field])
    if str(record.get('key'))!=key:
        raise ValueError(f'snapshot_key_field_mismatch:{table}')
    return table,key,value


def validate_snapshot(records,watermark,schema,contract=None):
    """Validate a snapshot, either legacy single-table or table-qualified.

    Without a contract the legacy global-namespace semantics are preserved:
    every record needs a unique bare key. With a compiled contract, identity
    is (table, key), keys only need to be unique per table, and every
    declared non-null reference must already hold in the snapshot view.
    """
    if not isinstance(watermark,int) or watermark<0:
        raise ValueError('invalid_watermark')
    if not schema or not isinstance(schema,str):
        raise ValueError('invalid_schema')
    if not records:
        raise ValueError('empty_snapshot')
    if contract is None:
        keys=[str(r['key']) for r in records]
        if len(keys)!=len(set(keys)):
            raise ValueError('snapshot_duplicate_key')
        return keys
    if schema!=contract.schema:
        raise ValueError('snapshot_contract_schema_mismatch')
    entities={}
    for record in records:
        table,key,value=_row_identity(record,contract)
        identity=(table,key)
        if identity in entities:
            raise ValueError(f'snapshot_duplicate_key:{table}:{key}')
        entities[identity]=json.dumps(value,sort_keys=True)
    contract.check_references(entities,contract.references)
    return [f'{table}:{key}' for table,key in sorted(entities)]
