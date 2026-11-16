def validate_snapshot(records,watermark,schema):
    if not isinstance(watermark,int) or watermark<0:raise ValueError('invalid_watermark')
    if not schema or not isinstance(schema,str):raise ValueError('invalid_schema')
    keys=[str(r['key']) for r in records]
    if len(keys)!=len(set(keys)):raise ValueError('snapshot_duplicate_key')
    return keys
