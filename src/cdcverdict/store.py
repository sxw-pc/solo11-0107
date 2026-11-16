import sqlite3
from pathlib import Path
SCHEMA="""
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS source_file(id INTEGER PRIMARY KEY,path TEXT,sha TEXT UNIQUE,raw BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS snapshot_row(table_name TEXT NOT NULL,key TEXT NOT NULL,value TEXT NOT NULL,watermark INTEGER NOT NULL,schema_version TEXT NOT NULL,source_id INTEGER REFERENCES source_file(id),PRIMARY KEY(table_name,key));
CREATE TABLE IF NOT EXISTS txn(id TEXT PRIMARY KEY,begin_seq INTEGER,commit_seq INTEGER,committed INTEGER NOT NULL DEFAULT 0,valid INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS change_event(id INTEGER PRIMARY KEY,source_id INTEGER REFERENCES source_file(id),line_no INTEGER,partition TEXT,seq INTEGER,txn_id TEXT,table_name TEXT NOT NULL DEFAULT '',kind TEXT,key TEXT,before_value TEXT,after_value TEXT,alias_to TEXT,sha TEXT,raw TEXT,UNIQUE(partition,seq,txn_id,table_name));
CREATE TABLE IF NOT EXISTS key_alias(table_name TEXT NOT NULL,old_key TEXT,new_key TEXT,effective_seq INTEGER,source_event INTEGER REFERENCES change_event(id),PRIMARY KEY(table_name,old_key,effective_seq));
CREATE TABLE IF NOT EXISTS replay_run(id INTEGER PRIMARY KEY,watermark INTEGER,status TEXT NOT NULL,started_at TEXT DEFAULT CURRENT_TIMESTAMP,ended_at TEXT);
CREATE TABLE IF NOT EXISTS visible_row(run_id INTEGER REFERENCES replay_run(id),table_name TEXT NOT NULL,key TEXT,value TEXT,status TEXT,PRIMARY KEY(run_id,table_name,key));
CREATE TABLE IF NOT EXISTS issue(id INTEGER PRIMARY KEY,code TEXT NOT NULL,table_name TEXT,key TEXT,detail TEXT,event_id INTEGER REFERENCES change_event(id));
CREATE TABLE IF NOT EXISTS evidence(id INTEGER PRIMARY KEY,run_id INTEGER REFERENCES replay_run(id),table_name TEXT NOT NULL,key TEXT,event_id INTEGER REFERENCES change_event(id),source_id INTEGER REFERENCES source_file(id),detail TEXT);
CREATE TABLE IF NOT EXISTS decision(run_id INTEGER REFERENCES replay_run(id),table_name TEXT NOT NULL,key TEXT,status TEXT,value TEXT,rule_version TEXT,PRIMARY KEY(run_id,table_name,key));
CREATE TABLE IF NOT EXISTS run_issue(run_id INTEGER NOT NULL REFERENCES replay_run(id),issue_id INTEGER NOT NULL REFERENCES issue(id),PRIMARY KEY(run_id,issue_id));
CREATE TABLE IF NOT EXISTS run_exclusion(run_id INTEGER NOT NULL REFERENCES replay_run(id),txn_id TEXT NOT NULL,PRIMARY KEY(run_id,txn_id));
CREATE TABLE IF NOT EXISTS expected(key TEXT PRIMARY KEY,value TEXT);
CREATE TABLE IF NOT EXISTS target_snapshot(id INTEGER PRIMARY KEY,watermark INTEGER NOT NULL,source_sha TEXT NOT NULL UNIQUE,raw BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS target_row(snapshot_id INTEGER REFERENCES target_snapshot(id),key TEXT,value TEXT NOT NULL,PRIMARY KEY(snapshot_id,key));
CREATE TABLE IF NOT EXISTS cutover_audit(id INTEGER PRIMARY KEY,run_id INTEGER REFERENCES replay_run(id),target_id INTEGER REFERENCES target_snapshot(id),status TEXT NOT NULL,reason TEXT NOT NULL,target_digest TEXT NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS cutover_difference(audit_id INTEGER REFERENCES cutover_audit(id),key TEXT,kind TEXT,replay_value TEXT,target_value TEXT,PRIMARY KEY(audit_id,key));
CREATE TABLE IF NOT EXISTS consumer_contract(id TEXT PRIMARY KEY,critical INTEGER NOT NULL,minimum_watermark INTEGER NOT NULL,config_sha TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS consumer_ack(id INTEGER PRIMARY KEY,consumer_id TEXT REFERENCES consumer_contract(id),run_id INTEGER REFERENCES replay_run(id),sequence INTEGER NOT NULL,status TEXT NOT NULL,source_digest TEXT NOT NULL,UNIQUE(consumer_id,sequence));
CREATE TABLE IF NOT EXISTS release_gate(id INTEGER PRIMARY KEY,audit_id INTEGER REFERENCES cutover_audit(id),run_id INTEGER REFERENCES replay_run(id),status TEXT NOT NULL,reason TEXT NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS release_member(gate_id INTEGER REFERENCES release_gate(id),consumer_id TEXT REFERENCES consumer_contract(id),status TEXT NOT NULL,ack_id INTEGER REFERENCES consumer_ack(id),PRIMARY KEY(gate_id,consumer_id));
CREATE TABLE IF NOT EXISTS repair_plan(id INTEGER PRIMARY KEY,audit_id INTEGER REFERENCES cutover_audit(id),manifest_sha TEXT NOT NULL,raw BLOB NOT NULL,status TEXT NOT NULL,output_target_id INTEGER REFERENCES target_snapshot(id),result TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(audit_id,manifest_sha));
CREATE TABLE IF NOT EXISTS repair_choice(plan_id INTEGER REFERENCES repair_plan(id),key TEXT NOT NULL,decision TEXT NOT NULL,reason TEXT NOT NULL,replay_value TEXT,target_value TEXT,PRIMARY KEY(plan_id,key));
CREATE TABLE IF NOT EXISTS contract_registry(id INTEGER PRIMARY KEY,schema_version TEXT NOT NULL,raw TEXT NOT NULL,sha TEXT NOT NULL UNIQUE,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS table_contract(contract_id INTEGER REFERENCES contract_registry(id),table_name TEXT NOT NULL,key_field TEXT NOT NULL,PRIMARY KEY(contract_id,table_name));
CREATE TABLE IF NOT EXISTS reference_contract(contract_id INTEGER REFERENCES contract_registry(id),child_table TEXT NOT NULL,child_field TEXT NOT NULL,parent_table TEXT NOT NULL,parent_field TEXT NOT NULL,mode TEXT NOT NULL,PRIMARY KEY(contract_id,child_table,child_field));
"""
_MIGRATION="""
PRAGMA foreign_keys=OFF;
ALTER TABLE snapshot_row RENAME TO _legacy_snapshot_row;
ALTER TABLE change_event RENAME TO _legacy_change_event;
ALTER TABLE key_alias RENAME TO _legacy_key_alias;
ALTER TABLE visible_row RENAME TO _legacy_visible_row;
ALTER TABLE issue RENAME TO _legacy_issue;
ALTER TABLE evidence RENAME TO _legacy_evidence;
ALTER TABLE decision RENAME TO _legacy_decision;
CREATE TABLE snapshot_row(table_name TEXT NOT NULL,key TEXT NOT NULL,value TEXT NOT NULL,watermark INTEGER NOT NULL,schema_version TEXT NOT NULL,source_id INTEGER REFERENCES source_file(id),PRIMARY KEY(table_name,key));
CREATE TABLE change_event(id INTEGER PRIMARY KEY,source_id INTEGER REFERENCES source_file(id),line_no INTEGER,partition TEXT,seq INTEGER,txn_id TEXT,table_name TEXT NOT NULL DEFAULT '',kind TEXT,key TEXT,before_value TEXT,after_value TEXT,alias_to TEXT,sha TEXT,raw TEXT,UNIQUE(partition,seq,txn_id,table_name));
CREATE TABLE key_alias(table_name TEXT NOT NULL,old_key TEXT,new_key TEXT,effective_seq INTEGER,source_event INTEGER REFERENCES change_event(id),PRIMARY KEY(table_name,old_key,effective_seq));
CREATE TABLE visible_row(run_id INTEGER REFERENCES replay_run(id),table_name TEXT NOT NULL,key TEXT,value TEXT,status TEXT,PRIMARY KEY(run_id,table_name,key));
CREATE TABLE issue(id INTEGER PRIMARY KEY,code TEXT NOT NULL,table_name TEXT,key TEXT,detail TEXT,event_id INTEGER REFERENCES change_event(id));
CREATE TABLE evidence(id INTEGER PRIMARY KEY,run_id INTEGER REFERENCES replay_run(id),table_name TEXT NOT NULL,key TEXT,event_id INTEGER REFERENCES change_event(id),source_id INTEGER REFERENCES source_file(id),detail TEXT);
CREATE TABLE decision(run_id INTEGER REFERENCES replay_run(id),table_name TEXT NOT NULL,key TEXT,status TEXT,value TEXT,rule_version TEXT,PRIMARY KEY(run_id,table_name,key));
INSERT INTO snapshot_row(table_name,key,value,watermark,schema_version,source_id)
 SELECT 'default',key,value,watermark,schema_version,source_id FROM _legacy_snapshot_row;
INSERT INTO change_event(source_id,line_no,partition,seq,txn_id,table_name,kind,key,before_value,after_value,alias_to,sha,raw)
 SELECT source_id,line_no,partition,seq,txn_id,CASE WHEN kind='change' THEN 'default' ELSE '' END,kind,key,before_value,after_value,alias_to,sha,raw FROM _legacy_change_event;
INSERT INTO key_alias(table_name,old_key,new_key,effective_seq,source_event)
 SELECT 'default',old_key,new_key,effective_seq,source_event FROM _legacy_key_alias;
INSERT INTO visible_row(run_id,table_name,key,value,status)
 SELECT run_id,'default',key,value,status FROM _legacy_visible_row;
INSERT INTO issue(id,code,table_name,key,detail,event_id)
 SELECT id,code,NULL,key,detail,event_id FROM _legacy_issue;
INSERT INTO evidence(id,run_id,table_name,key,event_id,source_id,detail)
 SELECT id,run_id,'default',key,event_id,source_id,detail FROM _legacy_evidence;
INSERT INTO decision(run_id,table_name,key,status,value,rule_version)
 SELECT run_id,'default',key,status,value,rule_version FROM _legacy_decision;
DROP TABLE _legacy_snapshot_row;
DROP TABLE _legacy_change_event;
DROP TABLE _legacy_key_alias;
DROP TABLE _legacy_visible_row;
DROP TABLE _legacy_issue;
DROP TABLE _legacy_evidence;
DROP TABLE _legacy_decision;
PRAGMA foreign_keys=ON;
"""
def _needs_migration(db):
    exists=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='snapshot_row'").fetchone()
    if not exists:return False
    cols=[r['name'] for r in db.execute('PRAGMA table_info(snapshot_row)')]
    return 'table_name' not in cols
def connect(path):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(p)
    db.row_factory=sqlite3.Row
    if _needs_migration(db):db.executescript(_MIGRATION)
    db.executescript(SCHEMA)
    return db
