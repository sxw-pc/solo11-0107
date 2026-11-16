import sqlite3
from pathlib import Path
SCHEMA="""
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS source_file(id INTEGER PRIMARY KEY,path TEXT,sha TEXT UNIQUE,raw BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS snapshot_row(key TEXT PRIMARY KEY,value TEXT NOT NULL,watermark INTEGER NOT NULL,schema_version TEXT NOT NULL,source_id INTEGER REFERENCES source_file(id));
CREATE TABLE IF NOT EXISTS txn(id TEXT PRIMARY KEY,begin_seq INTEGER,commit_seq INTEGER,committed INTEGER NOT NULL DEFAULT 0,valid INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS change_event(id INTEGER PRIMARY KEY,source_id INTEGER REFERENCES source_file(id),line_no INTEGER,partition TEXT,seq INTEGER,txn_id TEXT,kind TEXT,key TEXT,before_value TEXT,after_value TEXT,alias_to TEXT,sha TEXT,raw TEXT,UNIQUE(partition,seq,txn_id));
CREATE TABLE IF NOT EXISTS key_alias(old_key TEXT,new_key TEXT,effective_seq INTEGER,source_event INTEGER REFERENCES change_event(id),PRIMARY KEY(old_key,effective_seq));
CREATE TABLE IF NOT EXISTS replay_run(id INTEGER PRIMARY KEY,watermark INTEGER,status TEXT NOT NULL,started_at TEXT DEFAULT CURRENT_TIMESTAMP,ended_at TEXT);
CREATE TABLE IF NOT EXISTS visible_row(run_id INTEGER REFERENCES replay_run(id),key TEXT,value TEXT,status TEXT,PRIMARY KEY(run_id,key));
CREATE TABLE IF NOT EXISTS issue(id INTEGER PRIMARY KEY,code TEXT NOT NULL,key TEXT,detail TEXT,event_id INTEGER REFERENCES change_event(id));
CREATE TABLE IF NOT EXISTS evidence(id INTEGER PRIMARY KEY,run_id INTEGER REFERENCES replay_run(id),key TEXT,event_id INTEGER REFERENCES change_event(id),source_id INTEGER REFERENCES source_file(id),detail TEXT);
CREATE TABLE IF NOT EXISTS decision(run_id INTEGER REFERENCES replay_run(id),key TEXT,status TEXT,value TEXT,rule_version TEXT,PRIMARY KEY(run_id,key));
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
"""
def connect(path):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(p)
    db.row_factory=sqlite3.Row
    db.executescript(SCHEMA)
    return db
