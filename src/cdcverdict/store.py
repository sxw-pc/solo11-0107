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
"""
def connect(path):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(p)
    db.row_factory=sqlite3.Row
    db.executescript(SCHEMA)
    return db
