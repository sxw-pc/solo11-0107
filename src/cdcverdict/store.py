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
CREATE TABLE IF NOT EXISTS repair_plan(id INTEGER PRIMARY KEY,audit_id INTEGER REFERENCES cutover_audit(id),manifest_sha TEXT NOT NULL,raw BLOB NOT NULL,status TEXT NOT NULL,output_target_id INTEGER REFERENCES target_snapshot(id),result TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(audit_id,manifest_sha));
CREATE TABLE IF NOT EXISTS repair_choice(plan_id INTEGER REFERENCES repair_plan(id),key TEXT NOT NULL,decision TEXT NOT NULL,reason TEXT NOT NULL,replay_value TEXT,target_value TEXT,PRIMARY KEY(plan_id,key));
CREATE TABLE IF NOT EXISTS consent_source(id INTEGER PRIMARY KEY,sha TEXT UNIQUE,raw BLOB NOT NULL,status TEXT NOT NULL,error TEXT);
CREATE TABLE IF NOT EXISTS data_subject(id TEXT PRIMARY KEY,owner TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS subject_key(subject_id TEXT REFERENCES data_subject(id),key TEXT UNIQUE,PRIMARY KEY(subject_id,key));
CREATE TABLE IF NOT EXISTS consent_event(id TEXT PRIMARY KEY,subject_id TEXT REFERENCES data_subject(id),purpose TEXT NOT NULL,version INTEGER NOT NULL,action TEXT NOT NULL,effective_watermark INTEGER NOT NULL,expires_watermark INTEGER,proof_sha TEXT NOT NULL,source_id INTEGER REFERENCES consent_source(id),UNIQUE(subject_id,purpose,version));
CREATE TABLE IF NOT EXISTS controlled_export(id INTEGER PRIMARY KEY,run_id INTEGER REFERENCES replay_run(id),purpose TEXT NOT NULL,recipient TEXT NOT NULL,status TEXT NOT NULL,content_sha TEXT NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS export_row(export_id INTEGER REFERENCES controlled_export(id),key TEXT NOT NULL,subject_id TEXT NOT NULL,value TEXT NOT NULL,consent_event_id TEXT REFERENCES consent_event(id),PRIMARY KEY(export_id,key));
CREATE TABLE IF NOT EXISTS export_exclusion(export_id INTEGER REFERENCES controlled_export(id),key TEXT NOT NULL,reason TEXT NOT NULL,PRIMARY KEY(export_id,key));
CREATE TABLE IF NOT EXISTS export_recall(id INTEGER PRIMARY KEY,export_id INTEGER REFERENCES controlled_export(id),key TEXT NOT NULL,revocation_id TEXT REFERENCES consent_event(id),status TEXT NOT NULL,reason TEXT NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(export_id,key,revocation_id));
CREATE TABLE IF NOT EXISTS recall_receipt(id INTEGER PRIMARY KEY,recall_id INTEGER REFERENCES export_recall(id),recipient TEXT NOT NULL,proof_sha TEXT NOT NULL,received_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(recall_id,recipient));
CREATE TABLE IF NOT EXISTS feed_slot(id TEXT PRIMARY KEY, partition TEXT NOT NULL, deadline TEXT NOT NULL, quorum INTEGER NOT NULL, revision INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'missing', selected_digest TEXT);
CREATE TABLE IF NOT EXISTS feed_observation(event_id TEXT PRIMARY KEY, slot_id TEXT REFERENCES feed_slot(id), producer TEXT NOT NULL, digest TEXT NOT NULL, row_count INTEGER NOT NULL, observed_at TEXT NOT NULL, source_sha TEXT NOT NULL, raw BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS feed_lineage_event(event_id TEXT PRIMARY KEY,producer TEXT NOT NULL,version INTEGER NOT NULL,source_domain TEXT NOT NULL,ancestry TEXT NOT NULL,root_origin TEXT NOT NULL,attested_by TEXT NOT NULL,proof_sha TEXT NOT NULL,source_sha TEXT NOT NULL,raw BLOB NOT NULL,UNIQUE(producer,version));
CREATE TABLE IF NOT EXISTS feed_lineage_head(producer TEXT PRIMARY KEY,version INTEGER NOT NULL,event_id TEXT REFERENCES feed_lineage_event(event_id),root_origin TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS feed_verdict(id INTEGER PRIMARY KEY, slot_id TEXT REFERENCES feed_slot(id), revision INTEGER NOT NULL, status TEXT NOT NULL, selected_digest TEXT, evidence TEXT NOT NULL, UNIQUE(slot_id,revision));
CREATE TABLE IF NOT EXISTS recovery_worker(id TEXT PRIMARY KEY, partition TEXT NOT NULL, capacity INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS recovery_job(id TEXT PRIMARY KEY, slot_id TEXT REFERENCES feed_slot(id), worker_id TEXT REFERENCES recovery_worker(id), priority INTEGER NOT NULL, units INTEGER NOT NULL, revision INTEGER NOT NULL, status TEXT NOT NULL, reason TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS recovery_event(id INTEGER PRIMARY KEY, job_id TEXT REFERENCES recovery_job(id), status TEXT NOT NULL, reason TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS dispatch_worker(id TEXT PRIMARY KEY,partition TEXT NOT NULL,capacity INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS dispatch_task(id TEXT PRIMARY KEY,job_id TEXT NOT NULL UNIQUE REFERENCES recovery_job(id),tenant TEXT NOT NULL,partition TEXT NOT NULL,lease_ticks INTEGER NOT NULL,status TEXT NOT NULL DEFAULT 'pending',generation INTEGER NOT NULL DEFAULT 0,worker_id TEXT REFERENCES dispatch_worker(id),expires_at INTEGER,artifact_sha TEXT);
CREATE TABLE IF NOT EXISTS dispatch_clock(id INTEGER PRIMARY KEY CHECK(id=1),tick INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS dispatch_cursor(partition TEXT PRIMARY KEY,last_tenant TEXT NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS dispatch_event(id INTEGER PRIMARY KEY,task_id TEXT REFERENCES dispatch_task(id),tick INTEGER NOT NULL,action TEXT NOT NULL,worker_id TEXT,generation INTEGER NOT NULL,detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS dispatch_artifact(task_id TEXT PRIMARY KEY REFERENCES dispatch_task(id),source_sha TEXT NOT NULL,raw BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS quality_contract(id TEXT PRIMARY KEY,source_sha TEXT NOT NULL,raw BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS quality_batch(id TEXT PRIMARY KEY,contract_id TEXT NOT NULL REFERENCES quality_contract(id),source_sha TEXT NOT NULL,raw BLOB NOT NULL,revision INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS quality_correction(batch_id TEXT NOT NULL REFERENCES quality_batch(id),fact_key TEXT NOT NULL,value_json TEXT NOT NULL,proposal_id TEXT NOT NULL,revision INTEGER NOT NULL,PRIMARY KEY(batch_id,fact_key));
CREATE TABLE IF NOT EXISTS quality_assessment(id INTEGER PRIMARY KEY,batch_id TEXT NOT NULL REFERENCES quality_batch(id),revision INTEGER NOT NULL,fact_sha TEXT NOT NULL,facts_json TEXT NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(batch_id,revision));
CREATE TABLE IF NOT EXISTS quality_rule_result(assessment_id INTEGER NOT NULL REFERENCES quality_assessment(id),rule_id TEXT NOT NULL,status TEXT NOT NULL,detail TEXT NOT NULL,PRIMARY KEY(assessment_id,rule_id));
CREATE TABLE IF NOT EXISTS quality_product_result(assessment_id INTEGER NOT NULL REFERENCES quality_assessment(id),product_id TEXT NOT NULL,status TEXT NOT NULL,blocked_by TEXT NOT NULL,PRIMARY KEY(assessment_id,product_id));
CREATE TABLE IF NOT EXISTS quality_proposal(id TEXT PRIMARY KEY,batch_id TEXT NOT NULL REFERENCES quality_batch(id),expected_revision INTEGER NOT NULL,fact_key TEXT NOT NULL,value_json TEXT NOT NULL,proposer TEXT NOT NULL,evidence_sha TEXT NOT NULL,source_sha TEXT NOT NULL,raw BLOB NOT NULL,state TEXT NOT NULL DEFAULT 'open',withdraw_reason TEXT);
CREATE TABLE IF NOT EXISTS quality_review(proposal_id TEXT PRIMARY KEY REFERENCES quality_proposal(id),reviewer TEXT NOT NULL,verdict TEXT NOT NULL,proof_sha TEXT NOT NULL,source_sha TEXT NOT NULL,raw BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS quality_arbitration(id INTEGER PRIMARY KEY,batch_id TEXT NOT NULL REFERENCES quality_batch(id),status TEXT NOT NULL,details TEXT NOT NULL,assessment_id INTEGER REFERENCES quality_assessment(id),created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS quality_publication(id INTEGER PRIMARY KEY,batch_id TEXT NOT NULL REFERENCES quality_batch(id),product_id TEXT NOT NULL,assessment_id INTEGER NOT NULL REFERENCES quality_assessment(id),manifest_sha TEXT NOT NULL,raw BLOB NOT NULL,out_path TEXT NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
"""
def connect(path):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(p)
    db.row_factory=sqlite3.Row
    db.executescript(SCHEMA)
    return db
