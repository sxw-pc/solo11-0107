"""Independent warehouse usage contract and immutable period close ledger."""

import sqlite3
from pathlib import Path

SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS period_contract(
  id TEXT PRIMARY KEY, tenant TEXT NOT NULL, period TEXT NOT NULL,
  starts TEXT NOT NULL, ends TEXT NOT NULL, shared_units INTEGER NOT NULL,
  reserved_json TEXT NOT NULL, prices_json TEXT NOT NULL, priority_json TEXT NOT NULL,
  signers_json TEXT NOT NULL, closers_json TEXT NOT NULL, approvers_json TEXT NOT NULL,
  source_sha TEXT NOT NULL, raw BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS period_choice(
  tenant TEXT NOT NULL, period TEXT NOT NULL, contract_id TEXT NOT NULL REFERENCES period_contract(id),
  signer TEXT NOT NULL, proof_sha TEXT NOT NULL, source_sha TEXT NOT NULL, raw BLOB NOT NULL,
  PRIMARY KEY(tenant,period));
CREATE TABLE IF NOT EXISTS period_amendment(
  id TEXT PRIMARY KEY, tenant TEXT NOT NULL, period TEXT NOT NULL,
  contract_id TEXT NOT NULL REFERENCES period_contract(id), contract_sha TEXT NOT NULL,
  effective_at TEXT NOT NULL, clause TEXT NOT NULL, prices_json TEXT NOT NULL,
  signer TEXT NOT NULL, proof_sha TEXT NOT NULL, source_sha TEXT NOT NULL, raw BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS period_source_file(
  id INTEGER PRIMARY KEY, source_sha TEXT UNIQUE NOT NULL, raw BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS period_event_identity(
  id TEXT PRIMARY KEY, kind TEXT NOT NULL, source_sha TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS period_usage(
  id TEXT PRIMARY KEY, tenant TEXT NOT NULL, period TEXT NOT NULL, product TEXT NOT NULL,
  occurred_at TEXT NOT NULL, units INTEGER NOT NULL, source_sha TEXT NOT NULL, raw BLOB NOT NULL,
  source_file INTEGER NOT NULL REFERENCES period_source_file(id));
CREATE TABLE IF NOT EXISTS period_correction(
  id TEXT PRIMARY KEY, usage_id TEXT NOT NULL REFERENCES period_usage(id), revision INTEGER NOT NULL,
  corrected_units INTEGER NOT NULL, observed_at TEXT NOT NULL, reason TEXT NOT NULL,
  source_sha TEXT NOT NULL, raw BLOB NOT NULL, source_file INTEGER NOT NULL REFERENCES period_source_file(id),
  UNIQUE(usage_id,revision));
CREATE TABLE IF NOT EXISTS period_close(
  id INTEGER PRIMARY KEY, tenant TEXT NOT NULL, period TEXT NOT NULL, version INTEGER NOT NULL,
  previous_id INTEGER REFERENCES period_close(id), contract_id TEXT NOT NULL REFERENCES period_contract(id),
  source_sha TEXT NOT NULL, total_cents INTEGER NOT NULL, actor TEXT NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP, UNIQUE(tenant,period,version));
CREATE TABLE IF NOT EXISTS period_line(
  close_id INTEGER NOT NULL REFERENCES period_close(id), product TEXT NOT NULL,
  actual_units INTEGER NOT NULL, reserved_used INTEGER NOT NULL, shared_used INTEGER NOT NULL,
  billable_units INTEGER NOT NULL, price_cents INTEGER NOT NULL, charge_cents INTEGER NOT NULL,
  PRIMARY KEY(close_id,product));
CREATE TABLE IF NOT EXISTS period_usage_snapshot(
  close_id INTEGER NOT NULL REFERENCES period_close(id), usage_id TEXT NOT NULL,
  effective_units INTEGER NOT NULL, correction_id TEXT,
  billable_units INTEGER NOT NULL DEFAULT 0, price_cents INTEGER NOT NULL DEFAULT 0,
  charge_cents INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(close_id,usage_id));
CREATE TABLE IF NOT EXISTS period_close_amendment(
  close_id INTEGER NOT NULL REFERENCES period_close(id), amendment_id TEXT NOT NULL,
  PRIMARY KEY(close_id,amendment_id));
CREATE TABLE IF NOT EXISTS period_state(
  tenant TEXT NOT NULL, period TEXT NOT NULL, current_close_id INTEGER REFERENCES period_close(id),
  status TEXT NOT NULL, PRIMARY KEY(tenant,period));
CREATE TABLE IF NOT EXISTS period_restatement(
  id TEXT PRIMARY KEY, tenant TEXT NOT NULL, period TEXT NOT NULL,
  previous_close_id INTEGER NOT NULL REFERENCES period_close(id), candidate_sha TEXT NOT NULL,
  requester TEXT NOT NULL, reason TEXT NOT NULL, reviewer TEXT, review_proof_sha TEXT,
  status TEXT NOT NULL, new_close_id INTEGER REFERENCES period_close(id), delta_cents INTEGER,
  source_sha TEXT NOT NULL, raw BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS period_restatement_line(
  request_id TEXT NOT NULL REFERENCES period_restatement(id), product TEXT NOT NULL,
  old_charge_cents INTEGER NOT NULL, new_charge_cents INTEGER NOT NULL,
  delta_cents INTEGER NOT NULL, PRIMARY KEY(request_id,product));
"""


def connect(path):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(target)
    db.row_factory = sqlite3.Row
    db.executescript(SCHEMA)
    return db
