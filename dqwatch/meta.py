"""SQLite metadata store: sources, rules, runs, results, fingerprints, attention rows."""
import json
import sqlite3
import uuid

from dqwatch.models import CheckResult, Column, Source

SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
  id INTEGER PRIMARY KEY, name TEXT NOT NULL, kind TEXT NOT NULL, location TEXT NOT NULL,
  schedule_minutes INTEGER NOT NULL, write_mode TEXT NOT NULL, key_column TEXT,
  columns TEXT NOT NULL, sample TEXT NOT NULL DEFAULT '[]');
CREATE TABLE IF NOT EXISTS rules (
  id TEXT NOT NULL, source_id INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY (source_id, id));
CREATE TABLE IF NOT EXISTS fingerprints (
  source_id INTEGER NOT NULL, item TEXT NOT NULL, sha TEXT NOT NULL, PRIMARY KEY (source_id, item));
CREATE TABLE IF NOT EXISTS runs (
  id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL, started_at TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'running', detail TEXT NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS results (
  id INTEGER PRIMARY KEY, run_id INTEGER NOT NULL, source_id INTEGER NOT NULL, rule_id TEXT NOT NULL,
  status TEXT NOT NULL, severity TEXT NOT NULL, failing_count INTEGER NOT NULL,
  sample TEXT NOT NULL, detail TEXT NOT NULL, at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS results_by_rule ON results (source_id, rule_id, id);
CREATE TABLE IF NOT EXISTS attention (
  id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL, batch_id TEXT NOT NULL,
  reason TEXT NOT NULL, raw TEXT NOT NULL);
"""


def connect(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path, timeout=30, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(SCHEMA)
    return con


def _source(row) -> Source:
    return Source(row["id"], row["name"], row["kind"], row["location"], row["schedule_minutes"],
                  row["write_mode"], row["key_column"],
                  [Column(**c) for c in json.loads(row["columns"])], json.loads(row["sample"]))


def create_source(con, src: Source) -> int:
    cur = con.execute(
        "INSERT INTO sources (name, kind, location, schedule_minutes, write_mode, key_column, columns, sample)"
        " VALUES (?,?,?,?,?,?,?,?)",
        (src.name, src.kind, src.location, src.schedule_minutes, src.write_mode, src.key_column,
         json.dumps([c.__dict__ for c in src.columns]), json.dumps(src.sample)))
    con.commit()
    return cur.lastrowid


def get_source(con, id: int) -> Source:
    row = con.execute("SELECT * FROM sources WHERE id = ?", (id,)).fetchone()
    if row is None:
        raise KeyError(id)
    return _source(row)


def list_sources(con) -> list[Source]:
    return [_source(r) for r in con.execute("SELECT * FROM sources ORDER BY id")]


def update_sample(con, id: int, rows: list[dict]):
    con.execute("UPDATE sources SET sample = ? WHERE id = ?", (json.dumps(rows), id))
    con.commit()


def add_rule(con, source_id: int, rule: dict) -> str:
    rule = {**rule, "id": uuid.uuid4().hex[:8]}
    con.execute("INSERT INTO rules (id, source_id, body) VALUES (?,?,?)",
                (rule["id"], source_id, json.dumps(rule)))
    con.commit()
    return rule["id"]


def list_rules(con, source_id: int) -> list[dict]:
    """A corrupt stored rule comes back as {"id": ..., "type": None} so callers report it, not crash."""
    out = []
    for r in con.execute("SELECT id, body FROM rules WHERE source_id = ? ORDER BY rowid", (source_id,)):
        try:
            body = json.loads(r["body"])
        except ValueError:
            body = None
        out.append(body if isinstance(body, dict) else {"id": r["id"], "type": None})
    return out


def delete_rule(con, source_id: int, rule_id: str):
    con.execute("DELETE FROM rules WHERE source_id = ? AND id = ?", (source_id, rule_id))
    con.commit()


def fingerprint_changed(con, source_id: int, item: str, sha: str) -> bool:
    row = con.execute("SELECT sha FROM fingerprints WHERE source_id = ? AND item = ?",
                      (source_id, item)).fetchone()
    return row is None or row["sha"] != sha


def record_fingerprint(con, source_id: int, item: str, sha: str):
    con.execute("INSERT OR REPLACE INTO fingerprints (source_id, item, sha) VALUES (?,?,?)",
                (source_id, item, sha))
    con.commit()


def start_run(con, source_id: int, at: str) -> int:
    cur = con.execute("INSERT INTO runs (source_id, started_at) VALUES (?,?)", (source_id, at))
    con.commit()
    return cur.lastrowid


def finish_run(con, run_id: int, status: str, detail: str = ""):
    con.execute("UPDATE runs SET status = ?, detail = ? WHERE id = ?", (status, detail, run_id))
    con.commit()


def last_status(con, source_id: int, rule_id: str) -> str | None:
    row = con.execute("SELECT status FROM results WHERE source_id = ? AND rule_id = ? ORDER BY id DESC LIMIT 1",
                      (source_id, rule_id)).fetchone()
    return row["status"] if row else None


def record_result(con, run_id: int, source_id: int, res: CheckResult, at: str):
    con.execute(
        "INSERT INTO results (run_id, source_id, rule_id, status, severity, failing_count, sample, detail, at)"
        " VALUES (?,?,?,?,?,?,?,?,?)",
        (run_id, source_id, res.rule_id, res.status, res.severity, res.failing_count,
         json.dumps(res.sample, default=str), res.detail, at))
    con.commit()


def results_history(con, source_id: int, limit: int = 30) -> list[dict]:
    rows = con.execute("SELECT * FROM results WHERE source_id = ? ORDER BY id DESC LIMIT ?", (source_id, limit))
    return [{**dict(r), "sample": json.loads(r["sample"])} for r in rows]


def add_attention(con, source_id: int, batch_id: str, reason: str, raw: dict):
    con.execute("INSERT INTO attention (source_id, batch_id, reason, raw) VALUES (?,?,?,?)",
                (source_id, batch_id, reason, json.dumps(raw)))
    con.commit()


def list_attention(con, source_id: int) -> list[dict]:
    rows = con.execute("SELECT * FROM attention WHERE source_id = ? ORDER BY id DESC", (source_id,))
    return [{**dict(r), "raw": json.loads(r["raw"])} for r in rows]


def latest_runs(con) -> dict[int, dict]:
    rows = con.execute("SELECT * FROM runs WHERE id IN (SELECT MAX(id) FROM runs GROUP BY source_id)")
    return {r["source_id"]: dict(r) for r in rows}
