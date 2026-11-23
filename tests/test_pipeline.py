from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from dqwatch import meta, pipeline, rules
from dqwatch.models import Column, Source
from dqwatch.notify import SmtpConfig

NOW = datetime(2026, 1, 10, tzinfo=timezone.utc)
SMTP = SmtpConfig("h", 587, "", "", "me@example.com")
COLS = [Column("id", "integer"), Column("email", "text"), Column("amount", "number")]


@pytest.fixture
def env(tmp_path):
    inbox = tmp_path / "inbox"
    (inbox / "orders").mkdir(parents=True)
    m = meta.connect(str(tmp_path / "m.sqlite"))
    sid = meta.create_source(m, Source(None, "orders", "folder", "orders", 1, "upsert", "id", COLS))
    return {"inbox": inbox, "meta": m, "duck": duckdb.connect(), "sid": sid}


def _drop(env, name, data):
    (env["inbox"] / "orders" / name).write_bytes(data)


def _run(env, now=NOW, send=None, smtp=SMTP):
    return pipeline.run_source(env["meta"], env["duck"], env["sid"], str(env["inbox"]), now,
                               smtp=smtp, send=send or (lambda m, c: None))


def _results(env, rule_id):
    return [r for r in meta.results_history(env["meta"], env["sid"], 100) if r["rule_id"] == rule_id]


def _rows(env):
    return env["duck"].execute("SELECT count(*) FROM src_1").fetchone()[0]


def test_load_then_checks(env):
    rid = meta.add_rule(env["meta"], env["sid"], {"type": "not_null", "column": "email", "severity": "error"})
    _drop(env, "a.csv", b"id,email,amount\n1,,5\n2,b@x.com,6\n")
    assert _run(env) == "loaded"
    assert _rows(env) == 2
    assert _results(env, rid)[0]["status"] == "fail"
    assert _results(env, "schema")[0]["status"] == "pass"
    assert meta.get_source(env["meta"], env["sid"]).sample[0]["id"] == "1"


def test_crashing_check_does_not_block(env, monkeypatch):
    rid = meta.add_rule(env["meta"], env["sid"], {"type": "not_null", "column": "email", "severity": "error"})
    monkeypatch.setattr(rules, "evaluate", lambda *a, **k: 1 / 0)
    _drop(env, "a.csv", b"id,email,amount\n1,a@x.com,5\n")
    assert _run(env) == "loaded"
    assert _rows(env) == 1
    assert _results(env, rid)[0]["status"] == "error"
    assert meta.latest_runs(env["meta"])[env["sid"]]["status"] == "loaded"


def test_smtp_failure_does_not_block(env):
    meta.add_rule(env["meta"], env["sid"], {"type": "not_null", "column": "email", "severity": "error"})
    _drop(env, "a.csv", b"id,email,amount\n1,,5\n")

    def boom(msg, cfg):
        raise OSError("smtp down")

    assert _run(env, send=boom) == "loaded"
    assert _rows(env) == 1
    assert _results(env, "email")[0]["status"] == "error"


def test_no_change_runs_freshness_only(env):
    nn = meta.add_rule(env["meta"], env["sid"], {"type": "not_null", "column": "email"})
    fr = meta.add_rule(env["meta"], env["sid"], {"type": "freshness", "max_age_hours": 1})
    _drop(env, "a.csv", b"id,email,amount\n1,a@x.com,5\n")
    _run(env)
    assert _run(env, now=NOW + timedelta(hours=2)) == "no_change"
    assert len(_results(env, nn)) == 1
    fresh = _results(env, fr)
    assert [r["status"] for r in fresh] == ["fail", "pass"]


def test_alert_only_on_transition(env):
    meta.add_rule(env["meta"], env["sid"], {"type": "not_null", "column": "email", "severity": "error"})
    sent = []
    _drop(env, "a.csv", b"id,email,amount\n1,,5\n")
    _run(env, send=lambda m, c: sent.append(m))
    _drop(env, "a.csv", b"id,email,amount\n2,,5\n")
    _run(env, send=lambda m, c: sent.append(m))
    assert len(sent) == 1


def test_warn_never_emails(env):
    meta.add_rule(env["meta"], env["sid"], {"type": "not_null", "column": "email", "severity": "warn"})
    sent = []
    _drop(env, "a.csv", b"id,email,amount\n1,,5\n")
    _run(env, send=lambda m, c: sent.append(m))
    assert sent == []


def test_fetch_error_recorded(env):
    meta.create_source(env["meta"], Source(None, "x", "folder", "missing", 1, "append", None, COLS))
    assert pipeline.run_source(env["meta"], env["duck"], 2, str(env["inbox"]), NOW) == "fetch_error"
    res = [r for r in meta.results_history(env["meta"], 2) if r["rule_id"] == "fetch"]
    assert res[0]["status"] == "error"


def test_unreadable_file_recorded_and_others_load(env):
    _drop(env, "a.csv", b"")
    _drop(env, "b.csv", b"id,email,amount\n1,a@x.com,5\n")
    assert _run(env) == "loaded"
    assert _rows(env) == 1
    assert _results(env, "fetch")[0]["status"] == "error"


def test_schema_mismatch_lands(env):
    _drop(env, "a.csv", b"id,email,amount\n1,a@x.com,N/A\n,b@x.com,3\n")
    assert _run(env) == "loaded"
    assert env["duck"].execute("SELECT id, amount FROM src_1").fetchall() == [(1, None)]
    assert _results(env, "schema")[0]["status"] == "fail"
    assert meta.list_attention(env["meta"], env["sid"])[0]["reason"] == "missing key"


def test_load_failure_reraised_and_recorded(env, monkeypatch):
    from dqwatch import load
    monkeypatch.setattr(load, "load", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("disk full")))
    _drop(env, "a.csv", b"id,email,amount\n1,a@x.com,5\n")
    with pytest.raises(RuntimeError):
        _run(env)
    assert meta.latest_runs(env["meta"])[env["sid"]]["status"] == "load_failed"


# --- mid-build review fixes ---

def test_sqlite_error_after_commit_does_not_raise(env, monkeypatch):
    import sqlite3
    def locked(*a, **k):
        raise sqlite3.OperationalError("database is locked")
    monkeypatch.setattr(meta, "record_fingerprint", locked)
    _drop(env, "a.csv", b"id,email,amount\n1,a@x.com,5\n")
    assert _run(env) == "loaded"
    assert _rows(env) == 1
    assert meta.latest_runs(env["meta"])[env["sid"]]["status"] == "loaded"
    assert _results(env, "internal")[0]["status"] == "error"


def test_malformed_stored_rule_is_a_result(env):
    env["meta"].execute("INSERT INTO rules (id, source_id, body) VALUES ('bad1', ?, '[1]')", (env["sid"],))
    env["meta"].execute("INSERT INTO rules (id, source_id, body) VALUES ('bad2', ?, '{oops')", (env["sid"],))
    env["meta"].commit()
    _drop(env, "a.csv", b"id,email,amount\n1,a@x.com,5\n")
    assert _run(env) == "loaded"
    assert _rows(env) == 1
    assert _results(env, "bad1")[0]["status"] == "error"
    assert _results(env, "bad2")[0]["status"] == "error"


def test_later_load_failure_keeps_earlier_alerts(env, monkeypatch):
    from dqwatch import load
    meta.add_rule(env["meta"], env["sid"], {"type": "not_null", "column": "email", "severity": "error"})
    real, calls = load.load, []

    def flaky(*a, **k):
        calls.append(1)
        if len(calls) == 2:
            raise RuntimeError("disk full")
        return real(*a, **k)
    monkeypatch.setattr(load, "load", flaky)
    _drop(env, "a.csv", b"id,email,amount\n1,,5\n")
    _drop(env, "b.csv", b"id,email,amount\n2,b@x.com,5\n")
    os_utime = __import__("os").utime
    os_utime(env["inbox"] / "orders" / "a.csv", (1, 1))
    sent = []
    with pytest.raises(RuntimeError):
        _run(env, send=lambda m, c: sent.append(m))
    assert _rows(env) == 1
    assert len(sent) == 1 and "load" in sent[0].get_content()
    assert meta.latest_runs(env["meta"])[env["sid"]]["status"] == "load_failed"


def test_unreadable_file_alerts_once(env):
    _drop(env, "a.csv", b"   \n")
    sent = []
    for i in range(3):
        _run(env, now=NOW + timedelta(minutes=i), send=lambda m, c: sent.append(m))
    assert len(sent) == 1


# --- final review fixes ---

def test_load_error_clears_and_realerts(env, monkeypatch):
    from dqwatch import load
    real = load.load
    sent = []

    def boom(*a, **k):
        raise RuntimeError("locked")
    _drop(env, "a.csv", b"id,email,amount\n1,a@x.com,5\n")
    monkeypatch.setattr(load, "load", boom)
    with pytest.raises(RuntimeError):
        _run(env, send=lambda m, c: sent.append(m))
    monkeypatch.setattr(load, "load", real)
    _run(env, now=NOW + timedelta(minutes=1), send=lambda m, c: sent.append(m))
    assert _results(env, "load")[0]["status"] == "pass"
    _drop(env, "a.csv", b"id,email,amount\n2,b@x.com,5\n")
    monkeypatch.setattr(load, "load", boom)
    with pytest.raises(RuntimeError):
        _run(env, now=NOW + timedelta(minutes=2), send=lambda m, c: sent.append(m))
    assert len(sent) == 2


def test_internal_and_email_errors_clear(env, monkeypatch):
    import sqlite3
    real_fp = meta.record_fingerprint
    monkeypatch.setattr(meta, "record_fingerprint",
                        lambda *a, **k: (_ for _ in ()).throw(sqlite3.OperationalError("locked")))
    meta.add_rule(env["meta"], env["sid"], {"type": "not_null", "column": "email", "severity": "error"})
    _drop(env, "a.csv", b"id,email,amount\n1,,5\n")

    def bad_send(m, c):
        raise OSError("smtp down")
    _run(env, send=bad_send)
    assert _results(env, "internal")[0]["status"] == "error"
    assert _results(env, "email")[0]["status"] == "error"
    monkeypatch.setattr(meta, "record_fingerprint", real_fp)
    _drop(env, "a.csv", b"id,email,amount\n2,b@x.com,5\n")
    _run(env, now=NOW + timedelta(minutes=1))
    _drop(env, "a.csv", b"id,email,amount\n3,,5\n")
    _run(env, now=NOW + timedelta(minutes=2))
    assert _results(env, "internal")[0]["status"] == "pass"
    assert _results(env, "email")[0]["status"] == "pass"
