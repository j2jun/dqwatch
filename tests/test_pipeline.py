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
