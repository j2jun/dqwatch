from dqwatch import meta
from dqwatch.models import CheckResult, Column, Source


def _src():
    return Source(None, "orders", "folder", "orders", 1, "upsert", "id",
                  [Column("id", "integer"), Column("email", "text")])


def test_source_roundtrip(tmp_path):
    con = meta.connect(str(tmp_path / "m.sqlite"))
    sid = meta.create_source(con, _src())
    got = meta.get_source(con, sid)
    assert got.id == sid
    assert got.name == "orders" and got.write_mode == "upsert" and got.key_column == "id"
    assert got.columns == [Column("id", "integer"), Column("email", "text")]
    assert [s.id for s in meta.list_sources(con)] == [sid]


def test_rules_add_list_delete(tmp_path):
    con = meta.connect(str(tmp_path / "m.sqlite"))
    sid = meta.create_source(con, _src())
    rid = meta.add_rule(con, sid, {"type": "not_null", "column": "email"})
    assert len(rid) == 8
    assert [r["id"] for r in meta.list_rules(con, sid)] == [rid]
    meta.delete_rule(con, sid, rid)
    assert meta.list_rules(con, sid) == []


def test_fingerprint(tmp_path):
    con = meta.connect(str(tmp_path / "m.sqlite"))
    assert meta.fingerprint_changed(con, 1, "a.csv", "x")
    meta.record_fingerprint(con, 1, "a.csv", "x")
    assert not meta.fingerprint_changed(con, 1, "a.csv", "x")
    assert meta.fingerprint_changed(con, 1, "a.csv", "y")


def test_last_status_and_history(tmp_path):
    con = meta.connect(str(tmp_path / "m.sqlite"))
    sid = meta.create_source(con, _src())
    run = meta.start_run(con, sid, "2026-01-01T00:00:00Z")
    assert meta.last_status(con, sid, "r1") is None
    meta.record_result(con, run, sid, CheckResult("r1", "pass", "error"), "2026-01-01T00:00:00Z")
    meta.record_result(con, run, sid, CheckResult("r1", "fail", "error", 2, [{"a": 1}]), "2026-01-01T00:01:00Z")
    meta.finish_run(con, run, "loaded")
    assert meta.last_status(con, sid, "r1") == "fail"
    hist = meta.results_history(con, sid)
    assert [h["status"] for h in hist] == ["fail", "pass"]
    assert hist[0]["sample"] == [{"a": 1}]
    assert meta.latest_runs(con)[sid]["status"] == "loaded"


def test_attention(tmp_path):
    con = meta.connect(str(tmp_path / "m.sqlite"))
    meta.add_attention(con, 1, "b1", "missing key", {"id": "", "email": "x"})
    rows = meta.list_attention(con, 1)
    assert rows[0]["reason"] == "missing key" and rows[0]["raw"]["email"] == "x"
