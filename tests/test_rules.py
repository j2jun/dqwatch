from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from dqwatch import load, rules, schema
from dqwatch.models import Column, Source, StageReport

NOW = datetime(2026, 1, 10, tzinfo=timezone.utc)
COLS = [Column("order id", "integer"), Column("email", "text"), Column("amount", "number"),
        Column("status", "text"), Column("day", "date")]
HEADER = b'"order id",email,amount,status,day\n'


def _table(*batches):
    """Each batch is a list of CSV lines; loaded in append mode. Returns (con, last_batch_id)."""
    con = duckdb.connect()
    src = Source(1, "s", "folder", "s", 1, "append", None, COLS)
    for i, lines in enumerate(batches):
        schema.stage(con, HEADER + "\n".join(lines).encode() + b"\n", COLS, None)
        load.ensure_table(con, src)
        load.load(con, src, f"b{i}", NOW)
    return con, f"b{len(batches) - 1}"


def _eval(con, batch, rule):
    return rules.evaluate(con, "src_1", rules.validate_rule(rule, COLS), batch, NOW)


GOOD = ["1,a@x.com,5,paid,2026-01-09", "2,b@x.com,7,open,2026-01-09"]


@pytest.mark.parametrize("rule,bad_line", [
    ({"type": "not_null", "column": "email"}, "3,,5,paid,2026-01-09"),
    ({"type": "range", "column": "amount", "min": 0, "max": 10}, "3,c@x.com,50,paid,2026-01-09"),
    ({"type": "allowed", "column": "status", "values": ["paid", "open"]}, "3,c@x.com,5,lost,2026-01-09"),
    ({"type": "pattern", "column": "email", "preset": "email"}, "3,not-an-email,5,paid,2026-01-09"),
])
def test_row_rules(rule, bad_line):
    con, b = _table(GOOD)
    assert _eval(con, b, rule).status == "pass"
    con, b = _table(GOOD + [bad_line])
    res = _eval(con, b, rule)
    assert res.status == "fail" and res.failing_count == 1 and len(res.sample) == 1


def test_unique():
    con, b = _table(GOOD)
    assert _eval(con, b, {"type": "unique", "column": "order id"}).status == "pass"
    con, b = _table(GOOD + ["1,c@x.com,5,paid,2026-01-09"])
    res = _eval(con, b, {"type": "unique", "column": "order id"})
    assert res.status == "fail" and res.failing_count == 2


def test_row_count():
    con, b = _table(GOOD)
    assert _eval(con, b, {"type": "row_count", "min": 2}).status == "pass"
    assert _eval(con, b, {"type": "row_count", "min": 3}).status == "fail"


def test_freshness():
    con, b = _table(GOOD)
    assert _eval(con, b, {"type": "freshness", "column": "day", "max_age_hours": 48}).status == "pass"
    assert _eval(con, b, {"type": "freshness", "column": "day", "max_age_hours": 12}).status == "fail"
    assert _eval(con, b, {"type": "freshness", "max_age_hours": 1}).status == "pass"  # _loaded_at == NOW
    later = rules.evaluate(con, "src_1", rules.validate_rule({"type": "freshness", "max_age_hours": 1}, COLS),
                           b, NOW + timedelta(hours=2))
    assert later.status == "fail"


def test_batch_scope():
    con, b = _table(["1,,5,paid,2026-01-09"], ["1,b@x.com,5,paid,2026-01-09"])
    assert _eval(con, b, {"type": "not_null", "column": "email"}).status == "pass"
    assert _eval(con, b, {"type": "unique", "column": "order id"}).status == "fail"


@pytest.mark.parametrize("rule", [
    {"type": "not_null", "column": "nope"},
    {"type": "range", "column": "amount"},
    {"type": "pattern", "column": "email", "preset": "zip"},
    {"type": "freshness", "column": "email", "max_age_hours": 1},
    {"type": "bogus"},
])
def test_validate_rejects(rule):
    with pytest.raises(ValueError):
        rules.validate_rule(rule, COLS)


def test_validate_defaults_severity():
    assert rules.validate_rule({"type": "not_null", "column": "email"}, COLS)["severity"] == "error"


def test_injection_safe():
    con, b = _table(GOOD)
    res = _eval(con, b, {"type": "allowed", "column": "status", "values": ["x'); DROP TABLE src_1;--"]})
    assert res.status == "fail"
    assert con.execute("SELECT count(*) FROM src_1").fetchone() == (2,)


def test_schema_and_dup_results():
    ok = StageReport([], [], 0, [], 0)
    assert rules.schema_result(ok).status == "pass"
    bad = rules.schema_result(StageReport(["extra"], ["amount"], 2, [{"id": None}], 0))
    assert bad.status == "fail" and bad.severity == "error"
    assert "extra" in bad.detail and "amount" in bad.detail and "2" in bad.detail
    assert rules.duplicate_keys_result(StageReport([], [], 0, [], 3)).status == "fail"
    assert rules.duplicate_keys_result(ok).severity == "warn"


@pytest.mark.parametrize("rule", [
    {"type": "range", "column": "status", "min": 1},          # text column
    {"type": "range", "column": "amount", "min": "abc"},      # non-numeric bound
    {"type": "range", "column": "amount", "min": True},       # bool is not a number here
    {"type": "freshness", "max_age_hours": True},
    {"type": "row_count", "min": True},
])
def test_validate_rejects_bad_types(rule):
    with pytest.raises(ValueError):
        rules.validate_rule(rule, COLS)
