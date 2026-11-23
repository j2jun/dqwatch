import json

import duckdb
import pytest

from dqwatch import schema
from dqwatch.models import Column


def _con():
    return duckdb.connect()


def test_infer_types():
    cols = schema.infer_columns(b"id,amount,when\n1,2.5,2026-01-02\n")
    assert cols == [Column("id", "integer"), Column("amount", "number"), Column("when", "date")]


def test_preview():
    rows = schema.preview(b"id,amount\n1,2.5\n2,3\n", n=1)
    assert rows == [{"id": "1", "amount": "2.5"}]


def test_stage_cast_failure_keeps_raw():
    con = _con()
    cols = [Column("id", "integer"), Column("amount", "number")]
    rep = schema.stage(con, b"id,amount\n1,N/A\n2,3.5\n", cols, None)
    rows = con.execute("SELECT id, amount, _raw FROM staging ORDER BY _row_idx").fetchall()
    assert rows[0][1] is None and "N/A" in json.loads(rows[0][2]).values()
    assert rows[1] == (2, 3.5, None)
    assert rep.cast_failed_rows == 1


def test_stage_missing_and_new_columns():
    con = _con()
    rep = schema.stage(con, b"id,extra\n1,x\n", [Column("id", "integer"), Column("amount", "number")], None)
    assert rep.missing_columns == ["amount"] and rep.new_columns == ["extra"]
    assert con.execute("SELECT amount FROM staging").fetchone() == (None,)


def test_stage_null_and_duplicate_keys():
    con = _con()
    rep = schema.stage(con, b"id,v\n1,a\n,b\n1,c\n", [Column("id", "integer"), Column("v", "text")], "id")
    assert con.execute("SELECT id, v FROM staging").fetchall() == [(1, "c")]
    assert rep.duplicate_keys == 1
    assert rep.null_key_rows == [{"id": None, "v": "b"}]


def test_weird_headers():
    con = _con()
    data = b'"order id","a""b",drop table\n1,2,3\n'
    cols = [Column("order id", "integer"), Column('a"b', "integer"), Column("drop table", "integer")]
    schema.stage(con, data, cols, None)
    assert con.execute('SELECT "order id", "a""b", "drop table" FROM staging').fetchone() == (1, 2, 3)


def test_header_only_and_bom():
    con = _con()
    rep = schema.stage(con, b"\xef\xbb\xbfid,amount\n", [Column("id", "integer"), Column("amount", "number")], None)
    assert con.execute("SELECT count(*) FROM staging").fetchone() == (0,)
    assert rep.missing_columns == []


def test_garbage_raises():
    with pytest.raises(ValueError):
        schema.stage(_con(), b"", [Column("id", "integer")], None)
