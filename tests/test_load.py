from datetime import datetime, timezone

import duckdb
import pytest

from dqwatch import load, schema
from dqwatch.models import Column, Source

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
COLS = [Column("id", "integer"), Column("v", "text")]


def _src(mode):
    return Source(1, "s", "folder", "s", 1, mode, "id" if mode == "upsert" else None, COLS)


def _load(con, src, data, batch):
    schema.stage(con, data, src.columns, src.key_column)
    load.ensure_table(con, src)
    return load.load(con, src, batch, NOW)


def _rows(con):
    return con.execute("SELECT id, v FROM src_1 ORDER BY id").fetchall()


def test_replace():
    con, src = duckdb.connect(), _src("replace")
    _load(con, src, b"id,v\n1,a\n2,b\n", "b1")
    _load(con, src, b"id,v\n3,c\n", "b2")
    assert _rows(con) == [(3, "c")]


def test_append_skips_exact_duplicates():
    con, src = duckdb.connect(), _src("append")
    assert _load(con, src, b"id,v\n1,a\n", "b1") == 1
    assert _load(con, src, b"id,v\n1,a\n2,b\n", "b2") == 1
    assert _rows(con) == [(1, "a"), (2, "b")]


def test_upsert_updates_and_keeps_vanished():
    con, src = duckdb.connect(), _src("upsert")
    _load(con, src, b"id,v\n1,a\n2,b\n", "b1")
    _load(con, src, b"id,v\n1,z\n", "b2")
    assert _rows(con) == [(1, "z"), (2, "b")]
    assert con.execute("SELECT _batch_id FROM src_1 WHERE id = 1").fetchone() == ("b2",)


def test_rollback_on_error(monkeypatch):
    con, src = duckdb.connect(), _src("replace")
    _load(con, src, b"id,v\n1,a\n", "b1")
    schema.stage(con, b"id,v\n2,b\n", COLS, None)
    monkeypatch.setattr(load, "_insert_sql", lambda *a: "INSERT INTO no_such_table VALUES (1)")
    with pytest.raises(duckdb.Error):
        load.load(con, src, "b2", NOW)
    assert _rows(con) == [(1, "a")]


def test_append_dedupes_within_one_file():
    con, src = duckdb.connect(), _src("append")
    assert _load(con, src, b"id,v\n1,a\n1,a\n2,b\n", "b1") == 2
    assert _rows(con) == [(1, "a"), (2, "b")]
