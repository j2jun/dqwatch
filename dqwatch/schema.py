"""Schema inference and staging: cast to the confirmed schema without ever rejecting a row."""
import os
import tempfile
from contextlib import contextmanager

import duckdb

from dqwatch.models import Column, StageReport

TYPES = {"integer": "BIGINT", "number": "DOUBLE", "text": "VARCHAR", "boolean": "BOOLEAN",
         "date": "DATE", "timestamp": "TIMESTAMP"}


def qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def ql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@contextmanager
def _csv_file(csv_bytes: bytes):
    fd, path = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(csv_bytes)
        yield path
    finally:
        os.unlink(path)


def _read(con, csv_bytes: bytes, name: str, all_varchar: bool):
    if not csv_bytes.strip():
        raise ValueError("The file is empty.")
    with _csv_file(csv_bytes) as path:
        try:
            con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT * FROM read_csv({ql(path)}, "
                        f"header=true, all_varchar={str(all_varchar).lower()})")
        except duckdb.Error as e:
            raise ValueError(f"Couldn't read this file as CSV: {e}") from e
    desc = con.execute(f"DESCRIBE {name}").fetchall()
    if not desc:
        raise ValueError("The file has no columns.")
    return desc


def _type_name(duck_type: str) -> str:
    t = duck_type.upper()
    if t in ("BIGINT", "INTEGER", "SMALLINT", "TINYINT", "HUGEINT", "UBIGINT", "UINTEGER"):
        return "integer"
    if t in ("DOUBLE", "FLOAT", "REAL") or t.startswith("DECIMAL"):
        return "number"
    if t == "BOOLEAN":
        return "boolean"
    if t == "DATE":
        return "date"
    if t.startswith("TIMESTAMP"):
        return "timestamp"
    return "text"


def infer_columns(csv_bytes: bytes) -> list[Column]:
    con = duckdb.connect()
    return [Column(r[0], _type_name(r[1])) for r in _read(con, csv_bytes, "infer_in", False)]


def preview(csv_bytes: bytes, n: int = 20) -> list[dict]:
    con = duckdb.connect()
    cols = [r[0] for r in _read(con, csv_bytes, "preview_in", True)]
    return [dict(zip(cols, row)) for row in con.execute(f"SELECT * FROM preview_in LIMIT {int(n)}").fetchall()]


def stage(con, csv_bytes: bytes, columns: list[Column], key: str | None) -> StageReport:
    """Create temp table `staging`: typed columns, _raw (original row JSON when a cast failed),
    _row_hash, _row_idx. Null-key rows are removed and returned; duplicate keys keep the last row."""
    file_cols = [r[0] for r in _read(con, csv_bytes, "raw_in", True)]
    configured = [c.name for c in columns]
    missing = [c for c in configured if c not in file_cols]
    new = [c for c in file_cols if c not in configured]

    typed, fails = [], []
    for c in columns:
        if c.name not in file_cols:
            typed.append(f"NULL::{TYPES[c.type]} AS {qi(c.name)}")
            continue
        val = f"NULLIF(trim({qi(c.name)}), '')"
        typed.append(f"TRY_CAST({val} AS {TYPES[c.type]}) AS {qi(c.name)}")
        if c.type != "text":
            fails.append(f"({val} IS NOT NULL AND TRY_CAST({val} AS {TYPES[c.type]}) IS NULL)")
    failed = " OR ".join(fails) or "false"
    raw_json = "json_object(" + ", ".join(f"{ql(c)}, {qi(c)}" for c in file_cols) + ")::VARCHAR"
    row_hash = "md5(concat_ws(chr(31), " + ", ".join(f"coalesce({qi(c)}, chr(0))" for c in file_cols) + "))"
    con.execute(
        "CREATE OR REPLACE TEMP TABLE staging AS SELECT " + ", ".join(typed) +
        f", CASE WHEN {failed} THEN {raw_json} END AS _raw, {row_hash} AS _row_hash,"
        f" row_number() OVER () AS _row_idx, ({failed}) AS _failed FROM raw_in")
    cast_failed = con.execute("SELECT count(*) FROM staging WHERE _failed").fetchone()[0]
    con.execute("ALTER TABLE staging DROP COLUMN _failed")

    null_key_rows, dupes = [], 0
    if key:
        k = qi(key)
        rows = con.execute(
            "WITH r AS (SELECT *, row_number() OVER () AS _i FROM raw_in) "
            f"SELECT {', '.join('r.' + qi(c) for c in file_cols)} FROM r JOIN staging s ON s._row_idx = r._i "
            f"WHERE s.{k} IS NULL ORDER BY r._i").fetchall()
        null_key_rows = [dict(zip(file_cols, row)) for row in rows]
        con.execute(f"DELETE FROM staging WHERE {k} IS NULL")
        before = con.execute("SELECT count(*) FROM staging").fetchone()[0]
        con.execute(f"DELETE FROM staging WHERE _row_idx NOT IN (SELECT max(_row_idx) FROM staging GROUP BY {k})")
        dupes = before - con.execute("SELECT count(*) FROM staging").fetchone()[0]

    return StageReport(new, missing, cast_failed, null_key_rows, dupes)
