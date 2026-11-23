"""Target tables and write modes (replace / append / upsert), each in one transaction."""
from datetime import datetime

from dqwatch.models import Source
from dqwatch.schema import TYPES, qi


def table_name(source_id: int) -> str:
    return f"src_{int(source_id)}"


def ensure_table(con, source: Source):
    cols = ", ".join(f"{qi(c.name)} {TYPES[c.type]}" for c in source.columns)
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name(source.id)} ({cols}, _raw VARCHAR, "
                "_row_hash VARCHAR, _batch_id VARCHAR, _loaded_at TIMESTAMP)")


def _insert_sql(source: Source, where: str) -> str:
    names = ", ".join(qi(c.name) for c in source.columns)
    return (f"INSERT INTO {table_name(source.id)} ({names}, _raw, _row_hash, _batch_id, _loaded_at) "
            f"SELECT {names}, _raw, _row_hash, ?, ? FROM staging {where}")


def load(con, source: Source, batch_id: str, now: datetime) -> int:
    t = table_name(source.id)
    where = ""
    con.begin()
    try:
        if source.write_mode == "replace":
            con.execute(f"DELETE FROM {t}")
        elif source.write_mode == "append":
            where = f"WHERE _row_hash NOT IN (SELECT _row_hash FROM {t} WHERE _row_hash IS NOT NULL)"
        elif source.write_mode == "upsert":
            k = qi(source.key_column)
            con.execute(f"DELETE FROM {t} WHERE {k} IN (SELECT {k} FROM staging)")
        else:
            raise ValueError(f"unknown write mode {source.write_mode!r}")
        con.execute(_insert_sql(source, where), [batch_id, now.replace(tzinfo=None)])
        written = con.execute(f"SELECT count(*) FROM {t} WHERE _batch_id = ?", [batch_id]).fetchone()[0]
        con.commit()
        return written
    except Exception:
        con.rollback()
        raise
