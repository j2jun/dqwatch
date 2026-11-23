"""No-code rules: validate the stored JSON, evaluate it against DuckDB."""
from datetime import datetime, timedelta

from dqwatch.models import CheckResult, Column, StageReport
from dqwatch.schema import qi

RULE_TYPES = {"not_null", "unique", "range", "allowed", "pattern", "row_count", "freshness"}
PATTERNS = {"email": r"^[^@\s]+@[^@\s]+\.[^@\s]+$", "date": r"^\d{4}-\d{2}-\d{2}$", "digits": r"^\d+$"}
SEVERITIES = {"warn", "error"}
_TIME_TYPES = {"date", "timestamp"}
_INTERNAL = "_raw, _row_hash, _batch_id, _loaded_at"


def validate_rule(rule: dict, columns: list[Column]) -> dict:
    r = {**rule}
    r.setdefault("severity", "error")
    t = r.get("type")
    if t not in RULE_TYPES:
        raise ValueError(f"Unknown check type: {t!r}.")
    if r["severity"] not in SEVERITIES:
        raise ValueError("Severity must be 'warn' or 'error'.")
    types = {c.name: c.type for c in columns}
    if t == "freshness":
        r.setdefault("column", "_loaded_at")
        if r["column"] != "_loaded_at" and types.get(r["column"]) not in _TIME_TYPES:
            raise ValueError("Freshness needs a date or timestamp column.")
        hours = r.get("max_age_hours")
        if not isinstance(hours, (int, float)) or hours <= 0:
            raise ValueError("Freshness needs a maximum age in hours greater than 0.")
    elif t == "row_count":
        if not isinstance(r.get("min"), int) or r["min"] < 0:
            raise ValueError("Row count needs a whole-number minimum.")
    else:
        if r.get("column") not in types:
            raise ValueError(f"Column {r.get('column')!r} isn't in this dataset.")
        if t == "range":
            lo, hi = r.get("min"), r.get("max")
            if lo is None and hi is None:
                raise ValueError("Set a minimum, a maximum, or both.")
            if lo is not None and hi is not None and lo > hi:
                raise ValueError("The minimum is larger than the maximum.")
        elif t == "allowed":
            if not isinstance(r.get("values"), list) or not r["values"]:
                raise ValueError("List at least one allowed value.")
        elif t == "pattern":
            if r.get("preset") not in PATTERNS:
                raise ValueError(f"Pick a pattern: {', '.join(PATTERNS)}.")
    return r


def _failing(con, table, where, params, batch_id, rule) -> CheckResult:
    scope = "_batch_id = ? AND " if batch_id is not None else ""
    args = ([batch_id] if batch_id is not None else []) + params
    count = con.execute(f"SELECT count(*) FROM {table} WHERE {scope}({where})", args).fetchone()[0]
    cur = con.execute(f"SELECT * EXCLUDE ({_INTERNAL}) FROM {table} WHERE {scope}({where}) LIMIT 5", args)
    names = [d[0] for d in cur.description]
    sample = [dict(zip(names, row)) for row in cur.fetchall()]
    return CheckResult(rule["id"] if "id" in rule else rule["type"], "fail" if count else "pass",
                       rule["severity"], count, sample)


def evaluate(con, table: str, rule: dict, batch_id: str | None, now: datetime) -> CheckResult:
    t, rid, sev = rule["type"], rule.get("id", rule["type"]), rule["severity"]
    col = qi(rule.get("column", "_loaded_at" if t == "freshness" else "")) if t != "row_count" else None
    if t == "not_null":
        return _failing(con, table, f"{col} IS NULL", [], batch_id, rule)
    if t == "range":
        conds, params = [], []
        if rule.get("min") is not None:
            conds.append(f"{col} < ?"); params.append(rule["min"])
        if rule.get("max") is not None:
            conds.append(f"{col} > ?"); params.append(rule["max"])
        return _failing(con, table, " OR ".join(conds), params, batch_id, rule)
    if t == "allowed":
        marks = ", ".join("?" for _ in rule["values"])
        return _failing(con, table, f"{col} IS NOT NULL AND {col}::VARCHAR NOT IN ({marks})",
                        [str(v) for v in rule["values"]], batch_id, rule)
    if t == "pattern":
        return _failing(con, table, f"{col} IS NOT NULL AND NOT regexp_matches({col}::VARCHAR, ?)",
                        [PATTERNS[rule["preset"]]], batch_id, rule)
    if t == "unique":
        return _failing(con, table, f"{col} IN (SELECT {col} FROM {table} GROUP BY {col} HAVING count(*) > 1)",
                        [], None, rule)
    if t == "row_count":
        n = con.execute(f"SELECT count(*) FROM {table} WHERE _batch_id = ?", [batch_id]).fetchone()[0]
        return CheckResult(rid, "pass" if n >= rule["min"] else "fail", sev, 0 if n >= rule["min"] else 1,
                           detail=f"{n} rows arrived; at least {rule['min']} expected.")
    if t == "freshness":
        newest = con.execute(f"SELECT max({col})::TIMESTAMP FROM {table}").fetchone()[0]
        cutoff = now.replace(tzinfo=None) - timedelta(hours=rule["max_age_hours"])
        ok = newest is not None and newest >= cutoff
        return CheckResult(rid, "pass" if ok else "fail", sev, 0 if ok else 1,
                           detail=f"Newest value: {newest or 'none'}; must be within {rule['max_age_hours']} hours.")
    raise ValueError(f"Unknown check type: {t!r}.")


def schema_result(report: StageReport) -> CheckResult:
    problems = []
    if report.cast_failed_rows:
        problems.append(f"{report.cast_failed_rows} row(s) had values that didn't match the column type "
                        "(stored as empty; originals kept)")
    if report.missing_columns:
        problems.append("missing columns: " + ", ".join(report.missing_columns))
    if report.new_columns:
        problems.append("new columns not stored: " + ", ".join(report.new_columns))
    if report.null_key_rows:
        problems.append(f"{len(report.null_key_rows)} row(s) had no key and were set aside")
    return CheckResult("schema", "fail" if problems else "pass", "error",
                       report.cast_failed_rows + len(report.null_key_rows), report.null_key_rows[:5],
                       "; ".join(problems))


def duplicate_keys_result(report: StageReport) -> CheckResult:
    n = report.duplicate_keys
    return CheckResult("duplicate_keys", "fail" if n else "pass", "warn", n,
                       detail=f"{n} duplicate key row(s) in the file; the last one was kept." if n else "")
