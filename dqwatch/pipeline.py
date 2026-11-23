"""fetch -> stage -> load (commit) -> checks -> notify.

The order is the guarantee: data is committed before any check runs, and nothing after the
commit (checks, email) can raise out of run_source or undo the load.
"""
import uuid
from datetime import datetime

from dqwatch import fetch, load, meta, notify, rules, schema
from dqwatch.models import CheckResult


def _table_exists(duck, table: str) -> bool:
    return duck.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
                        [table]).fetchone()[0] > 0


def _safe_eval(duck, table, rule, columns, batch_id, now) -> CheckResult:
    try:
        return rules.evaluate(duck, table, rules.validate_rule(rule, columns), batch_id, now)
    except Exception as e:  # a broken check is a result, never a failed load
        return CheckResult(rule.get("id", rule.get("type", "?")), "error", rule.get("severity", "error"),
                           detail=f"The check couldn't run: {e!r}")


def run_source(meta_con, duck_con, source_id: int, inbox_root: str, now: datetime,
               smtp: notify.SmtpConfig | None = None, send=None) -> str:
    source = meta.get_source(meta_con, source_id)
    table = load.table_name(source_id)
    at = now.isoformat()
    run_id = meta.start_run(meta_con, source_id, at)
    results: list[tuple[CheckResult, str | None]] = []

    def record(res: CheckResult):
        prev = meta.last_status(meta_con, source_id, res.rule_id)
        meta.record_result(meta_con, run_id, source_id, res, at)
        results.append((res, prev))

    status = "no_change"
    try:
        items = fetch.fetch(source, inbox_root)
        record(CheckResult("fetch", "pass", "error"))
    except fetch.FetchError as e:
        items, status = [], "fetch_error"
        record(CheckResult("fetch", "error", "error", detail=str(e)))

    user_rules = meta.list_rules(meta_con, source_id)
    for item, data in items:
        sha = fetch.sha256(data)
        if not meta.fingerprint_changed(meta_con, source_id, item, sha):
            continue
        batch_id = uuid.uuid4().hex
        try:
            report = schema.stage(duck_con, data, source.columns, source.key_column)
        except ValueError as e:
            record(CheckResult("fetch", "error", "error", detail=f"{item or source.location}: {e}"))
            continue
        load.ensure_table(duck_con, source)
        try:
            load.load(duck_con, source, batch_id, now)
        except Exception as e:
            meta.finish_run(meta_con, run_id, "load_failed", repr(e))
            raise
        # --- the load is committed; nothing below may fail the run ---
        status = "loaded"
        meta.record_fingerprint(meta_con, source_id, item, sha)
        for row in report.null_key_rows:
            meta.add_attention(meta_con, source_id, batch_id, "missing key", row)
        try:
            meta.update_sample(meta_con, source_id, schema.preview(data))
        except Exception:
            pass  # the sample only feeds the UI/assistant
        record(rules.schema_result(report))
        if source.key_column:
            record(rules.duplicate_keys_result(report))
        for rule in user_rules:
            record(_safe_eval(duck_con, table, rule, source.columns, batch_id, now))

    if status != "loaded" and _table_exists(duck_con, table):
        for rule in user_rules:
            if rule.get("type") == "freshness":
                record(_safe_eval(duck_con, table, rule, source.columns, None, now))

    newly_failing = [r for r, prev in results
                     if r.severity == "error" and r.status in ("fail", "error") and prev in (None, "pass")]
    if newly_failing and smtp:
        try:
            notify.send_alert(source.name, newly_failing, smtp, send=send)
        except Exception as e:
            meta.record_result(meta_con, run_id, source_id,
                               CheckResult("email", "error", "warn", detail=f"Alert email failed: {e!r}"), at)

    meta.finish_run(meta_con, run_id, status)
    return status
