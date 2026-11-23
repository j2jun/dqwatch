"""fetch -> stage -> load (commit) -> checks -> notify.

The order is the guarantee: data is committed before any check runs, and nothing after a
commit (checks, bookkeeping, email) can raise out of run_source or undo the load. A load that
itself fails is recorded and alerted like a check, the run finishes, and only then is the error
re-raised so Dagster shows a failed run.
"""
import uuid
from datetime import datetime

from dqwatch import fetch, load, meta, notify, rules, schema
from dqwatch.models import CheckResult


def _table_exists(duck, table: str) -> bool:
    return duck.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
                        [table]).fetchone()[0] > 0


def _safe_eval(duck, table, rule, columns, batch_id, now) -> CheckResult:
    r = rule if isinstance(rule, dict) else {}
    try:
        return rules.evaluate(duck, table, rules.validate_rule(r, columns), batch_id, now)
    except Exception as e:  # a broken check is a result, never a failed load
        return CheckResult(str(r.get("id", r.get("type", "?"))), "error", str(r.get("severity", "error")),
                           detail=f"The check couldn't run: {e}")


def run_source(meta_con, duck_con, source_id: int, inbox_root: str, now: datetime,
               smtp: notify.SmtpConfig | None = None, send=None) -> str:
    source = meta.get_source(meta_con, source_id)
    table = load.table_name(source_id)
    at = now.isoformat()
    run_id = meta.start_run(meta_con, source_id, at)
    results: list[tuple[CheckResult, str | None]] = []

    def record(res: CheckResult):
        prev = None
        try:
            prev = meta.last_status(meta_con, source_id, res.rule_id)
            meta.record_result(meta_con, run_id, source_id, res, at)
        except Exception:
            pass  # bookkeeping is best-effort; the result still counts toward the alert below
        results.append((res, prev))

    def best_effort(what: str, fn, *args):
        try:
            fn(*args)
        except Exception as e:
            record(CheckResult("internal", "error", "warn", detail=f"{what} failed: {e}"))

    status, load_error, unreadable = "no_change", None, 0
    try:
        try:
            items = fetch.fetch(source, inbox_root)
            record(CheckResult("fetch", "pass", "error"))
        except fetch.FetchError as e:
            items, status = [], "fetch_error"
            record(CheckResult("fetch", "error", "error", detail=str(e)))

        user_rules = meta.list_rules(meta_con, source_id)
        for item, data in items:
            label = item or source.location
            sha = fetch.sha256(data)
            if not meta.fingerprint_changed(meta_con, source_id, item, sha):
                continue
            batch_id = uuid.uuid4().hex
            try:
                report = schema.stage(duck_con, data, source.columns, source.key_column)
            except Exception as e:
                record(CheckResult("fetch", "error", "error", detail=f"{label}: {e}"))
                unreadable += 1
                best_effort("Saving the file fingerprint", meta.record_fingerprint, meta_con, source_id, item, sha)
                continue  # an unreadable version is reported once, then skipped until it changes
            try:
                load.ensure_table(duck_con, source)
                load.load(duck_con, source, batch_id, now)
            except Exception as e:
                load_error = e
                record(CheckResult("load", "error", "error", detail=f"{label}: storing the data failed: {e}"))
                continue
            # --- this file is committed; nothing below may fail the run ---
            status = "loaded"
            record(CheckResult("load", "pass", "error"))
            best_effort("Saving the file fingerprint", meta.record_fingerprint, meta_con, source_id, item, sha)
            for row in report.null_key_rows:
                best_effort("Saving a needs-attention row", meta.add_attention, meta_con, source_id, batch_id,
                            "missing key", row)
            best_effort("Updating the sample", lambda: meta.update_sample(meta_con, source_id, schema.preview(data)))
            def per_file(res: CheckResult) -> CheckResult:
                if item:  # several files can load in one run; say which one this is about
                    res.detail = f"{item}: {res.detail}" if res.detail else item
                return res

            record(per_file(rules.schema_result(report)))
            if source.key_column:
                record(per_file(rules.duplicate_keys_result(report)))
            for rule in user_rules:
                record(per_file(_safe_eval(duck_con, table, rule, source.columns, batch_id, now)))

        if status == "no_change" and unreadable:
            status = "fetch_error"
        if status != "loaded" and _table_exists(duck_con, table):
            for rule in user_rules:
                if isinstance(rule, dict) and rule.get("type") == "freshness":
                    record(_safe_eval(duck_con, table, rule, source.columns, None, now))
    except Exception as e:  # anything unexpected (e.g. SQLite locked) after a commit is a result, not a crash
        record(CheckResult("internal", "error", "error", detail=f"The run stopped early: {e}"))

    newly_failing = [r for r, prev in results
                     if r.severity == "error" and r.status in ("fail", "error") and prev in (None, "pass")]
    def clear(rule_id: str, severity: str):
        """Record a pass for a built-in step that errored before, so it doesn't stay red forever."""
        try:
            if meta.last_status(meta_con, source_id, rule_id) not in (None, "pass"):
                record(CheckResult(rule_id, "pass", severity))
        except Exception:
            pass

    if newly_failing and smtp:
        try:
            notify.send_alert(source.name, newly_failing, smtp, send=send)
            clear("email", "warn")
        except Exception as e:
            record(CheckResult("email", "error", "warn", detail=f"Alert email failed: {e}"))
    if not any(r.rule_id == "internal" for r, _ in results):
        clear("internal", "warn")

    final = "load_failed" if load_error else status
    try:
        meta.finish_run(meta_con, run_id, final, repr(load_error) if load_error else "")
    except Exception:
        pass
    if load_error:
        raise load_error
    return final
