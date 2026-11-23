from datetime import datetime, timedelta, timezone

import duckdb

from dqwatch import meta
from dqwatch.models import Column, Source
from dqwatch.orchestration import defs, due_run_requests, ingest_and_check

NOW = datetime(2026, 1, 10, 12, 3, tzinfo=timezone.utc)
SRC = Source(7, "s", "url", "http://x", 15, "append", None, [])


def test_run_keys_stable_within_slot():
    a = due_run_requests([SRC], NOW)
    b = due_run_requests([SRC], NOW + timedelta(seconds=10))
    assert [r.run_key for r in a] == [r.run_key for r in b]
    assert a[0].run_config == {"ops": {"ingest_and_check_op": {"config": {"source_id": 7}}}}


def test_run_keys_change_next_slot():
    a = due_run_requests([SRC], NOW)
    b = due_run_requests([SRC], NOW + timedelta(minutes=15))
    assert a[0].run_key != b[0].run_key


def test_job_executes(tmp_path, monkeypatch):
    inbox = tmp_path / "inbox"
    (inbox / "orders").mkdir(parents=True)
    (inbox / "orders" / "a.csv").write_bytes(b"id,v\n1,a\n2,b\n")
    mp, dp = str(tmp_path / "m.sqlite"), str(tmp_path / "d.duckdb")
    sid = meta.create_source(meta.connect(mp), Source(None, "o", "folder", "orders", 1, "upsert", "id",
                                                      [Column("id", "integer"), Column("v", "text")]))
    monkeypatch.setenv("DQ_META", mp)
    monkeypatch.setenv("DQ_DATA", dp)
    monkeypatch.setenv("DQ_INBOX", str(inbox))
    monkeypatch.delenv("DQ_SMTP_HOST", raising=False)
    result = ingest_and_check.execute_in_process(
        run_config={"ops": {"ingest_and_check_op": {"config": {"source_id": sid}}}})
    assert result.success
    assert duckdb.connect(dp).execute(f"SELECT count(*) FROM src_{sid}").fetchone() == (2,)


def test_definitions_load():
    assert defs.get_job_def("ingest_and_check") is not None
    assert defs.get_sensor_def("due_sources") is not None


def test_dagster_yaml_serializes_runs(tmp_path, monkeypatch):
    import shutil
    from pathlib import Path

    from dagster import DagsterInstance
    shutil.copy(Path(__file__).parent.parent / "dagster.yaml", tmp_path / "dagster.yaml")
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path))
    with DagsterInstance.get() as inst:
        assert inst.run_coordinator.get_run_queue_config().max_concurrent_runs == 1
