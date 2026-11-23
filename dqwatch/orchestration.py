"""Dagster: one generic job, launched per source by a sensor that reads the sources from SQLite."""
import os
from datetime import datetime, timezone

import duckdb
from dagster import (DefaultSensorStatus, Definitions, OpExecutionContext, RunRequest, SensorEvaluationContext,
                     job, op, sensor)

from dqwatch import meta, pipeline
from dqwatch.models import Source
from dqwatch.notify import smtp_from_env


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@op(config_schema={"source_id": int})
def ingest_and_check_op(context: OpExecutionContext) -> str:
    source_id = context.op_config["source_id"]
    meta_con = meta.connect(_env("DQ_META", "data/meta.sqlite"))
    duck = duckdb.connect(_env("DQ_DATA", "data/data.duckdb"))
    try:
        status = pipeline.run_source(meta_con, duck, source_id, _env("DQ_INBOX", "data/inbox"),
                                     datetime.now(timezone.utc), smtp=smtp_from_env())
    finally:
        duck.close()
        meta_con.close()
    context.log.info(f"source {source_id}: {status}")
    return status


@job
def ingest_and_check():
    ingest_and_check_op()


def due_run_requests(sources: list[Source], now: datetime) -> list[RunRequest]:
    """One request per source per schedule slot; Dagster skips run keys it has already seen."""
    out = []
    for s in sources:
        slot = int(now.timestamp() // (max(1, s.schedule_minutes) * 60))
        out.append(RunRequest(run_key=f"{s.id}:{slot}",
                              run_config={"ops": {"ingest_and_check_op": {"config": {"source_id": s.id}}}}))
    return out


@sensor(job=ingest_and_check, minimum_interval_seconds=30, default_status=DefaultSensorStatus.RUNNING)
def due_sources(context: SensorEvaluationContext):
    con = meta.connect(_env("DQ_META", "data/meta.sqlite"))
    try:
        return due_run_requests(meta.list_sources(con), datetime.now(timezone.utc))
    finally:
        con.close()


defs = Definitions(jobs=[ingest_and_check], sensors=[due_sources])
