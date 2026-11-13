"""Daily late-data reconciliation + housekeeping.

Events can reach the lake after their hour was already loaded (upstream lag,
producer restarts replaying from Last-Event-ID). This DAG re-compacts each hour
of the previous UTC day, compares the row count with what BigQuery holds, and
re-runs load + models + quality gate only for hours that changed. Then it prunes
the local lake buffer (GCS keeps the durable copy).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, dag, get_current_context, task

from wikipulse import pipeline as p
from wikipulse.settings import SETTINGS as S

log = logging.getLogger(__name__)


def _bq():
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    return BigQueryHook(gcp_conn_id=S.gcp_conn_id, use_legacy_sql=False, location=S.bq.location)


def _loaded_rows(client, hour: datetime) -> int:
    sql = f"""
        SELECT IFNULL(SUM(total_rows), 0)
        FROM `{S.bq.project}.{S.bq.raw_dataset}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE table_name = 'recentchange' AND partition_id = '{hour.strftime('%Y%m%d%H')}'
    """
    return int(next(iter(client.query(sql, location=S.bq.location).result()))[0])


@task
def target_day() -> str:
    ctx = get_current_context()
    override = (ctx["params"] or {}).get("day")
    if override:
        return override
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")


@task
def reconcile_day(day: str) -> dict:
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    hook = _bq()
    client = hook.get_client(project_id=S.bq.project, location=S.bq.location)
    gcs = GCSHook(gcp_conn_id=S.gcp_conn_id)
    start = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
    changed, unchanged, empty = [], 0, 0

    for i in range(24):  # in order, so trending baselines see corrected history
        hour = start + timedelta(hours=i)
        res = p.compact_hour(S.bronze_path, hour, S.compacted_path)
        if res.rows == 0:
            empty += 1
            continue
        if res.rows == _loaded_rows(client, hour):
            unchanged += 1
            continue
        gcs.upload(bucket_name=S.bucket, object_name=p.gcs_object_name(hour), filename=res.output_path)
        hook.insert_job(configuration=p.load_job_config(S.bq, S.bucket, hour),
                        project_id=S.bq.project, location=S.bq.location).result()
        for model in [*p.MODELS, "hour_quality"]:
            p.run_sql(client, model, S.bq, hour)
        changed.append(hour.strftime("%H"))

    summary = {"day": day, "reloaded_hours": changed, "unchanged": unchanged, "empty": empty}
    log.info("reconcile summary: %s", summary)
    return summary


@task
def prune_local_lake() -> dict:
    now = datetime.now(timezone.utc)
    return {
        "bronze": p.prune_local(S.bronze_path, "event_date", S.local_retention_days, now),
        "compacted": p.prune_local(S.compacted_path, "dt", S.local_retention_days, now),
    }


@task
def storage_guard() -> None:
    p.run_sql(_bq().get_client(project_id=S.bq.project, location=S.bq.location),
              "storage_guard", S.bq)


@dag(
    dag_id="wikipulse_reconcile",
    schedule="17 3 * * *",
    start_date=S.start_date,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10), "owner": "wikipulse"},
    params={"day": Param("", type="string", description="UTC day to reconcile, e.g. 2026-11-11")},
    tags=["wikipulse", "bigquery", "daily"],
    doc_md=__doc__,
)
def wikipulse_reconcile():
    reconcile_day(target_day()) >> prune_local_lake() >> storage_guard()


wikipulse_reconcile()
