"""Hourly batch layer: bronze lake hour -> GCS -> BigQuery raw -> models -> DQ gate.

    resolve_hour -> wait_for_hour -> compact -> upload_to_gcs -> load_raw
        -> page_hourly -> wiki_hourly -> trending_hourly -> quality_gate -> storage_guard

Trigger manually with {"hour": "2026-11-12T05"} to (re)process any hour; every
step is idempotent so re-runs are always safe.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task

from wikipulse import pipeline as p
from wikipulse.settings import SETTINGS as S


def _bq():
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    return BigQueryHook(gcp_conn_id=S.gcp_conn_id, use_legacy_sql=False, location=S.bq.location)


@task
def resolve_hour() -> str:
    ctx = get_current_context()
    override = (ctx["params"] or {}).get("hour")
    if override:
        hour = p.parse_hour(override)
    elif ctx["dag_run"].run_type == "scheduled" and ctx.get("data_interval_start"):
        hour = p.floor_hour(ctx["data_interval_start"])
    else:  # manual run without an explicit hour: the last fully elapsed hour
        hour = p.floor_hour(datetime.now(timezone.utc)) - timedelta(hours=1)
    return hour.isoformat()


@task.sensor(poke_interval=300, timeout=3 * 3600, mode="reschedule")
def wait_for_hour(hour: str) -> bool:
    """Ready once the stream has moved past the hour plus a grace period."""
    return p.hour_is_complete(S.bronze_path, p.parse_hour(hour),
                              grace=timedelta(minutes=S.grace_minutes),
                              now=datetime.now(timezone.utc))


@task
def compact(hour: str) -> dict:
    res = p.compact_hour(S.bronze_path, p.parse_hour(hour), S.compacted_path)
    if res.rows == 0:
        # A 24/7 stream with an empty hour is an outage, not a quiet hour.
        raise AirflowFailException(f"no committed bronze data for {hour}")
    return res.__dict__


@task
def upload_to_gcs(compacted: dict, hour: str) -> str:
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    obj = p.gcs_object_name(p.parse_hour(hour))
    GCSHook(gcp_conn_id=S.gcp_conn_id).upload(bucket_name=S.bucket, object_name=obj,
                                              filename=compacted["output_path"])
    return f"gs://{S.bucket}/{obj}"


@task
def load_raw(_uri: str, hour: str) -> int:
    h = p.parse_hour(hour)
    hook = _bq()
    job = hook.insert_job(configuration=p.load_job_config(S.bq, S.bucket, h),
                          project_id=S.bq.project, location=S.bq.location)
    job.result()
    return int(job.output_rows or 0)


@task
def run_model(name: str, hour: str) -> None:
    p.run_sql(_bq().get_client(project_id=S.bq.project, location=S.bq.location),
              name, S.bq, p.parse_hour(hour))


@task
def storage_guard() -> None:
    p.run_sql(_bq().get_client(project_id=S.bq.project, location=S.bq.location),
              "storage_guard", S.bq)


@dag(
    dag_id="wikipulse_hourly",
    schedule="@hourly",
    start_date=S.start_date,
    catchup=False,
    max_active_runs=2,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5), "owner": "wikipulse"},
    params={"hour": Param("", type="string",
                          description="Optional event hour to (re)process, e.g. 2026-11-12T05")},
    tags=["wikipulse", "bigquery", "hourly"],
    doc_md=__doc__,
)
def wikipulse_hourly():
    hour = resolve_hour()
    files = compact(hour)
    wait_for_hour(hour) >> files
    loaded = load_raw(upload_to_gcs(files, hour), hour)

    prev = loaded
    for name in p.MODELS:  # page_hourly -> wiki_hourly -> trending_hourly
        step = run_model.override(task_id=name)(name, hour)
        prev >> step
        prev = step
    gate = run_model.override(task_id="quality_gate", retries=0)("hour_quality", hour)
    prev >> gate >> storage_guard()


wikipulse_hourly()
