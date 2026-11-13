"""Hourly batch layer for WikiPulse, independent of Airflow so it is unit-testable.

Flow for one event-hour H:
  committed bronze files (Spark _spark_metadata log)
    -> DuckDB compaction + de-dup  (many small files -> one zstd Parquet)
    -> GCS  gs://<bucket>/bronze/recentchange/dt=YYYY-MM-DD/hr=HH/events.parquet
    -> BigQuery load into raw.recentchange$YYYYMMDDHH  (WRITE_TRUNCATE = idempotent)
    -> SQL models (delete+insert per hour) -> data-quality ASSERTs -> storage guard

Re-running any hour any number of times converges to the same state, which is
what makes late-data reconciliation and backfills safe.
"""
from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

log = logging.getLogger(__name__)

SQL_DIR = Path(__file__).parent / "sql"
_LOG_FILE = re.compile(r"^(\d+)(\.compact)?$")


# --------------------------------------------------------------------------- hours
def floor_hour(ts: datetime) -> datetime:
    ts = ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return ts.replace(minute=0, second=0, microsecond=0)


def parse_hour(value: str) -> datetime:
    """Accepts '2026-11-12T05', '2026-11-12T05:00:00Z', '2026-11-12 05:00:00+00:00'."""
    v = value.strip().replace(" ", "T").replace("Z", "+00:00")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}", v):
        v += ":00:00"
    return floor_hour(datetime.fromisoformat(v))


def hour_parts(hour: datetime) -> tuple[str, str]:
    return hour.strftime("%Y-%m-%d"), hour.strftime("%H")


# --------------------------------------------------------------------------- spark sink log
def _local_path(uri: str) -> str:
    p = urlparse(uri)
    return unquote(p.path) if p.scheme in ("file", "") else uri


def committed_files(sink_root: str | Path) -> list[str]:
    """Files committed by a Spark file sink, read from its `_spark_metadata` log.

    Globbing the directory is wrong: it also returns files from micro-batches that
    failed mid-write. The metadata log is the sink's source of truth. Spark
    periodically writes `N.compact` (full state) and deletes older logs, so we
    read the newest compact file plus every delta after it.
    """
    meta = Path(sink_root) / "_spark_metadata"
    if not meta.is_dir():
        return []
    batches: dict[int, Path] = {}
    latest_compact = -1
    for f in meta.iterdir():
        m = _LOG_FILE.match(f.name)
        if not m:
            continue  # ignore .tmp / crc / in-flight files
        bid = int(m.group(1))
        batches[bid] = f
        if m.group(2):
            latest_compact = max(latest_compact, bid)

    files: dict[str, None] = {}
    for bid in sorted(b for b in batches if b >= latest_compact):
        lines = batches[bid].read_text().splitlines()
        if not lines or not lines[0].startswith("v"):
            raise ValueError(f"unexpected sink log format in {batches[bid]}")
        for line in lines[1:]:
            if not line.strip():
                continue
            entry = json.loads(line)
            path = _local_path(entry["path"])
            if entry.get("action", "add") == "delete":
                files.pop(path, None)
            elif not entry.get("isDir", False):
                files[path] = None
    return list(files)


def files_for_hour(files: Iterable[str], hour: datetime) -> list[str]:
    d, h = hour_parts(hour)
    needle = f"/event_date={d}/event_hour={h}/"
    return sorted(f for f in files if needle in f)


def hour_is_complete(sink_root: str | Path, hour: datetime, grace: timedelta, now: datetime) -> bool:
    """An hour is ready once the stream has committed data for a *later* hour
    and the grace period for stragglers has elapsed. Late events after that are
    picked up by the daily reconcile run."""
    if now < hour + timedelta(hours=1) + grace:
        return False
    later = hour + timedelta(hours=1)
    files = committed_files(sink_root)
    for f in files:
        m = re.search(r"/event_date=(\d{4}-\d{2}-\d{2})/event_hour=(\d{2})/", f)
        if m and datetime.strptime(f"{m[1]}T{m[2]}", "%Y-%m-%dT%H").replace(tzinfo=timezone.utc) >= later:
            return True
    return False


# --------------------------------------------------------------------------- compaction
@dataclass(frozen=True)
class CompactionResult:
    rows: int
    duplicates_dropped: int
    input_files: int
    output_path: str | None


def compact_hour(sink_root: str | Path, hour: datetime, out_dir: str | Path) -> CompactionResult:
    import duckdb

    inputs = files_for_hour(committed_files(sink_root), hour)
    if not inputs:
        return CompactionResult(0, 0, 0, None)
    d, h = hour_parts(hour)
    out = Path(out_dir) / f"dt={d}" / f"hr={h}" / "events.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    file_list = "[" + ",".join("'" + f.replace("'", "''") + "'" for f in inputs) + "]"
    # Partition columns live in the directory names only. DuckDB would auto-detect them
    # from the paths and add columns the BigQuery table doesn't have, so disable that.
    con.execute(f"""
        CREATE TEMP VIEW src AS
        SELECT * REPLACE (event_ts::TIMESTAMPTZ AS event_ts,
                          kafka_ts::TIMESTAMPTZ AS kafka_ts,
                          ingest_ts::TIMESTAMPTZ AS ingest_ts)
        FROM read_parquet({file_list}, union_by_name=true, hive_partitioning=false)
    """)
    total = con.execute("SELECT count(*) FROM src").fetchone()[0]
    # Upstream reconnects can replay events; keep the first copy seen by Kafka.
    tmp = out.with_suffix(".parquet.tmp")
    con.execute(f"""
        COPY (
            SELECT * FROM src
            QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY kafka_ts, kafka_partition, kafka_offset) = 1
            ORDER BY wiki, event_ts
        ) TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{tmp}')").fetchone()[0]
    con.close()
    tmp.replace(out)
    log.info("compacted %s: %d files, %d rows (%d dupes dropped)", hour, len(inputs), rows, total - rows)
    return CompactionResult(rows, total - rows, len(inputs), str(out))


def prune_local(root: str | Path, prefix: str, keep_days: int, now: datetime) -> list[str]:
    """Delete local date partitions (`<prefix>=YYYY-MM-DD` dirs) older than keep_days.
    GCS holds the durable copy; the laptop disk only needs a short buffer."""
    import shutil

    cutoff = (floor_hour(now) - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    removed = []
    base = Path(root)
    if not base.is_dir():
        return removed
    for d in base.iterdir():
        if d.is_dir() and d.name.startswith(prefix + "=") and d.name.split("=", 1)[1] < cutoff:
            shutil.rmtree(d)
            removed.append(d.name)
    return sorted(removed)


def gcs_object_name(hour: datetime) -> str:
    d, h = hour_parts(hour)
    return f"bronze/recentchange/dt={d}/hr={h}/events.parquet"


# --------------------------------------------------------------------------- bigquery
@dataclass(frozen=True)
class BQConfig:
    project: str
    raw_dataset: str = "wikipulse_raw"
    analytics_dataset: str = "wikipulse_analytics"
    location: str = "us-central1"
    storage_guard_gib: float = 8.0  # free tier is 10 GiB; fail well before it


def load_job_config(cfg: BQConfig, bucket: str, hour: datetime) -> dict:
    """REST-shaped load config. The $YYYYMMDDHH decorator + WRITE_TRUNCATE replaces
    exactly one hourly partition, so loads are idempotent. Load jobs are free."""
    return {
        "load": {
            "sourceUris": [f"gs://{bucket}/{gcs_object_name(hour)}"],
            "destinationTable": {
                "projectId": cfg.project,
                "datasetId": cfg.raw_dataset,
                "tableId": f"recentchange${hour.strftime('%Y%m%d%H')}",
            },
            "sourceFormat": "PARQUET",
            "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_NEVER",  # schema is owned by Terraform
            "parquetOptions": {"enableListInference": True},
        }
    }


def render_sql(name: str, cfg: BQConfig) -> str:
    """Identifiers are substituted; values (the hour) are passed as query parameters."""
    sql = (SQL_DIR / f"{name}.sql").read_text()
    subs = {
        "project": cfg.project,
        "raw": f"`{cfg.project}.{cfg.raw_dataset}`",
        "analytics": f"`{cfg.project}.{cfg.analytics_dataset}`",
        "region": f"`region-{cfg.location}`",
        "storage_guard_bytes": str(int(cfg.storage_guard_gib * 1024**3)),
    }
    for k, v in subs.items():
        sql = sql.replace("{{ " + k + " }}", v)
    leftover = re.search(r"\{\{.*?\}\}", sql)
    if leftover:
        raise KeyError(f"unrendered placeholder {leftover.group(0)} in {name}.sql")
    return sql


def run_sql(client, name: str, cfg: BQConfig, hour: datetime | None = None):
    from google.cloud import bigquery

    params = [bigquery.ScalarQueryParameter("hour_ts", "TIMESTAMP", hour)] if hour else []
    job = client.query(render_sql(name, cfg), location=cfg.location,
                       job_config=bigquery.QueryJobConfig(query_parameters=params))
    job.result()
    log.info("%s ok (%s bytes billed)", name, getattr(job, "total_bytes_billed", "?"))
    return job


MODELS = ["page_hourly", "wiki_hourly", "trending_hourly"]  # dependency order
