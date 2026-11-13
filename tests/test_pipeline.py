"""Batch-layer tests. The bronze lake is produced by a *real* Spark streaming
file sink, so `_spark_metadata` parsing is tested against genuine Spark output."""
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "airflow" / "include"))
sys.path.insert(0, str(ROOT / "streaming" / "jobs"))

from wikipulse import pipeline as p  # noqa: E402

HOUR = datetime(2026, 11, 12, 5, tzinfo=timezone.utc)


# --------------------------------------------------------------------------- pure helpers
def test_parse_hour_variants():
    for v in ["2026-11-12T05", "2026-11-12T05:00:00Z", "2026-11-12 05:42:10+00:00",
              "2026-11-12T00:42:10-05:00"]:
        assert p.parse_hour(v) == HOUR


def test_load_config_targets_single_partition_idempotently():
    cfg = p.BQConfig(project="proj")
    load = p.load_job_config(cfg, "bkt", HOUR)["load"]
    assert load["destinationTable"]["tableId"] == "recentchange$2026111205"
    assert load["writeDisposition"] == "WRITE_TRUNCATE"
    assert load["createDisposition"] == "CREATE_NEVER"
    assert load["sourceUris"] == ["gs://bkt/bronze/recentchange/dt=2026-11-12/hr=05/events.parquet"]


@pytest.mark.parametrize("name", [*p.MODELS, "hour_quality", "storage_guard"])
def test_every_sql_file_renders(name):
    sql = p.render_sql(name, p.BQConfig(project="proj"))
    assert "{{" not in sql and "proj" in sql
    if name != "storage_guard":
        assert "@hour_ts" in sql


def test_render_sql_rejects_unknown_placeholders(tmp_path, monkeypatch):
    (tmp_path / "x.sql").write_text("SELECT {{ nope }}")
    monkeypatch.setattr(p, "SQL_DIR", tmp_path)
    with pytest.raises(KeyError):
        p.render_sql("x", p.BQConfig(project="proj"))


def test_committed_files_handles_compact_and_delete(tmp_path):
    meta = tmp_path / "_spark_metadata"
    meta.mkdir()
    entry = lambda path, action="add": json.dumps({"path": f"file://{path}", "size": 1,  # noqa: E731
                                                   "isDir": False, "action": action})
    (meta / "0").write_text("v1\n" + entry("/x/stale.parquet"))  # superseded by 9.compact
    (meta / "9.compact").write_text("v1\n" + entry("/x/a.parquet") + "\n" + entry("/x/b.parquet"))
    (meta / "10").write_text("v1\n" + entry("/x/c%20d.parquet") + "\n" + entry("/x/a.parquet", "delete"))
    (meta / ".11.tmp").write_text("garbage")
    assert sorted(p.committed_files(tmp_path)) == ["/x/b.parquet", "/x/c d.parquet"]


def test_prune_local(tmp_path):
    for d in ["event_date=2026-11-01", "event_date=2026-11-10", "event_date=2026-11-12", "other"]:
        (tmp_path / d).mkdir()
    removed = p.prune_local(tmp_path, "event_date", 3, HOUR)
    assert removed == ["event_date=2026-11-01"]
    assert sorted(x.name for x in tmp_path.iterdir()) == ["event_date=2026-11-10", "event_date=2026-11-12", "other"]


# --------------------------------------------------------------------------- real spark sink
pytest.importorskip("pyspark")
pytest.importorskip("duckdb")


def _evt(i, ts):
    return {"meta": {"id": f"id-{i}", "domain": "en.wikipedia.org"}, "id": i, "type": "edit",
            "namespace": 0, "title": f"T{i % 3}", "comment": "", "timestamp": int(ts.timestamp()),
            "user": f"u{i}", "bot": False, "wiki": "enwiki", "length": {"old": 1, "new": 2}}


@pytest.fixture(scope="module")
def bronze(tmp_path_factory):
    from pyspark.sql import SparkSession

    from transforms import parse, to_events

    spark = (SparkSession.builder.master("local[1]").appName("sink-test")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
             .config("spark.ui.enabled", "false").getOrCreate())
    root = tmp_path_factory.mktemp("lake")
    src, sink = root / "src", root / "bronze"
    schema = "value binary, topic string, partition int, offset long, timestamp timestamp"

    def batch(rows, name):
        spark.createDataFrame(rows, schema).write.mode("append").parquet(str(src / name))

    def run():
        q = (to_events(parse(spark.readStream.schema(schema).parquet(str(src) + "/*")))
             .coalesce(1).writeStream.format("parquet")
             .option("path", str(sink)).option("checkpointLocation", str(root / "chk"))
             .partitionBy("event_date", "event_hour").trigger(availableNow=True).start())
        q.awaitTermination()

    kts = (HOUR + timedelta(minutes=59)).replace(tzinfo=None)
    # batch 1: 10 events in 05:xx, 2 in 06:xx
    rows = [(json.dumps(_evt(i, HOUR + timedelta(minutes=i))).encode(), "raw", 0, i, kts) for i in range(10)]
    rows += [(json.dumps(_evt(100 + i, HOUR + timedelta(hours=1, minutes=i))).encode(), "raw", 0, 100 + i, kts)
             for i in range(2)]
    batch(rows, "b1")
    run()
    # batch 2: replays of id-0..id-2 (upstream reconnect) + one late 05:xx event
    replay = [(json.dumps(_evt(i, HOUR + timedelta(minutes=i))).encode(), "raw", 1, 500 + i, kts) for i in range(3)]
    replay += [(json.dumps(_evt(50, HOUR + timedelta(minutes=58))).encode(), "raw", 1, 600, kts)]
    batch(replay, "b2")
    run()
    # an orphan file from a "failed" micro-batch that never committed
    orphan_dir = sink / "event_date=2026-11-12" / "event_hour=05"
    (orphan_dir / "part-99999-orphan.parquet").write_bytes(b"not parquet")
    yield sink, root
    spark.stop()


def test_committed_files_ignores_orphans(bronze):
    sink, _ = bronze
    files = p.committed_files(sink)
    assert files and not any("orphan" in f for f in files)
    assert len(p.files_for_hour(files, HOUR)) == 2  # one per micro-batch thanks to coalesce(1)
    assert len(p.files_for_hour(files, HOUR + timedelta(hours=1))) == 1


def test_hour_completeness(bronze):
    sink, _ = bronze
    grace = timedelta(minutes=10)
    assert not p.hour_is_complete(sink, HOUR, grace, now=HOUR + timedelta(minutes=65))  # in grace
    assert p.hour_is_complete(sink, HOUR, grace, now=HOUR + timedelta(minutes=75))
    nxt = HOUR + timedelta(hours=1)  # nothing committed after 06:xx yet
    assert not p.hour_is_complete(sink, nxt, grace, now=nxt + timedelta(hours=5))


def test_compaction_dedups_and_writes_utc_timestamps(bronze):
    import pyarrow.parquet as pq

    sink, root = bronze
    res = p.compact_hour(sink, HOUR, root / "compacted")
    assert (res.input_files, res.rows, res.duplicates_dropped) == (2, 11, 3)
    table = pq.read_table(res.output_path)
    assert len(set(table.column("event_id").to_pylist())) == 11
    ts_type = table.schema.field("event_ts").type
    assert ts_type.tz is not None, "BigQuery needs UTC-adjusted timestamps to load into TIMESTAMP"
    # first copy kept: replays came from partition 1, originals from partition 0
    rows = {r["event_id"]: r for r in table.to_pylist()}
    assert rows["id-0"]["kafka_partition"] == 0
    assert "event_date" not in table.column_names  # partition cols live in paths only
    # idempotent
    again = p.compact_hour(sink, HOUR, root / "compacted")
    assert again.rows == res.rows


def test_compaction_of_empty_hour(bronze):
    sink, root = bronze
    res = p.compact_hour(sink, HOUR - timedelta(hours=3), root / "compacted")
    assert res.rows == 0 and res.output_path is None



# --------------------------------------------------------------------------- contracts
TF = ROOT / "terraform"


def test_raw_table_schema_matches_compacted_parquet(bronze):
    """The BigQuery raw schema (owned by Terraform) must equal what the pipeline writes."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    sink, root = bronze
    res = p.compact_hour(sink, HOUR, root / "compacted")
    arrow = pq.read_schema(res.output_path)
    bq = json.loads((TF / "schemas" / "recentchange.json").read_text())
    assert [f["name"] for f in bq] == arrow.names

    def bq_type(t):
        if pa.types.is_timestamp(t):
            assert t.tz is not None
            return "TIMESTAMP"
        if pa.types.is_integer(t):
            return "INTEGER"
        if pa.types.is_boolean(t):
            return "BOOLEAN"
        if pa.types.is_string(t) or pa.types.is_large_string(t):
            return "STRING"
        raise AssertionError(f"unmapped arrow type {t}")

    assert {f["name"]: f["type"] for f in bq} == {f.name: bq_type(f.type) for f in arrow}


def test_model_inserts_match_terraform_table_schemas():
    import re

    hcl = (TF / "bigquery.tf").read_text()
    for table in ["page_hourly", "wiki_hourly", "trending_hourly"]:
        block = re.search(rf"\n    {table} = \{{(.*?)\n    \}}", hcl, re.S).group(1)
        tf_cols = re.findall(r'name = "(\w+)"', block)
        sql = (p.SQL_DIR / f"{table}.sql").read_text()
        ins = re.search(rf"INSERT INTO \{{\{{ analytics \}}\}}\.{table}\s*\((.*?)\)", sql, re.S).group(1)
        sql_cols = [c.strip() for c in ins.split(",")]
        assert sql_cols == tf_cols, table
