"""DAG integrity: the DAGs import cleanly on real Airflow and have the intended shape."""
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
pytest.importorskip("airflow")
sys.path.insert(0, str(ROOT / "airflow" / "include"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")


@pytest.fixture(scope="module")
def dagbag():
    try:
        from airflow.dag_processing.dagbag import DagBag
    except ImportError:  # Airflow < 3.2
        from airflow.models.dagbag import DagBag
    return DagBag(dag_folder=str(ROOT / "airflow" / "dags"), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}
    assert set(dagbag.dag_ids) == {"wikipulse_hourly", "wikipulse_reconcile"}


def _downstream(dag, task_id):
    return set(dag.get_task(task_id).downstream_task_ids)


def test_hourly_dag_shape(dagbag):
    dag = dagbag.get_dag("wikipulse_hourly")
    assert dag.catchup is False and dag.max_active_runs == 2
    chain = ["wait_for_hour", "compact", "upload_to_gcs", "load_raw", "page_hourly",
             "wiki_hourly", "trending_hourly", "quality_gate", "storage_guard"]
    for a, b in zip(chain, chain[1:], strict=False):
        assert b in _downstream(dag, a), f"{a} -> {b} missing"
    assert dag.get_task("quality_gate").retries == 0  # data problems should not be retried blindly
    assert dag.get_task("wait_for_hour").mode == "reschedule"  # don't hold a worker slot


def test_reconcile_dag_shape(dagbag):
    dag = dagbag.get_dag("wikipulse_reconcile")
    assert "prune_local_lake" in _downstream(dag, "reconcile_day")
    assert "storage_guard" in _downstream(dag, "prune_local_lake")


def test_run_sql_passes_hour_as_query_parameter():
    from datetime import datetime, timezone

    from wikipulse import pipeline as p
    HOUR = datetime(2026, 11, 12, 5, tzinfo=timezone.utc)

    class FakeJob:
        total_bytes_billed = 0
        def result(self):
            return []

    class FakeClient:
        def query(self, sql, location, job_config):
            self.sql, self.location, self.cfg = sql, location, job_config
            return FakeJob()

    c = FakeClient()
    p.run_sql(c, "page_hourly", p.BQConfig(project="proj"), HOUR)
    (param,) = c.cfg.query_parameters
    assert (param.name, param.type_, param.value) == ("hour_ts", "TIMESTAMP", HOUR)
    assert c.location == "us-central1" and "`proj.wikipulse_analytics`.page_hourly" in c.sql
