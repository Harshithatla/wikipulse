"""Static checks on the Terraform (CI additionally runs `terraform validate`)."""
from pathlib import Path

import pytest

hcl2 = pytest.importorskip("hcl2")
TF = Path(__file__).resolve().parents[1] / "terraform"


def _norm(x):
    """python-hcl2 >= 5 keeps HCL quotes on keys and string values; strip them."""
    if isinstance(x, dict):
        return {_norm(k): _norm(v) for k, v in x.items() if k != "__is_block__"}
    if isinstance(x, list):
        return [_norm(v) for v in x]
    if isinstance(x, str) and len(x) >= 2 and x[0] == x[-1] == '"':
        return x[1:-1]
    return x


@pytest.fixture(scope="module")
def tf():
    merged = {}
    for f in sorted(TF.glob("*.tf")):
        with f.open() as fh:
            for k, v in _norm(hcl2.load(fh)).items():
                merged.setdefault(k, []).extend(v)
    return merged


def _resources(tf, rtype):
    return {name: body for r in tf["resource"] for t, inst in r.items() if t == rtype
            for name, body in inst.items()}


def test_parses_and_declares_variables(tf):
    names = {k for v in tf["variable"] for k in v}
    assert {"project_id", "region", "billing_account_id"} <= names


def test_bucket_is_free_tier_safe(tf):
    b = _resources(tf, "google_storage_bucket")["lake"]
    assert b["public_access_prevention"] == "enforced"
    assert b["soft_delete_policy"][0]["retention_duration_seconds"] == 0
    assert b["lifecycle_rule"][0]["action"][0]["type"] == "Delete"


def test_raw_table_guards_cost(tf):
    t = _resources(tf, "google_bigquery_table")["recentchange"]
    assert t["require_partition_filter"] is True
    tp = t["time_partitioning"][0]
    assert tp["type"] == "HOUR" and tp["field"] == "event_ts" and "expiration_ms" in tp


def test_no_service_account_keys_in_state(tf):
    assert not _resources(tf, "google_service_account_key")
