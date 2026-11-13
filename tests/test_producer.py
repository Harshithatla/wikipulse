import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "producer"))

from producer import OffsetStore, Synthetic, parse_allowlist, route  # noqa: E402
from sse import parse_sse  # noqa: E402


def test_sse_multiline_ids_comments_and_trailing_event():
    stream = [
        ": keep-alive",
        "event: message",
        'id: [{"topic":"x","partition":0,"offset":1}]',
        'data: {"a":',
        "data: 1}",
        "",
        "data:no-space",
        "",
        "retry: 5000",
        "data: tail",
    ]
    evs = list(parse_sse(stream))
    assert [e.data for e in evs] == ['{"a":\n1}', "no-space", "tail"]
    assert evs[0].id == '[{"topic":"x","partition":0,"offset":1}]'
    assert evs[1].id == evs[0].id  # id persists until changed
    assert evs[0].event == "message"


def test_sse_blank_lines_without_data_do_not_emit():
    assert list(parse_sse(["", "", "\r", ": hi", ""])) == []


def test_route_filters_canary_and_allowlist_and_keys_by_page():
    allow = parse_allowlist(" enwiki, dewiki ,")
    assert allow == {"enwiki", "dewiki"}
    ok = json.dumps({"wiki": "enwiki", "title": "Kafka", "meta": {"domain": "en.wikipedia.org"}})
    assert route(ok, allow) == (True, "enwiki:Kafka")
    assert route(json.dumps({"wiki": "frwiki", "title": "x", "meta": {}}), allow) == (False, None)
    assert route(json.dumps({"wiki": "enwiki", "meta": {"domain": "canary"}}), allow) == (False, None)
    assert route(ok, frozenset()) == (True, "enwiki:Kafka")  # empty allowlist = everything


def test_route_forwards_garbage_for_the_dlq():
    assert route('{"broken": ', frozenset()) == (True, None)
    assert route("[1,2]", frozenset({"enwiki"})) == (True, None)


def test_offset_store_roundtrip(tmp_path):
    store = OffsetStore(str(tmp_path / "s" / "last_id"))
    assert store.load() is None
    store.save('[{"offset": 5}]')
    assert store.load() == '[{"offset": 5}]'
    assert OffsetStore(None).load() is None


def test_synthetic_is_deterministic_and_produces_storms():
    a, b = Synthetic(seed=1, storm_every=200), Synthetic(seed=1, storm_every=200)
    seq_a = [a.next() for _ in range(3000)]
    seq_b = [b.next() for _ in range(3000)]
    strip = lambda s: {k: v for k, v in json.loads(s).items() if k not in ("timestamp", "meta")}  # noqa: E731
    valid = [s for s in seq_a if not s.startswith('{"broken')]
    assert [strip(x) for x in valid[:50]] == [strip(x) for x in [s for s in seq_b if not s.startswith('{"broken')][:50]]
    titles = {}
    for s in valid:
        e = json.loads(s)
        titles[(e["wiki"], e["title"])] = titles.get((e["wiki"], e["title"]), 0) + 1
    assert max(titles.values()) >= 30, "expected at least one injected storm"
    assert len(valid) < len(seq_a), "expected a few malformed records to exercise the DLQ"


def test_synthetic_events_satisfy_the_stream_contract():
    """Cross-component contract: producer output must parse cleanly in the Spark job."""
    import pytest
    pytest.importorskip("pyspark")
    sys.path.insert(0, str(ROOT / "streaming" / "jobs"))
    from datetime import datetime, timezone

    from pyspark.sql import SparkSession

    from transforms import parse

    spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    gen = Synthetic(seed=3)
    vals = [gen.next() for _ in range(500)]
    rows = [(v.encode(), "raw", 0, i, datetime.now(timezone.utc).replace(tzinfo=None)) for i, v in enumerate(vals)]
    df = spark.createDataFrame(rows, "value binary, topic string, partition int, offset long, timestamp timestamp")
    reasons = {r.dq_reason for r in parse(df).select("dq_reason").distinct().collect()}
    assert reasons <= {None, "unparseable_json"}
