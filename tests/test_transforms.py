import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "streaming" / "jobs"))
pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from transforms import collapse_windows, detect_storms, parse, storms_to_kafka, to_dlq, to_events  # noqa: E402

NOW = datetime(2026, 11, 12, 5, 30, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def evt(i=0, wiki="enwiki", title="Apache Kafka", user="alice", bot=False, ts=NOW,
        comment="copyedit", etype="edit", ns=0):
    return {
        "meta": {"id": f"id-{i}", "dt": ts.isoformat(), "domain": "en.wikipedia.org"},
        "id": 1000 + i, "type": etype, "namespace": ns, "title": title, "comment": comment,
        "timestamp": int(ts.timestamp()), "user": user, "bot": bot, "minor": False,
        "server_name": "en.wikipedia.org", "wiki": wiki,
        "length": {"old": 100, "new": 150}, "revision": {"old": 1, "new": 2},
    }


def kafka_df(spark, values, kafka_ts=NOW):
    rows = [(v.encode() if isinstance(v, str) else json.dumps(v).encode(),
             "wiki.recentchange.raw", 0, i, kafka_ts.replace(tzinfo=None))
            for i, v in enumerate(values)]
    return spark.createDataFrame(rows, "value binary, topic string, partition int, offset long, timestamp timestamp")


def test_valid_event_flattens_and_partitions(spark):
    out = to_events(parse(kafka_df(spark, [evt(comment="Reverted edits by X")]))).collect()
    assert len(out) == 1
    r = out[0]
    assert r.event_id == "id-0" and r.wiki == "enwiki" and r.user_name == "alice"
    assert r.event_date == "2026-11-12" and r.event_hour == "05"
    assert r.bytes_delta == 50 and r.is_revert is True and r.is_bot is False


@pytest.mark.parametrize("payload,reason", [
    ('{"broken": ', "unparseable_json"),
    ("[1,2,3]", "unparseable_json"),
    ({**evt(), "meta": {"dt": "x"}}, "missing_event_id"),
    ({**evt(), "wiki": None}, "missing_wiki"),
    ({**evt(), "title": None}, "missing_title"),
    ({**evt(), "type": "teleport"}, "unknown_type"),
    ({**evt(), "timestamp": int((NOW - timedelta(days=30)).timestamp())}, "timestamp_out_of_range"),
])
def test_bad_records_go_to_dlq_with_reason(spark, payload, reason):
    parsed = parse(kafka_df(spark, [payload]))
    assert to_events(parsed).count() == 0
    dlq = to_dlq(parsed).collect()
    assert len(dlq) == 1 and dlq[0].key == reason
    body = json.loads(dlq[0].value)
    assert body["kafka_offset"] == 0 and body["raw_value"]


def test_every_record_lands_exactly_once(spark):
    vals = [evt(i) for i in range(20)] + ['nope', {**evt(99), "wiki": None}]
    parsed = parse(kafka_df(spark, vals))
    assert to_events(parsed).count() + to_dlq(parsed).count() == len(vals)


def test_storm_detected_for_many_humans_on_one_page(spark):
    storm = [evt(i, user=f"u{i % 8}", ts=NOW + timedelta(seconds=10 * i)) for i in range(20)]
    noise = [evt(100 + i, title=f"Page {i}", ts=NOW) for i in range(30)]
    bots = [evt(200 + i, title="Bot target", user=f"b{i}", bot=True, ts=NOW) for i in range(40)]
    events = to_events(parse(kafka_df(spark, storm + noise + bots)))
    storms = detect_storms(events, min_edits=15, min_editors=5).collect()
    assert storms, "expected at least one storm window"
    assert {(s.wiki, s.title) for s in storms} == {("enwiki", "Apache Kafka")}
    assert all(s.kind == "breaking_news_candidate" for s in storms)


def test_revert_heavy_storm_classified_as_edit_war(spark):
    vals = [evt(i, user=f"u{i % 6}", comment="Undid revision 123" if i % 2 else "rv vandalism",
                ts=NOW + timedelta(seconds=5 * i)) for i in range(16)]
    storms = detect_storms(to_events(parse(kafka_df(spark, vals))), min_edits=15, min_editors=5).collect()
    assert storms and all(s.kind == "edit_war_or_vandalism" for s in storms)


def test_storm_kafka_key_is_page(spark):
    vals = [evt(i, user=f"u{i}", ts=NOW) for i in range(15)]
    storms = detect_storms(to_events(parse(kafka_df(spark, vals))), min_edits=15, min_editors=5)
    rows = storms_to_kafka(storms).collect()
    assert rows and {r.key for r in rows} == {"enwiki:Apache Kafka"}
    assert json.loads(rows[0].value)["edits"] == 15


def test_collapse_windows_emits_one_alert_per_page(spark):
    storm = [evt(i, user=f"u{i % 8}", ts=NOW + timedelta(seconds=10 * i)) for i in range(20)]
    other = [evt(100 + i, title="Other", user=f"v{i}", ts=NOW + timedelta(seconds=i)) for i in range(16)]
    storms = detect_storms(to_events(parse(kafka_df(spark, storm + other))), min_edits=15, min_editors=5)
    assert storms.count() > 2, "sliding windows should produce overlapping rows"
    rows = collapse_windows(storms).collect()
    assert sorted(r.title for r in rows) == ["Apache Kafka", "Other"]
    kafka = {r.title: r.edits for r in rows}
    assert kafka["Apache Kafka"] == max(s.edits for s in storms.where("title = 'Apache Kafka'").collect())
