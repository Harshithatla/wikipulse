"""Pure DataFrame transforms for the WikiPulse stream job.

Every function takes and returns a DataFrame and works identically on batch and
streaming inputs, so the logic is unit-tested with plain batch DataFrames.

Input contract (what the Kafka source yields):
    key binary, value binary, topic string, partition int, offset long, timestamp timestamp
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, LongType, StringType, StructField, StructType

RECENTCHANGE_SCHEMA = StructType([
    StructField("meta", StructType([
        StructField("id", StringType()),
        StructField("dt", StringType()),
        StructField("domain", StringType()),
        StructField("uri", StringType()),
    ])),
    StructField("id", LongType()),
    StructField("type", StringType()),
    StructField("namespace", IntegerType()),
    StructField("title", StringType()),
    StructField("comment", StringType()),
    StructField("timestamp", LongType()),
    StructField("user", StringType()),
    StructField("bot", BooleanType()),
    StructField("minor", BooleanType()),
    StructField("patrolled", BooleanType()),
    StructField("server_name", StringType()),
    StructField("wiki", StringType()),
    StructField("length", StructType([StructField("old", LongType()), StructField("new", LongType())])),
    StructField("revision", StructType([StructField("old", LongType()), StructField("new", LongType())])),
])

VALID_TYPES = ["edit", "new", "log", "categorize", "external"]
MAX_CLOCK_SKEW_SECONDS = 7 * 24 * 3600  # event time vs broker time
# English + common German/French/Spanish revert phrasings, incl. Twinkle/rollback summaries.
REVERT_REGEX = r"(?i)(\brevert(ed)?\b|\bundid\b|\brv\b|\brvv\b|rückgängig|annulation|revertid)"


def parse(raw: DataFrame) -> DataFrame:
    """Decode the Kafka value and attach a single `dq_reason` (null = valid)."""
    df = raw.select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
    ).withColumn("e", F.from_json("raw_value", RECENTCHANGE_SCHEMA, {"mode": "PERMISSIVE"}))

    e = F.col("e")
    skew = F.abs(e["timestamp"] - F.unix_timestamp("kafka_ts"))
    reason: Column = (
        F.when(e.isNull() | (e["meta"].isNull() & e["wiki"].isNull() & e["timestamp"].isNull()),
               F.lit("unparseable_json"))
        .when(e["meta"]["id"].isNull(), F.lit("missing_event_id"))
        .when(e["wiki"].isNull(), F.lit("missing_wiki"))
        .when(e["title"].isNull(), F.lit("missing_title"))
        .when(e["timestamp"].isNull(), F.lit("missing_timestamp"))
        .when(~e["type"].isin(VALID_TYPES) | e["type"].isNull(), F.lit("unknown_type"))
        .when(skew > MAX_CLOCK_SKEW_SECONDS, F.lit("timestamp_out_of_range"))
    )
    return df.withColumn("dq_reason", reason)


def to_events(parsed: DataFrame) -> DataFrame:
    """Valid rows -> flat, typed bronze event records with lake partition columns."""
    e = F.col("e")
    event_ts = F.timestamp_seconds(e["timestamp"])
    return (
        parsed.where(F.col("dq_reason").isNull())
        .select(
            e["meta"]["id"].alias("event_id"),
            event_ts.alias("event_ts"),
            e["wiki"].alias("wiki"),
            e["server_name"].alias("server_name"),
            e["type"].alias("change_type"),
            e["namespace"].alias("namespace"),
            e["title"].alias("title"),
            e["user"].alias("user_name"),
            F.coalesce(e["bot"], F.lit(False)).alias("is_bot"),
            F.coalesce(e["minor"], F.lit(False)).alias("is_minor"),
            e["patrolled"].alias("is_patrolled"),
            e["comment"].alias("comment"),
            F.coalesce(e["comment"], F.lit("")).rlike(REVERT_REGEX).alias("is_revert"),
            e["id"].alias("rc_id"),
            e["revision"]["old"].alias("rev_old"),
            e["revision"]["new"].alias("rev_new"),
            e["length"]["old"].alias("len_old"),
            e["length"]["new"].alias("len_new"),
            (F.coalesce(e["length"]["new"], F.lit(0)) - F.coalesce(e["length"]["old"], F.lit(0)))
            .alias("bytes_delta"),
            "kafka_partition",
            "kafka_offset",
            "kafka_ts",
            F.current_timestamp().alias("ingest_ts"),
            F.date_format(event_ts, "yyyy-MM-dd").alias("event_date"),
            F.date_format(event_ts, "HH").alias("event_hour"),
        )
    )


def to_dlq(parsed: DataFrame) -> DataFrame:
    """Invalid rows -> Kafka-ready (key, value) with full provenance for replay."""
    bad = parsed.where(F.col("dq_reason").isNotNull())
    return bad.select(
        F.col("dq_reason").alias("key"),
        F.to_json(F.struct(
            "dq_reason", "raw_value", "kafka_topic", "kafka_partition", "kafka_offset",
            F.date_format("kafka_ts", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("kafka_ts"),
        )).alias("value"),
    )


def detect_storms(events: DataFrame, window: str = "10 minutes", slide: str = "1 minute",
                  watermark: str = "5 minutes", min_edits: int = 15, min_editors: int = 5) -> DataFrame:
    """Sliding-window "edit storm" detector on human article edits.

    A storm = many distinct humans editing one article in a short window: the
    signature of breaking news, a death, an edit war, or vandalism waves.
    Bots and non-article namespaces are excluded because they are bursty by design.
    """
    human_article_edits = events.where(
        F.col("change_type").isin("edit", "new") & ~F.col("is_bot") & (F.col("namespace") == 0)
    )
    agg = (
        human_article_edits.withWatermark("event_ts", watermark)
        .groupBy(F.window("event_ts", window, slide).alias("w"), "wiki", "title")
        .agg(
            F.count(F.lit(1)).alias("edits"),
            F.approx_count_distinct("user_name").alias("editors"),
            F.sum(F.col("is_revert").cast("int")).alias("reverts"),
            F.sum("bytes_delta").alias("net_bytes"),
        )
        .where((F.col("edits") >= min_edits) & (F.col("editors") >= min_editors))
    )
    revert_ratio = F.round(F.col("reverts") / F.col("edits"), 3)
    kind = (F.when(revert_ratio >= 0.4, F.lit("edit_war_or_vandalism"))
            .otherwise(F.lit("breaking_news_candidate")))
    return agg.select(
        "wiki", "title",
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "edits", "editors", "reverts", "net_bytes",
        revert_ratio.alias("revert_ratio"),
        kind.alias("kind"),
    )


def collapse_windows(storms: DataFrame) -> DataFrame:
    """One row per page per micro-batch.

    A 10-minute window sliding every minute puts each edit into 10 overlapping
    windows, so a single storm updates ~10 windows per trigger. Alert consumers
    want one message per page: keep the strongest window (most edits, then the
    latest). Batch-only (row_number), so it runs inside foreachBatch.
    """
    from pyspark.sql import Window

    w = Window.partitionBy("wiki", "title").orderBy(F.desc("edits"), F.desc("window_end"))
    return storms.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def storms_to_kafka(storms: DataFrame) -> DataFrame:
    """Key by page so the log-compacted alerts topic keeps the latest state per page."""
    return storms.select(
        F.concat_ws(":", "wiki", "title").alias("key"),
        F.to_json(F.struct(*storms.columns)).alias("value"),
    )
