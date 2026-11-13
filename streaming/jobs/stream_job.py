"""WikiPulse Spark Structured Streaming job.

Kafka(raw) ──parse/validate──┬─► bronze Parquet lake (exactly-once file sink, partitioned by event hour)
                             ├─► Kafka DLQ topic (bad records + reason + provenance)
                             └─► sliding-window storm detector ─► Kafka alerts topic (log-compacted)

Three independent queries share one source definition; each has its own
checkpoint so they can be restarted / reset independently.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

from transforms import collapse_windows, detect_storms, parse, storms_to_kafka, to_dlq, to_events


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def main() -> None:
    bootstrap = env("KAFKA_BOOTSTRAP", "kafka:9092")
    lake = env("LAKE_PATH", "/lake").rstrip("/")
    trigger = f"{env('TRIGGER_SECONDS', '60')} seconds"

    spark = (
        SparkSession.builder.appName("wikipulse-stream")
        .config("spark.sql.shuffle.partitions", env("SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.session.timeZone", "UTC")
        # Write real UTC-adjusted timestamps (not legacy INT96) so BigQuery loads them as TIMESTAMP.
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        # RocksDB keeps streaming state off-heap: stable memory on a laptop-sized box.
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", env("RAW_TOPIC", "wiki.recentchange.raw"))
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", env("MAX_OFFSETS_PER_TRIGGER", "200000"))
        .option("failOnDataLoss", "false")  # topic retention may expire unread data on a laptop
        .load()
    )
    parsed = parse(raw)
    events = to_events(parsed)

    # 1) Bronze lake. coalesce(1) => one file per hour-partition per micro-batch,
    #    which keeps the small-files problem bounded before hourly compaction.
    (events.coalesce(1).writeStream.format("parquet")
     .option("path", f"{lake}/bronze/recentchange")
     .option("checkpointLocation", f"{lake}/_checkpoints/events")
     .option("compression", "zstd")
     # TTL for entries in the sink's metadata log, so it doesn't grow forever on a 24/7 job.
     .option("retention", env("SINK_LOG_RETENTION", "4d"))
     .partitionBy("event_date", "event_hour")
     .outputMode("append")
     .trigger(processingTime=trigger)
     .queryName("bronze_events")
     .start())

    # 2) Dead-letter queue.
    (to_dlq(parsed).writeStream.format("kafka")
     .option("kafka.bootstrap.servers", bootstrap)
     .option("topic", env("DLQ_TOPIC", "wiki.recentchange.dlq"))
     .option("checkpointLocation", f"{lake}/_checkpoints/dlq")
     .trigger(processingTime=trigger)
     .queryName("dlq")
     .start())

    # 3) Real-time edit-storm alerts. Update mode => an alert fires on the first
    #    trigger a window crosses the threshold, not only after the window closes.
    #    foreachBatch collapses the ~10 overlapping sliding windows to one alert per
    #    page per trigger. (foreachBatch is at-least-once; harmless here because the
    #    alerts topic is log-compacted by page key, so a replay just overwrites.)
    storms = detect_storms(
        events,
        min_edits=int(env("STORM_MIN_EDITS", "15")),
        min_editors=int(env("STORM_MIN_EDITORS", "5")),
    )
    alerts_topic = env("ALERTS_TOPIC", "wiki.alerts")

    def publish_alerts(batch, _batch_id: int) -> None:
        (storms_to_kafka(collapse_windows(batch)).write.format("kafka")
         .option("kafka.bootstrap.servers", bootstrap)
         .option("topic", alerts_topic)
         .save())

    (storms.writeStream.foreachBatch(publish_alerts)
     # new checkpoint: the sink type changed from kafka to foreachBatch
     .option("checkpointLocation", f"{lake}/_checkpoints/storms_v2")
     .outputMode("update")
     .trigger(processingTime="30 seconds")
     .queryName("storm_alerts")
     .start())

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
