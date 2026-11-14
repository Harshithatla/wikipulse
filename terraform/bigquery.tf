resource "google_bigquery_dataset" "raw" {
  dataset_id    = "wikipulse_raw"
  friendly_name = "WikiPulse raw"
  description   = "Hour-partitioned recentchange events loaded from the GCS lake."
  location      = var.region
  labels        = local.labels
  depends_on    = [google_project_service.apis]
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id    = "wikipulse_analytics"
  friendly_name = "WikiPulse analytics"
  description   = "Hourly models, trending pages and dashboard views."
  location      = var.region
  labels        = local.labels
  depends_on    = [google_project_service.apis]
}

resource "google_bigquery_table" "recentchange" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "recentchange"
  description         = "One row per recentchange event, de-duplicated on event_id per hour."
  schema              = file("${path.module}/schemas/recentchange.json")
  clustering          = ["wiki", "change_type", "namespace"]
  deletion_protection = var.deletion_protection
  labels              = local.labels

  time_partitioning {
    type          = "HOUR"
    field         = "event_ts"
    expiration_ms = var.raw_partition_expiration_days * 24 * 3600 * 1000
  }
  require_partition_filter = true # a forgotten WHERE can't scan the whole table
}

locals {
  analytics_tables = {
    page_hourly = {
      description = "Per-article activity per hour (namespace 0, edit/new)."
      clustering  = ["wiki", "title"]

      schema = [
        { name = "hour_ts", type = "TIMESTAMP", mode = "REQUIRED" },
        { name = "wiki", type = "STRING", mode = "REQUIRED" },
        { name = "title", type = "STRING", mode = "REQUIRED" },
        { name = "edits", type = "INT64", mode = "NULLABLE" },
        { name = "human_edits", type = "INT64", mode = "NULLABLE" },
        { name = "bot_edits", type = "INT64", mode = "NULLABLE" },
        { name = "editors", type = "INT64", mode = "NULLABLE" },
        { name = "human_editors", type = "INT64", mode = "NULLABLE" },
        { name = "reverts", type = "INT64", mode = "NULLABLE" },
        { name = "new_pages", type = "INT64", mode = "NULLABLE" },
        { name = "net_bytes", type = "INT64", mode = "NULLABLE" },
      ]
    }
    wiki_hourly = {
      description = "Per-wiki health metrics per hour."
      clustering  = ["wiki"]

      schema = [
        { name = "hour_ts", type = "TIMESTAMP", mode = "REQUIRED" },
        { name = "wiki", type = "STRING", mode = "REQUIRED" },
        { name = "events", type = "INT64", mode = "NULLABLE" },
        { name = "edits", type = "INT64", mode = "NULLABLE" },
        { name = "new_pages", type = "INT64", mode = "NULLABLE" },
        { name = "log_events", type = "INT64", mode = "NULLABLE" },
        { name = "bot_share", type = "FLOAT64", mode = "NULLABLE" },
        { name = "unique_editors", type = "INT64", mode = "NULLABLE" },
        { name = "anon_like_editors", type = "INT64", mode = "NULLABLE" },
        { name = "reverts", type = "INT64", mode = "NULLABLE" },
        { name = "revert_rate", type = "FLOAT64", mode = "NULLABLE" },
        { name = "p50_kafka_lag_s", type = "INT64", mode = "NULLABLE" },
        { name = "p95_kafka_lag_s", type = "INT64", mode = "NULLABLE" },
      ]
    }
    trending_hourly = {
      description = "Top-100 anomalous articles per hour vs their 7-day baseline."
      clustering  = ["wiki"]

      schema = [
        { name = "hour_ts", type = "TIMESTAMP", mode = "REQUIRED" },
        { name = "rank", type = "INT64", mode = "REQUIRED" },
        { name = "wiki", type = "STRING", mode = "REQUIRED" },
        { name = "title", type = "STRING", mode = "REQUIRED" },
        { name = "human_edits", type = "INT64", mode = "NULLABLE" },
        { name = "human_editors", type = "INT64", mode = "NULLABLE" },
        { name = "reverts", type = "INT64", mode = "NULLABLE" },
        { name = "baseline_mean", type = "FLOAT64", mode = "NULLABLE" },
        { name = "baseline_stddev", type = "FLOAT64", mode = "NULLABLE" },
        { name = "spike_score", type = "FLOAT64", mode = "NULLABLE" },
        { name = "active_hours_7d", type = "INT64", mode = "NULLABLE" },
      ]
    }
  }
}

resource "google_bigquery_table" "analytics" {
  for_each            = local.analytics_tables
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = each.key
  description         = each.value.description
  schema              = jsonencode(each.value.schema)
  clustering          = each.value.clustering
  deletion_protection = var.deletion_protection
  labels              = local.labels

  time_partitioning {
    type          = "DAY"
    field         = "hour_ts"
    expiration_ms = var.analytics_partition_expiration_days * 24 * 3600 * 1000
  }
}

# Dashboard-ready view (connect Looker Studio to this; it's free).
resource "google_bigquery_table" "v_trending_latest" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "v_trending_latest"
  deletion_protection = false
  view {
    use_legacy_sql = false
    query          = <<-SQL
      SELECT
        t.hour_ts, t.rank, t.wiki, t.title, t.human_edits, t.human_editors, t.reverts,
        t.baseline_mean, t.spike_score,
        CONCAT('https://', w.server_name, '/wiki/', REPLACE(t.title, ' ', '_')) AS url
      FROM `${var.project_id}.${google_bigquery_dataset.analytics.dataset_id}.trending_hourly` AS t
      LEFT JOIN (
        -- One host per wiki, taken from article edits only: login/account log events
        -- for e.g. enwiki come from auth.wikimedia.org and would duplicate every row.
        SELECT wiki, APPROX_TOP_COUNT(server_name, 1)[OFFSET(0)].value AS server_name
        FROM `${var.project_id}.${google_bigquery_dataset.raw.dataset_id}.recentchange`
        WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
          AND server_name IS NOT NULL AND change_type IN ('edit', 'new') AND namespace = 0
        GROUP BY wiki
      ) AS w USING (wiki)
      WHERE t.hour_ts = (
        SELECT MAX(hour_ts) FROM `${var.project_id}.${google_bigquery_dataset.analytics.dataset_id}.trending_hourly`
        WHERE hour_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
      )
    SQL
  }
  depends_on = [google_bigquery_table.analytics, google_bigquery_table.recentchange]
}
