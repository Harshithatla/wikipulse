-- Per-wiki health metrics for one event hour (all namespaces and change types).
BEGIN TRANSACTION;

DELETE FROM {{ analytics }}.wiki_hourly WHERE hour_ts = @hour_ts;

INSERT INTO {{ analytics }}.wiki_hourly
  (hour_ts, wiki, events, edits, new_pages, log_events, bot_share,
   unique_editors, anon_like_editors, reverts, revert_rate, p50_kafka_lag_s, p95_kafka_lag_s)
SELECT
  @hour_ts,
  wiki,
  COUNT(*),
  COUNTIF(change_type = 'edit'),
  COUNTIF(change_type = 'new'),
  COUNTIF(change_type = 'log'),
  ROUND(SAFE_DIVIDE(COUNTIF(is_bot), COUNT(*)), 4),
  COUNT(DISTINCT user_name),
  -- Unregistered editors show up as IPv4/IPv6 addresses or temporary accounts (~2025-xxxxx).
  COUNT(DISTINCT IF(REGEXP_CONTAINS(user_name, r'^(\d{1,3}(\.\d{1,3}){3}|[0-9A-Fa-f:]+:[0-9A-Fa-f:]+|~\d{4}-)'),
                    user_name, NULL)),
  COUNTIF(is_revert),
  ROUND(SAFE_DIVIDE(COUNTIF(is_revert), NULLIF(COUNTIF(change_type = 'edit'), 0)), 4),
  -- End-to-end freshness: event time -> Kafka append time.
  APPROX_QUANTILES(TIMESTAMP_DIFF(kafka_ts, event_ts, SECOND), 100)[OFFSET(50)],
  APPROX_QUANTILES(TIMESTAMP_DIFF(kafka_ts, event_ts, SECOND), 100)[OFFSET(95)]
FROM {{ raw }}.recentchange
WHERE event_ts >= @hour_ts AND event_ts < TIMESTAMP_ADD(@hour_ts, INTERVAL 1 HOUR)
GROUP BY wiki;

COMMIT TRANSACTION;
