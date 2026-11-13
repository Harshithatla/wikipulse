-- Data-quality gate for one hour. Any failing ASSERT fails the Airflow task.
DECLARE raw_rows INT64;
DECLARE dupes INT64;
DECLARE raw_article_edits INT64;
DECLARE null_users INT64;
DECLARE model_article_edits INT64;
DECLARE wiki_events INT64;

SET (raw_rows, dupes, raw_article_edits, null_users) = (
  SELECT AS STRUCT
    COUNT(*),
    COUNT(*) - COUNT(DISTINCT event_id),
    COUNTIF(change_type IN ('edit', 'new') AND namespace = 0),
    COUNTIF(user_name IS NULL)
  FROM {{ raw }}.recentchange
  WHERE event_ts >= @hour_ts AND event_ts < TIMESTAMP_ADD(@hour_ts, INTERVAL 1 HOUR)
);
SET model_article_edits = (
  SELECT IFNULL(SUM(edits), 0) FROM {{ analytics }}.page_hourly WHERE hour_ts = @hour_ts
);
SET wiki_events = (
  SELECT IFNULL(SUM(events), 0) FROM {{ analytics }}.wiki_hourly WHERE hour_ts = @hour_ts
);

ASSERT raw_rows > 0 AS 'completeness: raw.recentchange has no rows for this hour';
ASSERT dupes = 0 AS 'uniqueness: duplicate event_id within the hour';
ASSERT null_users <= raw_rows * 0.01 AS 'validity: more than 1% of events have no user';
ASSERT model_article_edits = raw_article_edits AS 'reconciliation: page_hourly edits != raw article edits';
ASSERT wiki_events = raw_rows AS 'reconciliation: wiki_hourly events != raw rows';
ASSERT NOT EXISTS (
  SELECT 1 FROM {{ analytics }}.trending_hourly
  WHERE hour_ts = @hour_ts AND (spike_score IS NULL OR human_editors < 3)
) AS 'validity: trending_hourly contains unscored or under-threshold rows';
