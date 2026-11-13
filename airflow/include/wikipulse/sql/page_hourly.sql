-- Per-article human/bot activity for one event hour (@hour_ts).
-- Delete+insert inside a transaction: reruns and late-data reconciles are idempotent.
BEGIN TRANSACTION;

DELETE FROM {{ analytics }}.page_hourly WHERE hour_ts = @hour_ts;

INSERT INTO {{ analytics }}.page_hourly
  (hour_ts, wiki, title, edits, human_edits, bot_edits, editors, human_editors,
   reverts, new_pages, net_bytes)
SELECT
  @hour_ts,
  wiki,
  title,
  COUNT(*),
  COUNTIF(NOT is_bot),
  COUNTIF(is_bot),
  COUNT(DISTINCT user_name),
  COUNT(DISTINCT IF(NOT is_bot, user_name, NULL)),
  COUNTIF(is_revert),
  COUNTIF(change_type = 'new'),
  SUM(bytes_delta)
FROM {{ raw }}.recentchange
WHERE event_ts >= @hour_ts AND event_ts < TIMESTAMP_ADD(@hour_ts, INTERVAL 1 HOUR)
  AND change_type IN ('edit', 'new')
  AND namespace = 0
GROUP BY wiki, title;

COMMIT TRANSACTION;
