-- "What is the world suddenly editing?" Rank articles whose human edit volume this
-- hour is anomalous against their own trailing 7-day hourly baseline.
--
-- Hours with no edits are real zeros, so the baseline divides by 168 hours rather
-- than averaging only the hours that have rows. The +1 in the denominator damps
-- pages with near-zero variance (and makes cold-start pages rank by raw volume).
DECLARE baseline_hours INT64 DEFAULT 168;

BEGIN TRANSACTION;

DELETE FROM {{ analytics }}.trending_hourly WHERE hour_ts = @hour_ts;

INSERT INTO {{ analytics }}.trending_hourly
  (hour_ts, rank, wiki, title, human_edits, human_editors, reverts,
   baseline_mean, baseline_stddev, spike_score, active_hours_7d)
WITH cur AS (
  SELECT wiki, title, human_edits, human_editors, reverts
  FROM {{ analytics }}.page_hourly
  WHERE hour_ts = @hour_ts AND human_editors >= 3
),
hist AS (
  SELECT p.wiki, p.title,
         SUM(p.human_edits) AS s,
         SUM(p.human_edits * p.human_edits) AS s2,
         COUNT(*) AS active_hours
  FROM {{ analytics }}.page_hourly AS p
  JOIN cur USING (wiki, title)
  WHERE p.hour_ts >= TIMESTAMP_SUB(@hour_ts, INTERVAL 168 HOUR) AND p.hour_ts < @hour_ts
  GROUP BY p.wiki, p.title
),
scored AS (
  SELECT
    cur.*,
    IFNULL(hist.s, 0) / baseline_hours AS mu,
    GREATEST(IFNULL(hist.s2, 0) / baseline_hours - POW(IFNULL(hist.s, 0) / baseline_hours, 2), 0) AS var,
    IFNULL(hist.active_hours, 0) AS active_hours
  FROM cur LEFT JOIN hist USING (wiki, title)
)
SELECT
  @hour_ts,
  RANK() OVER (ORDER BY (human_edits - mu) / SQRT(var + 1) DESC, human_editors DESC),
  wiki, title, human_edits, human_editors, reverts,
  ROUND(mu, 3),
  ROUND(SQRT(var), 3),
  ROUND((human_edits - mu) / SQRT(var + 1), 2),
  active_hours
FROM scored
WHERE TRUE  -- BigQuery requires WHERE/GROUP BY/HAVING alongside QUALIFY
QUALIFY RANK() OVER (ORDER BY (human_edits - mu) / SQRT(var + 1) DESC, human_editors DESC) <= 100;

COMMIT TRANSACTION;
