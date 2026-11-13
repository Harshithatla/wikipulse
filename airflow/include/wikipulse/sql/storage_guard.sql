-- Fail loudly *before* the project leaves the BigQuery free tier (10 GiB logical storage).
-- Partition expiration (set in Terraform) is the real control, this is the alarm.
-- Uses per-dataset INFORMATION_SCHEMA.PARTITIONS: unlike TABLE_STORAGE it needs no
-- project-level opt-in and only dataset read access, which the pipeline already has.
ASSERT (
  SELECT IFNULL(SUM(total_logical_bytes), 0)
  FROM (
    SELECT total_logical_bytes FROM {{ raw }}.INFORMATION_SCHEMA.PARTITIONS
    UNION ALL
    SELECT total_logical_bytes FROM {{ analytics }}.INFORMATION_SCHEMA.PARTITIONS
  )
) < {{ storage_guard_bytes }} AS 'storage guard: BigQuery logical storage is near the 10 GiB free tier';
