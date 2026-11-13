"""Runtime settings from environment variables (set in docker-compose / .env)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone

from wikipulse.pipeline import BQConfig


@dataclass(frozen=True)
class Settings:
    lake_path: str = os.getenv("LAKE_PATH", "/lake")
    bucket: str = os.getenv("WIKIPULSE_BUCKET", "set-WIKIPULSE_BUCKET")
    gcp_conn_id: str = os.getenv("WIKIPULSE_GCP_CONN_ID", "google_cloud_default")
    grace_minutes: int = int(os.getenv("WIKIPULSE_GRACE_MINUTES", "10"))
    local_retention_days: int = int(os.getenv("WIKIPULSE_LOCAL_RETENTION_DAYS", "3"))
    start_date: datetime = datetime.fromisoformat(
        os.getenv("WIKIPULSE_START", "2026-01-01T00:00:00")).replace(tzinfo=timezone.utc)
    bq: BQConfig = field(default_factory=lambda: BQConfig(
        project=os.getenv("WIKIPULSE_GCP_PROJECT", "set-WIKIPULSE_GCP_PROJECT"),
        raw_dataset=os.getenv("WIKIPULSE_RAW_DATASET", "wikipulse_raw"),
        analytics_dataset=os.getenv("WIKIPULSE_ANALYTICS_DATASET", "wikipulse_analytics"),
        location=os.getenv("WIKIPULSE_BQ_LOCATION", "us-central1"),
        storage_guard_gib=float(os.getenv("WIKIPULSE_STORAGE_GUARD_GIB", "8")),
    ))

    @property
    def bronze_path(self) -> str:
        return f"{self.lake_path}/bronze/recentchange"

    @property
    def compacted_path(self) -> str:
        return f"{self.lake_path}/compacted/recentchange"


SETTINGS = Settings()
