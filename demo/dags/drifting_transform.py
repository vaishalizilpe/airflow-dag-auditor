"""
Seed fixture: a DAG whose runtime creeps upward every day.

Never fails, so it never alerts. The damage is scheduling pressure. It slowly
eats the gap before whatever runs after it, until one day the two overlap and
something downstream reads half-written data.

Ground truth for validating the runtime-drift metric:
    duration(day_n) = BASE_SECONDS + n * DRIFT_SECONDS_PER_DAY
    => a clean linear trend with a known positive slope.
"""

from __future__ import annotations

import time
from datetime import datetime

from airflow.decorators import dag, task

START = datetime(2026, 7, 1)

# Small absolute durations keep a 21-day seed fast; the auditor measures the
# *slope*, not the magnitude, so a 0.2s/day drift is as detectable as 30s/day.
BASE_SECONDS = 0.5
DRIFT_SECONDS_PER_DAY = 0.2


@dag(
    dag_id="drifting_transform",
    schedule="@daily",
    start_date=START,
    catchup=False,
    tags=["auditor-demo", "pathology:drift"],
    doc_md=__doc__,
)
def drifting_transform():
    @task
    def transform(**context) -> float:
        """Runtime grows linearly with how far the logical date is from START."""
        logical_date = context["logical_date"]
        days_elapsed = (logical_date.replace(tzinfo=None) - START).days
        duration = BASE_SECONDS + max(days_elapsed, 0) * DRIFT_SECONDS_PER_DAY
        time.sleep(duration)
        return duration

    transform()


drifting_transform()
