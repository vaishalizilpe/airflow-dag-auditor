"""
Seed fixture: a DAG that fails intermittently but almost always passes on retry.

This is the pathology the auditor exists to catch. Nothing here pages anyone, because
the DAG's final state is usually `success`, so dashboards look clean. The cost
is hidden in the retry count, and it only becomes an incident when a bad run
exhausts its retries at the worst moment.

Ground truth for validating the flakiness metric:
    failure probability per attempt = 0.4
    retries                         = 2
    => ~40% of attempts fail; ~6.4% of runs (0.4^3) exhaust retries and fail.

The seed is derived from the logical date so a backfill is reproducible: the
same date range always produces the same pass/fail pattern.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta

from airflow.decorators import dag, task

FAILURE_RATE = 0.4

# Airflow's default retry_delay is 5 minutes, which would make seeding three
# weeks of history take hours. A demo fixture has to be fast to be useful.
RETRY_DELAY = timedelta(seconds=1)


def _deterministic_roll(logical_date: datetime) -> float:
    """A stable pseudo-random value in [0, 1) derived from the logical date."""
    digest = hashlib.sha256(logical_date.isoformat().encode()).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF


@dag(
    dag_id="flaky_ingest",
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": RETRY_DELAY},
    tags=["auditor-demo", "pathology:flaky"],
    doc_md=__doc__,
)
def flaky_ingest():
    @task
    def pull_from_upstream(**context) -> int:
        """Simulates a flaky network dependency."""
        roll = _deterministic_roll(context["logical_date"])
        attempt = context["task_instance"].try_number

        # Later attempts get a better roll, so retries usually succeed,
        # which is exactly what makes this pathology invisible on a dashboard.
        if roll < FAILURE_RATE / attempt:
            raise RuntimeError(
                f"upstream timed out (roll={roll:.3f}, attempt={attempt})"
            )
        return 1

    pull_from_upstream()


flaky_ingest()
