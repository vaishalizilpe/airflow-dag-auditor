"""
Seed fixture: a scheduled DAG that has never succeeded.

Someone shipped it, it broke, and the team stopped looking at it. It still
occupies a schedule slot and still shows up in the DAG list, so it reads as
"we have a report for that" long after the report stopped existing.

Ground truth for validating the orphaned-DAG metric:
    successful runs = 0, across every run in the window.

The failure is a missing config key rather than a random roll, so this DAG
fails identically on every attempt. Unlike `flaky_ingest`, no retry saves it.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

REQUIRED_CONNECTION = "warehouse_readonly_v2"

# Keep seeding fast. See the note in flaky_ingest.py.
RETRY_DELAY = timedelta(seconds=1)


@dag(
    dag_id="orphaned_report",
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": RETRY_DELAY},
    tags=["auditor-demo", "pathology:orphaned"],
    doc_md=__doc__,
)
def orphaned_report():
    @task
    def build_weekly_report() -> None:
        """Depends on a connection that was removed during a migration."""
        raise RuntimeError(
            f"connection '{REQUIRED_CONNECTION}' is not defined, "
            "renamed during the warehouse migration and never updated here"
        )

    build_weekly_report()


orphaned_report()
