"""
Flakiness: the failures that never page anyone.

A task that fails and then succeeds on retry leaves the DAG green. Dashboards
show success, nobody investigates, and the cost stays invisible until a bad run
exhausts its retries during an incident, at which point it looks like a sudden
new problem rather than one that has been there for months.

The signal is in the attempt history. Airflow archives every failed attempt, so
a run that "succeeded" after two failures is distinguishable from one that
succeeded cleanly, if you go and look.

Definitions (all computed per DAG, over the runs available):

    attempts            every recorded task attempt, successful or not
    failed_attempts     attempts that ended in a non-success state
    flakiness_rate      failed_attempts / attempts
    retry_masked_runs   runs that SUCCEEDED but needed at least one retry

`flakiness_rate` is the headline number. `retry_masked_runs` is the one that
tends to change minds, because those runs are indistinguishable from healthy
ones in every other tool.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from auditor.client import AirflowClient

SUCCESS_STATES = {"success"}


@dataclass
class DagFlakiness:
    """Flakiness evidence for one DAG. Every field is checkable by hand."""

    dag_id: str
    total_runs: int = 0
    successful_runs: int = 0
    failed_runs: int = 0
    attempts: int = 0
    failed_attempts: int = 0
    retry_masked_runs: int = 0
    worst_task: str | None = None
    _task_failures: dict[str, int] = field(default_factory=dict, repr=False)

    @property
    def flakiness_rate(self) -> float:
        """Share of attempts that failed. 0.0 when a DAG has never run."""
        if self.attempts == 0:
            return 0.0
        return self.failed_attempts / self.attempts

    @property
    def retry_masked_rate(self) -> float:
        """Share of successful runs that were only green because of a retry."""
        if self.successful_runs == 0:
            return 0.0
        return self.retry_masked_runs / self.successful_runs

    @property
    def has_evidence(self) -> bool:
        """False when there is no history to judge. Report, do not score."""
        return self.total_runs > 0


def collect_flakiness(client: AirflowClient, dag_id: str) -> DagFlakiness:
    """
    Walk a DAG's runs and count attempts.

    One request per task per run, so this is chatty on large deployments. That
    is acceptable for v0 against a demo instance; batching is a later problem
    and should not shape the metric's definition now.
    """
    stats = DagFlakiness(dag_id=dag_id)

    for run in client.list_dag_runs(dag_id):
        run_id = run["dag_run_id"]
        run_state = run.get("state")
        stats.total_runs += 1

        run_had_retry = False
        for task_instance in client.list_task_instances(dag_id, run_id):
            task_id = task_instance["task_id"]

            tries = client.list_task_tries(dag_id, run_id, task_id)
            # A task that never retried may report no archived tries; it still
            # ran once, so count the task instance itself as the single attempt.
            if not tries:
                tries = [task_instance]

            for attempt in tries:
                stats.attempts += 1
                if attempt.get("state") not in SUCCESS_STATES:
                    stats.failed_attempts += 1
                    run_had_retry = True
                    stats._task_failures[task_id] = (
                        stats._task_failures.get(task_id, 0) + 1
                    )

        if run_state in SUCCESS_STATES:
            stats.successful_runs += 1
            if run_had_retry:
                stats.retry_masked_runs += 1
        else:
            stats.failed_runs += 1

    if stats._task_failures:
        stats.worst_task = max(stats._task_failures, key=stats._task_failures.get)

    return stats


def audit_all(client: AirflowClient) -> list[DagFlakiness]:
    """Collect flakiness for every DAG, worst first."""
    results = [
        collect_flakiness(client, dag["dag_id"]) for dag in client.list_dags()
    ]
    return sorted(results, key=lambda s: s.flakiness_rate, reverse=True)
