"""
Flakiness maths, verified against hand-built histories.

These use a fake client rather than a live Airflow so the expected numbers can
be reasoned about directly. The end-to-end check against a real instance is the
seeded demo; this is the part that pins the arithmetic.
"""

from __future__ import annotations

import pytest

from auditor.metrics import collect_flakiness


class FakeClient:
    """Minimal stand-in shaped like the endpoints collect_flakiness() calls."""

    def __init__(self, runs: list[dict]) -> None:
        # runs: [{"state": ..., "tries": [state, state, ...]}, ...]
        self._runs = runs

    def list_dag_runs(self, dag_id: str) -> list[dict]:
        return [
            {"dag_run_id": f"run_{i}", "state": r["state"]}
            for i, r in enumerate(self._runs)
        ]

    def list_task_instances(self, dag_id: str, run_id: str) -> list[dict]:
        return [{"task_id": "t", "state": "success"}]

    def list_task_tries(self, dag_id: str, run_id: str, task_id: str) -> list[dict]:
        index = int(run_id.split("_")[1])
        return [{"state": s} for s in self._runs[index]["tries"]]


def test_clean_dag_scores_zero():
    client = FakeClient([{"state": "success", "tries": ["success"]}] * 5)
    stats = collect_flakiness(client, "clean")

    assert stats.flakiness_rate == 0.0
    assert stats.retry_masked_runs == 0
    assert stats.total_runs == 5


def test_retry_masked_run_is_counted():
    """The core case: the run is green, but only because a retry rescued it."""
    client = FakeClient([{"state": "success", "tries": ["failed", "success"]}])
    stats = collect_flakiness(client, "masked")

    assert stats.successful_runs == 1
    assert stats.retry_masked_runs == 1, "a green run that retried must be flagged"
    assert stats.failed_attempts == 1
    assert stats.attempts == 2
    assert stats.flakiness_rate == 0.5


def test_exhausted_retries_are_not_retry_masked():
    """A run that failed outright is visible already, so it is not 'masked'."""
    client = FakeClient(
        [{"state": "failed", "tries": ["failed", "failed", "failed"]}]
    )
    stats = collect_flakiness(client, "loud")

    assert stats.failed_runs == 1
    assert stats.retry_masked_runs == 0
    assert stats.flakiness_rate == 1.0


def test_mixed_history_matches_hand_computation():
    """
    Mirrors the seeded fixture's shape:
    15 clean, 3 rescued by one retry, 3 that exhausted 3 attempts.
    """
    runs = (
        [{"state": "success", "tries": ["success"]}] * 15
        + [{"state": "success", "tries": ["failed", "success"]}] * 3
        + [{"state": "failed", "tries": ["failed", "failed", "failed"]}] * 3
    )
    stats = collect_flakiness(FakeClient(runs), "flaky_ingest")

    assert stats.total_runs == 21
    assert stats.successful_runs == 18
    assert stats.failed_runs == 3
    # 15*1 + 3*2 + 3*3 = 30 attempts; 3*1 + 3*3 = 12 of them failed
    assert stats.attempts == 30
    assert stats.failed_attempts == 12
    assert stats.flakiness_rate == pytest.approx(12 / 30)
    assert stats.retry_masked_runs == 3
    assert stats.retry_masked_rate == pytest.approx(3 / 18)


def test_dag_with_no_history_is_reported_not_scored():
    """Zero runs must not look like a perfect score."""
    stats = collect_flakiness(FakeClient([]), "never_ran")

    assert stats.has_evidence is False
    assert stats.flakiness_rate == 0.0


def test_worst_task_is_the_most_failing_one():
    class MultiTaskClient(FakeClient):
        def list_task_instances(self, dag_id, run_id):
            return [{"task_id": "ok", "state": "success"},
                    {"task_id": "bad", "state": "success"}]

        def list_task_tries(self, dag_id, run_id, task_id):
            if task_id == "bad":
                return [{"state": "failed"}, {"state": "success"}]
            return [{"state": "success"}]

    stats = collect_flakiness(MultiTaskClient([{"state": "success", "tries": []}]), "d")
    assert stats.worst_task == "bad"
