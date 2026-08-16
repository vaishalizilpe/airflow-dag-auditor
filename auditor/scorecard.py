"""
Render the audit as a ranked markdown report.

    python -m auditor.scorecard              # to stdout
    python -m auditor.scorecard -o out.md    # to a file

Every number here is derived from counts the reader can verify against their
own metadata database, and the report says which counts they are. A scorecard
that can't be checked is just an opinion with a table around it.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from auditor.client import AirflowAuthError, AirflowClient, AirflowUnavailable
from auditor.metrics import DagFlakiness, audit_all

# Below this, a DAG is not worth a reader's attention.
REPORTING_THRESHOLD = 0.01


def _severity(stats: DagFlakiness) -> str:
    rate = stats.flakiness_rate
    if rate >= 0.30:
        return "high"
    if rate >= 0.10:
        return "medium"
    if rate > 0:
        return "low"
    return "clean"


def render(results: list[DagFlakiness]) -> str:
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# Airflow DAG Audit",
        "",
        f"_Generated {generated}_",
        "",
        "## Flakiness",
        "",
        "Share of task attempts that failed. A high rate with a low failure "
        "count means retries are absorbing the problem — the DAG looks healthy "
        "while doing the same work several times.",
        "",
    ]

    flagged = [s for s in results if s.has_evidence and s.flakiness_rate > REPORTING_THRESHOLD]

    if not flagged:
        lines += ["No DAG exceeded the reporting threshold. Nothing to act on.", ""]
    else:
        lines += [
            "| DAG | Flakiness | Severity | Failed / total attempts | Retry-masked runs | Worst task |",
            "|---|---:|---|---:|---:|---|",
        ]
        for s in flagged:
            masked = (
                f"{s.retry_masked_runs} of {s.successful_runs}"
                if s.successful_runs
                else "—"
            )
            lines.append(
                f"| `{s.dag_id}` | {s.flakiness_rate:.1%} | {_severity(s)} | "
                f"{s.failed_attempts} / {s.attempts} | {masked} | "
                f"`{s.worst_task or '—'}` |"
            )
        lines.append("")

        worst = flagged[0]
        if worst.retry_masked_runs:
            lines += [
                "### What to look at first",
                "",
                f"`{worst.dag_id}` reported **{worst.retry_masked_runs} successful "
                f"run(s) that required a retry**. Those runs are green in every "
                f"dashboard, so this will not appear in a failure report — the "
                f"task `{worst.worst_task}` is failing regularly and being "
                f"rescued. Retries are currently the only thing keeping it "
                f"passing.",
                "",
            ]

    silent = [s for s in results if not s.has_evidence]
    if silent:
        lines += [
            "## No run history",
            "",
            "These DAGs have no runs in the window, so flakiness cannot be "
            "judged. That absence is itself worth checking.",
            "",
        ]
        lines += [f"- `{s.dag_id}`" for s in silent]
        lines.append("")

    clean = [
        s for s in results
        if s.has_evidence and s.flakiness_rate <= REPORTING_THRESHOLD
    ]
    lines += [
        "## Summary",
        "",
        f"- {len(results)} DAG(s) examined",
        f"- {len(flagged)} flagged",
        f"- {len(clean)} clean",
        f"- {len(silent)} without run history",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-o", "--output", help="write the report here instead of stdout")
    args = parser.parse_args()

    try:
        with AirflowClient.from_env() as client:
            report = render(audit_all(client))
    except (AirflowAuthError, AirflowUnavailable) as exc:
        sys.exit(f"error: {exc}")

    if args.output:
        with open(args.output, "w") as handle:
            handle.write(report)
        print(f"wrote {args.output}")
    else:
        print(report)


if __name__ == "__main__":
    main()
