# airflow-dag-auditor

An Airflow DAG that audits other Airflow deployments.

It reads your DAG, run, and task history through the Airflow 3 REST API and
produces a ranked scorecard of the pipelines most likely to be causing you
problems — flaky tasks, runtime drift, failures hidden behind retries, and DAGs
that have quietly stopped succeeding.

> **Status: v0 in progress.** Nothing below marked ✅ is built yet. This README
> documents the design and the build order; it will be updated as each version
> lands rather than describing features that don't exist.

---

## Why this exists

Most Airflow monitoring tells you a task failed. That's the easy case — it
pages someone. The expensive failures are the quiet ones:

- A task that fails 40% of the time but always passes on retry, so nobody
  notices until it exhausts retries during an incident.
- A transform whose runtime creeps up 30 seconds a week until it starts
  colliding with the job downstream of it.
- A DAG that hasn't succeeded in three weeks and nobody has looked at it.

None of these fire an alert. All of them are visible in the metadata database
if you go looking. This goes looking.

---

## Design: five layers, built bottom-up

AI engineering has accumulated a stack of techniques — prompting, then context,
then harness, then loop, and now graph. They are a **stack, not a ladder**: each
layer only earns its place if the layer beneath it is solid. The common failure
is reaching for an agent loop on top of a foundation that returns wrong answers,
producing confident elaboration on garbage.

This project applies them in order, and only where they pay for themselves:

| Layer | Where it lands here | Earns its place when |
|---|---|---|
| — | Metrics computed from run history | Always. This is the foundation. |
| **Harness** | The audit runs *as* an Airflow DAG | Airflow can audit Airflow on a schedule |
| **Prompt** | Remediation text written per finding | Findings are already correct without it |
| **Context** | DAG source + run history fed into the prompt | Generic advice is provably worse than specific |
| **Loop** | Investigate a finding across several steps | A single pass demonstrably misdiagnoses |
| **Graph** | Fan out investigations, join the reports | Sequential investigation is too slow to use |

**The scorecard makes zero LLM calls.** If it isn't useful with no API key, an
LLM won't rescue it. Every finding is deterministic arithmetic over the
metadata DB; the model's only job is turning a correct finding into an
actionable one.

---

## Build order

| Ver | Ships | Layer | Done when |
|---|---|---|---|
| **v0** | REST client, demo seeder, flakiness metric, markdown scorecard | none | Scorecard names the seeded flaky DAG worst, verified by hand against the DB |
| v1 | Runtime drift, retry-masked failures, orphaned DAGs, per-DAG score | none | All three seeded pathologies rank correctly |
| v2 | `dags/dag_audit.py` — the auditor as a scheduled DAG | harness | Airflow audits Airflow; report lands as an artifact |
| v3 | `explain.py` — remediation per finding, skipped cleanly with no API key | prompt | Advice is actionable without opening the DAG |
| v4 | DAG source + history slice fed into the prompt | context | Advice cites the actual failing operator, not generic tips |
| v5 | Per-DAG investigation, then concurrent fan-out | loop → graph | Single-pass explanations demonstrably misdiagnose something the loop gets right |

v0 deliberately implements **one** metric rather than four. One metric
end-to-end proves the client, the seeder, the computation, and the report all
work together. The remaining three are then copy-and-adapt.

---

## The demo seeder

A scorecard is only trustworthy if you can check it. You cannot validate a
flakiness metric against DAGs whose real flakiness you don't already know, so
the seeder **authors** its fixtures rather than borrowing them — three DAGs,
each engineered to exhibit exactly one pathology:

| DAG | Pathology | Induced by | Ground truth |
|---|---|---|---|
| `flaky_ingest` | Flakiness | Fails when `random() < 0.4`, `retries=2` | ~40% retry rate |
| `drifting_transform` | Runtime drift | Sleeps longer each logical date | Known linear slope |
| `orphaned_report` | Orphaned | Scheduled, never succeeds | 0 successful runs |

Run history is generated with `airflow backfill create` over a past date range,
which compresses three weeks of scheduling into a few minutes. The flaky task
genuinely fails and retries, which is what populates `task_instance_history` —
without it, flakiness is not merely thin but uncomputable.

**The seeder never touches an existing Airflow installation.** It provisions a
throwaway `AIRFLOW_HOME` (`./airflow-demo/`, gitignored) so cloning this repo
cannot write DAG files into your real scheduler.

---

## Requirements

- Python 3.11+
- Airflow 3.2+ (the client targets the `/api/v2` REST API and its
  `/auth/token` JWT flow, which do not exist in Airflow 2.x)

---

## Quickstart

Not yet — lands with v0.

---

## License

MIT
