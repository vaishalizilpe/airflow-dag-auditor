# airflow-dag-auditor

An Airflow DAG that audits other Airflow deployments.

It reads your DAG, run, and task history through the Airflow 3 REST API and
produces a ranked scorecard of the pipelines most likely to be causing you
problems: flaky tasks, runtime drift, failures hidden behind retries, and DAGs
that have quietly stopped succeeding.

> **Status: v0 shipped.** Flakiness detection works end to end against a real
> Airflow 3 instance. Runtime drift, orphan detection, and the LLM layers are
> not built yet. See the build order below. This README is updated as each
> version lands rather than describing features that don't exist.

---

## Why this exists

Most Airflow monitoring tells you a task failed. That's the easy case, because it
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

AI engineering has accumulated a stack of techniques: prompting, then context,
then harness, then loop, and now graph. They are a **stack, not a ladder**: each
layer only earns its place if the layer beneath it is solid. The common failure
is reaching for an agent loop on top of a foundation that returns wrong answers,
producing confident elaboration on garbage.

This project applies them in order, and only where they pay for themselves:

| Layer | Where it lands here | Earns its place when |
|---|---|---|
| (none) | Metrics computed from run history | Always. This is the foundation. |
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
| **v0** ✅ | REST client, demo seeder, flakiness metric, markdown scorecard | none | Shipped. Scorecard leads with the retry-masked DAG; all figures match fixture ground truth |
| v1 | Runtime drift, retry-masked failures, orphaned DAGs, per-DAG score | none | All three seeded pathologies rank correctly |
| v2 | `dags/dag_audit.py`, the auditor as a scheduled DAG | harness | Airflow audits Airflow; report lands as an artifact |
| v3 | `explain.py`, remediation per finding, skipped cleanly with no API key | prompt | Advice is actionable without opening the DAG |
| v4 | DAG source + history slice fed into the prompt | context | Advice cites the actual failing operator, not generic tips |
| v5 | Per-DAG investigation, then concurrent fan-out | loop → graph | Single-pass explanations demonstrably misdiagnose something the loop gets right |

v0 deliberately implements **one** metric rather than four. One metric
end-to-end proves the client, the seeder, the computation, and the report all
work together. The remaining three are then copy-and-adapt.

---

## The demo seeder

A scorecard is only trustworthy if you can check it. You cannot validate a
flakiness metric against DAGs whose real flakiness you don't already know, so
the seeder **authors** its fixtures rather than borrowing them. Three DAGs,
each engineered to exhibit exactly one pathology:

| DAG | Pathology | Induced by | Ground truth over 21 days |
|---|---|---|---|
| `flaky_ingest` | Flakiness | Attempt fails when a date-seeded roll `< 0.4 / attempt`, `retries=2` | 10 failed of 30 attempts (33.3%); 5 runs rescued by a retry |
| `drifting_transform` | Runtime drift | Sleeps `0.5s + 0.2s × days_elapsed` | Linear slope of +0.2s/day |
| `orphaned_report` | Orphaned | Raises on a connection removed during a migration | 0 successful runs; 42 failed of 42 attempts |

The roll is derived from the logical date rather than `random()`, so the same
date range always produces the same pass/fail pattern. A fixture you cannot
predict is a fixture you can't validate against.

Run history is generated with `airflow dags test`, which executes one DagRun
synchronously per logical date. That needs no scheduler and no broker, so the
seed is a single command. The flaky task genuinely fails and retries, and each
failed attempt is archived to `task_instance_history`, which is what makes
flakiness measurable at all. Without those archived attempts a rescued run is
indistinguishable from a clean one.

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

Five minutes from clone to a scorecard, against a throwaway Airflow that this
creates for you. Your own Airflow is never touched.

```bash
pip install -r requirements.txt

# 1. Provision an isolated Airflow and seed 21 days of history.
#    Takes ~4 minutes; the fixtures genuinely run, fail, and retry.
python -m demo.seed --reset

# 2. Start the demo instance's API server.
AIRFLOW_HOME=$PWD/airflow-demo \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
  airflow api-server --port 8081

# 3. In another shell, audit it.
AIRFLOW_HOME=$PWD/airflow-demo \
AIRFLOW_API_URL=http://localhost:8081 \
  python -m auditor.scorecard
```

To audit a **real** deployment instead, skip steps 1 and 2 and point the
auditor at it:

```bash
AIRFLOW_API_URL=https://airflow.example.com \
AIRFLOW_USERNAME=your_user \
AIRFLOW_PASSWORD=your_password \
  python -m auditor.scorecard -o audit.md
```

The auditor only issues GET requests.

### What the demo produces

```
| DAG                 | Flakiness | Severity | Failed / total | Retry-masked | Worst task           |
|---------------------|----------:|----------|---------------:|-------------:|----------------------|
| `orphaned_report`   |    100.0% | high     |        42 / 42 |          n/a | `build_weekly_report`|
| `flaky_ingest`      |     33.3% | high     |        10 / 30 |    5 of 20   | `pull_from_upstream` |
```

Every figure is checkable against the fixtures' documented ground truth:
`flaky_ingest` fails an attempt when a date-seeded roll falls under
`0.4 / attempt_number`, which over 21 days produces 15 clean runs, 3 rescued on
the second attempt, 2 on the third, and 1 that exhausts its retries, which is exactly
the 10 failed attempts out of 30 shown above.

The report leads with `flaky_ingest` rather than the DAG at the top of the
table, and that is deliberate: `orphaned_report` fails outright, so it already
turns dashboards red and someone knows about it. `flaky_ingest` passes 20 of 21
runs and is invisible everywhere else. That is the finding this tool exists to
produce.

---

## License

MIT
