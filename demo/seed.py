"""
Provision a throwaway Airflow with a known-bad DAG population.

The auditor's findings are only trustworthy if you can check them, so this
seeds fixtures whose pathologies are known in advance (see demo/dags/*.py for
each one's ground truth). Run the auditor against this and you can verify every
number by hand.

    python -m demo.seed              # provision + seed 21 days
    python -m demo.seed --days 7     # shorter run
    python -m demo.seed --reset      # wipe and start over

This never touches an existing Airflow install. It creates its own AIRFLOW_HOME
at ./airflow-demo/ (gitignored) so cloning this repo cannot write DAG files
into your real scheduler.

History is generated with `airflow dags test`, which executes one DagRun
synchronously without needing a scheduler or API server running. Failed
attempts are archived to `task_instance_history`, which is what makes
flakiness measurable at all.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEMO_HOME = REPO_ROOT / "airflow-demo"
FIXTURE_DAGS = Path(__file__).resolve().parent / "dags"

# The fixtures' start_date. Seeding walks forward from here.
SEED_START = date(2026, 7, 1)
DEFAULT_DAYS = 21


def _env() -> dict[str, str]:
    """Environment that pins Airflow to the demo home, examples off."""
    env = dict(os.environ)
    env["AIRFLOW_HOME"] = str(DEMO_HOME)
    env["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    # Fixtures must run in-process; a demo shouldn't need a broker.
    env["AIRFLOW__CORE__EXECUTOR"] = "LocalExecutor"
    return env


def _run(args: list[str], quiet: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["airflow", *args],
        env=_env(),
        capture_output=quiet,
        text=True,
    )


def _require_airflow() -> None:
    if shutil.which("airflow") is None:
        sys.exit(
            "airflow not found on PATH.\n"
            "Activate the virtualenv that has Airflow 3.2+ installed, then re-run."
        )


def provision(reset: bool) -> None:
    if reset and DEMO_HOME.exists():
        print(f"removing {DEMO_HOME}")
        shutil.rmtree(DEMO_HOME)

    (DEMO_HOME / "dags").mkdir(parents=True, exist_ok=True)
    for fixture in sorted(FIXTURE_DAGS.glob("*.py")):
        shutil.copy2(fixture, DEMO_HOME / "dags" / fixture.name)
    print(f"provisioned {DEMO_HOME}")

    print("initializing database...")
    _run(["db", "migrate"])

    print("parsing fixtures...")
    result = _run(["dags", "reserialize"])
    if result.returncode != 0:
        sys.exit(f"failed to parse fixture DAGs:\n{result.stderr[-2000:]}")


def seed(days: int) -> None:
    """Run each fixture once per logical date, oldest first."""
    dag_ids = sorted(p.stem for p in FIXTURE_DAGS.glob("*.py"))
    total = days * len(dag_ids)
    done = 0

    print(f"seeding {days} days x {len(dag_ids)} dags = {total} runs")
    for offset in range(days):
        logical_date = SEED_START + timedelta(days=offset)
        for dag_id in dag_ids:
            done += 1
            # Fixtures are *designed* to fail, so a non-zero exit is expected
            # and is itself the data we want. Only report it at high verbosity.
            _run(["dags", "test", dag_id, logical_date.isoformat()])
            print(f"  [{done:3}/{total}] {logical_date} {dag_id}", flush=True)

    print(f"\nseeded. AIRFLOW_HOME={DEMO_HOME}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS,
                        help=f"logical dates to seed (default {DEFAULT_DAYS})")
    parser.add_argument("--reset", action="store_true",
                        help="delete the demo home before provisioning")
    args = parser.parse_args()

    _require_airflow()
    provision(reset=args.reset)
    seed(days=args.days)


if __name__ == "__main__":
    main()
