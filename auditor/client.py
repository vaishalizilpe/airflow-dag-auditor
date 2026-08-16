"""
Airflow 3 REST client.

Talks to `/api/v2` using the JWT flow introduced in Airflow 3: POST credentials
to `/auth/token`, then send the returned token as a bearer on every call. This
is *not* compatible with Airflow 2.x, whose API is `/api/v1` and authenticates
differently.

    from auditor.client import AirflowClient

    client = AirflowClient.from_env()
    for dag in client.list_dags():
        print(dag["dag_id"])

Credentials come from the environment so they never land in source:

    AIRFLOW_API_URL       default http://localhost:8080
    AIRFLOW_USERNAME      default admin
    AIRFLOW_PASSWORD      required

With the default SimpleAuthManager, the generated password lives in
$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterator

import httpx

DEFAULT_URL = "http://localhost:8080"
DEFAULT_USERNAME = "admin"
PAGE_SIZE = 100


class AirflowAuthError(RuntimeError):
    """Credentials were rejected, or no credentials were available."""


class AirflowUnavailable(RuntimeError):
    """The API server could not be reached."""


class AirflowClient:
    """Minimal read-only client for the endpoints the auditor needs."""

    def __init__(
        self,
        base_url: str = DEFAULT_URL,
        username: str = DEFAULT_USERNAME,
        password: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self._token: str | None = None
        self._http = httpx.Client(timeout=timeout)

    # ── construction ─────────────────────────────────────────────────────────

    @classmethod
    def from_env(cls) -> AirflowClient:
        """Build from environment, falling back to the SimpleAuthManager file."""
        password = os.getenv("AIRFLOW_PASSWORD") or _password_from_generated_file()
        if not password:
            raise AirflowAuthError(
                "No Airflow password found. Set AIRFLOW_PASSWORD, or point "
                "AIRFLOW_HOME at an instance whose "
                "simple_auth_manager_passwords.json.generated is readable."
            )
        return cls(
            base_url=os.getenv("AIRFLOW_API_URL", DEFAULT_URL),
            username=os.getenv("AIRFLOW_USERNAME", DEFAULT_USERNAME),
            password=password,
        )

    # ── auth ─────────────────────────────────────────────────────────────────

    def _authenticate(self) -> str:
        try:
            response = self._http.post(
                f"{self.base_url}/auth/token",
                json={"username": self.username, "password": self.password},
            )
        except httpx.RequestError as exc:
            raise AirflowUnavailable(
                f"Could not reach Airflow at {self.base_url}. "
                "Is it running? Try: airflow standalone"
            ) from exc

        if response.status_code in (401, 403):
            raise AirflowAuthError(
                f"Airflow rejected credentials for user '{self.username}'."
            )
        response.raise_for_status()

        token = response.json().get("access_token")
        if not token:
            raise AirflowAuthError("/auth/token returned no access_token.")
        return token

    @property
    def token(self) -> str:
        if self._token is None:
            self._token = self._authenticate()
        return self._token

    # ── requests ─────────────────────────────────────────────────────────────

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict:
        url = f"{self.base_url}/api/v2/{path.lstrip('/')}"
        headers = {"Authorization": f"Bearer {self.token}"}
        try:
            response = self._http.get(url, headers=headers, params=params)
        except httpx.RequestError as exc:
            raise AirflowUnavailable(f"Request to {url} failed: {exc}") from exc

        # The token is short-lived; refresh once before giving up.
        if response.status_code == 401:
            self._token = None
            headers = {"Authorization": f"Bearer {self.token}"}
            response = self._http.get(url, headers=headers, params=params)

        response.raise_for_status()
        return response.json()

    def _paginate(self, path: str, key: str,
                  params: dict[str, Any] | None = None) -> Iterator[dict]:
        """Yield every item across pages. Airflow uses limit/offset."""
        params = dict(params or {})
        offset = 0
        while True:
            params.update({"limit": PAGE_SIZE, "offset": offset})
            payload = self._get(path, params)
            batch = payload.get(key, [])
            yield from batch
            offset += len(batch)
            if len(batch) < PAGE_SIZE or offset >= payload.get("total_entries", 0):
                return

    # ── endpoints the auditor uses ───────────────────────────────────────────

    def version(self) -> str:
        return self._get("version").get("version", "unknown")

    def list_dags(self, include_paused: bool = True) -> list[dict]:
        params = {} if include_paused else {"paused": False}
        return list(self._paginate("dags", "dags", params))

    def list_dag_runs(self, dag_id: str) -> list[dict]:
        return list(self._paginate(f"dags/{dag_id}/dagRuns", "dag_runs"))

    def list_task_instances(self, dag_id: str, run_id: str) -> list[dict]:
        return list(
            self._paginate(
                f"dags/{dag_id}/dagRuns/{run_id}/taskInstances", "task_instances"
            )
        )

    def list_task_tries(self, dag_id: str, run_id: str, task_id: str) -> list[dict]:
        """
        Every recorded attempt for a task, including failed ones.

        This is the endpoint that makes flakiness measurable: a task that fails
        and then succeeds on retry looks like a plain success everywhere else,
        but each failed attempt is archived and surfaced here.
        """
        return list(
            self._paginate(
                f"dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/tries",
                "task_instances",
            )
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> AirflowClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def _password_from_generated_file() -> str | None:
    """Read the SimpleAuthManager password that `airflow standalone` writes."""
    airflow_home = os.getenv("AIRFLOW_HOME")
    if not airflow_home:
        return None
    path = Path(airflow_home) / "simple_auth_manager_passwords.json.generated"
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text()).get(os.getenv("AIRFLOW_USERNAME", DEFAULT_USERNAME))
    except (json.JSONDecodeError, OSError):
        return None
