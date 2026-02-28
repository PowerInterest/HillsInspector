from __future__ import annotations

import datetime as dt
from typing import Self

from src.services import pg_job_control_service


class _FakeResult:
    def __init__(
        self,
        *,
        scalar: object | None = None,
        scalar_one: object | None = None,
        fetchone: object | None = None,
        fetchall: list[object] | None = None,
    ) -> None:
        self._scalar = scalar
        self._scalar_one = scalar if scalar_one is None else scalar_one
        self._fetchone = fetchone
        self._fetchall = fetchall or []

    def scalar(self) -> object | None:
        return self._scalar

    def scalar_one(self) -> object | None:
        return self._scalar_one

    def fetchone(self) -> object | None:
        return self._fetchone

    def fetchall(self) -> list[object]:
        return self._fetchall

    def mappings(self) -> _FakeResult:
        return self


class _LockingConnection:
    def __init__(self, *, lock_acquired: bool) -> None:
        self.lock_acquired = lock_acquired
        self.run_rows: list[dict[str, object]] = []
        self.statements: list[str] = []
        self.commits = 0
        self.rollbacks = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> bool:
        return False

    def execute(
        self,
        statement: object,
        params: dict[str, object] | None = None,
    ) -> _FakeResult:
        sql = " ".join(str(statement).split())
        self.statements.append(sql)

        if "INSERT INTO pipeline_job_config" in sql:
            return _FakeResult()
        if "SELECT pg_try_advisory_lock" in sql:
            return _FakeResult(scalar=self.lock_acquired)
        if "INSERT INTO pipeline_job_runs" in sql:
            assert params is not None
            self.run_rows.append(dict(params))
            return _FakeResult(scalar_one=len(self.run_rows))
        raise AssertionError(f"Unexpected SQL in test: {sql}")

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class _FakeEngine:
    def __init__(self, conn: _LockingConnection) -> None:
        self.conn = conn

    def connect(self) -> _LockingConnection:
        return self.conn


class _RecentConnection:
    def __init__(self, latest: dt.datetime) -> None:
        self.latest = latest
        self.sql = ""

    def execute(
        self,
        statement: object,
        _params: dict[str, object] | None = None,
    ) -> _FakeResult:
        self.sql = " ".join(str(statement).split())
        return _FakeResult(fetchone=(self.latest,))


def test_run_job_records_lock_contention_skip(monkeypatch: object) -> None:
    conn = _LockingConnection(lock_acquired=False)
    monkeypatch.setattr(
        pg_job_control_service,
        "resolve_pg_dsn",
        lambda _dsn: "postgresql://user:pw@host:5432/db",
    )
    monkeypatch.setattr(
        pg_job_control_service,
        "get_engine",
        lambda _dsn: _FakeEngine(conn),
    )

    service = pg_job_control_service.PgJobControlService()
    definition = pg_job_control_service.JobDefinition(
        name="test_job",
        handler=lambda *_args, **_kwargs: {
            "success": True
        },
    )

    result = service.run_job(definition, triggered_by="cron")

    assert result["status"] == "skipped"
    assert result["reason"] == "lock_not_acquired"
    assert result["run_id"] == 1
    assert len(conn.run_rows) == 1
    assert conn.run_rows[0]["status"] == "skipped"
    assert '"reason": "lock_not_acquired"' in str(conn.run_rows[0]["summary_json"])


def test_ran_recently_only_checks_running_and_success_rows() -> None:
    latest_success = dt.datetime.now(dt.UTC) - dt.timedelta(hours=2)
    conn = _RecentConnection(latest_success)
    service = object.__new__(pg_job_control_service.PgJobControlService)

    recent = service._ran_recently(  # noqa: SLF001
        conn,
        job_name="test_job",
        min_interval_sec=3600,
    )

    assert recent is False
    assert "status IN ('running', 'success')" in conn.sql
    assert "skipped" not in conn.sql
