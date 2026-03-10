from __future__ import annotations

from typing import Any
from typing import Self

from src.scripts import refresh_foreclosures


class _CaptureResult:
    def __init__(self, rows: list[dict[str, Any]] | None = None, rowcount: int | None = None) -> None:
        self._rows = rows or []
        self.rowcount = rowcount

    def mappings(self) -> Self:
        return self

    def fetchall(self) -> list[dict[str, Any]]:
        return self._rows


class _CaptureConnection:
    def __init__(self, update_rows: list[dict[str, Any]], copy_rowcount: int = 0) -> None:
        self.update_rows = update_rows
        self.copy_rowcount = copy_rowcount
        self.executed: list[tuple[str, Any]] = []

    def execute(self, sql: Any, params: Any = None) -> _CaptureResult:
        sql_text = str(sql)
        self.executed.append((sql_text, params))
        if "RETURNING" in sql_text:
            return _CaptureResult(rows=self.update_rows)
        return _CaptureResult(rowcount=self.copy_rowcount)


def test_reuse_rescheduled_enrichment_copies_survival_rows_for_ready_pairs() -> None:
    conn = _CaptureConnection(
        update_rows=[
            {
                "new_foreclosure_id": 200,
                "donor_foreclosure_id": 150,
                "copy_survival": True,
            }
        ],
        copy_rowcount=3,
    )

    counts = refresh_foreclosures._reuse_rescheduled_enrichment(conn)  # noqa: SLF001

    assert counts == {"updated_foreclosures": 1, "copied_survival_rows": 3}
    assert len(conn.executed) == 2
    update_sql, _update_params = conn.executed[0]
    copy_sql, copy_params = conn.executed[1]
    assert "step_identifier_recovery = coalesce(" in update_sql.lower()
    assert "insert into foreclosure_encumbrance_survival" in copy_sql.lower()
    assert copy_params == [{"new_foreclosure_id": 200, "donor_foreclosure_id": 150}]


def test_reuse_rescheduled_enrichment_skips_survival_copy_when_donor_incomplete() -> None:
    conn = _CaptureConnection(
        update_rows=[
            {
                "new_foreclosure_id": 200,
                "donor_foreclosure_id": 150,
                "copy_survival": False,
            }
        ],
    )

    counts = refresh_foreclosures._reuse_rescheduled_enrichment(conn)  # noqa: SLF001

    assert counts == {"updated_foreclosures": 1, "copied_survival_rows": 0}
    assert len(conn.executed) == 1
