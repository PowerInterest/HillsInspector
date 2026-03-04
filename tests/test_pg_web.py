from collections.abc import Sequence
from typing import Any

from sqlalchemy.sql.elements import TextClause

from app.web.pg_web import _build_bank_escrow_profiles
from app.web.pg_web import _compute_intel_flags


class _FakeResult:
    def __init__(self, rows: Sequence[Any]) -> None:
        self._rows = list(rows)

    def fetchall(self) -> list[Any]:
        return list(self._rows)


class _FakeRow:
    def __init__(self, **kwargs: Any) -> None:
        self.__dict__.update(kwargs)

    def __getitem__(self, index: int) -> Any:
        return list(self.__dict__.values())[index]


class _FakeConn:
    def __init__(self, handlers: dict[str, Sequence[Any]]) -> None:
        self._handlers = handlers

    def execute(self, sql: TextClause, params: dict[str, Any] | None = None) -> _FakeResult:
        sql_text = str(sql)
        for needle, rows in self._handlers.items():
            if needle in sql_text:
                return _FakeResult(rows)
        raise AssertionError(f"unexpected SQL: {sql_text} params={params}")


def test_compute_intel_flags_uses_conservative_title_and_permit_labels() -> None:
    auction = {
        "hcpa_market_value": 250000,
        "final_judgment_amount": 100000,
        "est_surviving_debt": 0,
        "net_equity": 150000,
        "liens_surviving": 0,
        "liens_uncertain": 0,
        "liens_total": 2,
        "open_permits": 1,
    }

    result = _compute_intel_flags(auction)
    tags = {flag["tag"] for flag in result["intel_flags"]}

    assert "OPEN PERMITS" in tags
    assert "NO KNOWN SURVIVING LIENS" in tags
    assert "ACTIVE PERMITS" not in tags
    assert "CLEAN TITLE" not in tags


def test_build_bank_escrow_profiles_excludes_unknown_counterparties() -> None:
    conn = _FakeConn(
        {
            'FROM "TrustAccount"': [
                _FakeRow(
                    plaintiff_name="Bank Alpha, N.A.",
                    case_number="24-CA-000001",
                    amount=10000.0,
                ),
                _FakeRow(
                    plaintiff_name="Bank Alpha, N.A.",
                    case_number="24-CA-000002",
                    amount=20000.0,
                ),
            ],
            "FROM foreclosures_history": [
                _FakeRow(
                    case_number_norm="24-CA-000001",
                    winning_bid=10000.0,
                    final_judgment_amount=50000.0,
                    auction_status="Sold",
                ),
                _FakeRow(
                    case_number_norm="24-CA-000002",
                    winning_bid=50000.0,
                    final_judgment_amount=55000.0,
                    auction_status="Sold",
                ),
            ],
            "SELECT case_number_norm FROM foreclosures": [
                ("24-CA-000001",),
                ("24-CA-000002",),
            ],
        }
    )

    profiles = _build_bank_escrow_profiles(conn)

    assert set(profiles) == {"BANK ALPHA"}
    profile = profiles["BANK ALPHA"]
    assert profile["total_cases"] == 2
    assert profile["median_deposit"] == 15000.0
    assert profile["bank_wins"] == 1
    assert profile["third_party_wins"] == 1
