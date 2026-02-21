from app.web.routers.properties import _summarize_encumbrances


def test_summarize_encumbrances_counts_surviving_and_uncertain_risk() -> None:
    encumbrances = [
        {"survival_status": "SURVIVED", "amount": 100000, "is_satisfied": False},
        {"survival_status": "uncertain", "amount": None, "is_satisfied": False},
        {"survival_status": "EXTINGUISHED", "amount": 25000, "is_satisfied": False},
        {"survival_status": "SURVIVED", "amount": 40000, "is_satisfied": True},
    ]

    summary = _summarize_encumbrances(encumbrances)

    assert summary["liens_total"] == 3
    assert summary["liens_survived"] == 1
    assert summary["liens_uncertain"] == 1
    assert summary["liens_surviving"] == 2
    assert summary["liens_total_amount"] == 100000.0
    assert summary["surviving_unknown_amount"] == 1
