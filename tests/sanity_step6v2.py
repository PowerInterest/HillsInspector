"""
Sanity test for Step 6 v2 Modular Architecture.
"""
from datetime import date
from src.services.lien_survival.survival_service import SurvivalService
from src.services.lien_survival import statutory_rules, priority_engine, joinder_validator

def test_statutory():
    print("Testing statutory rules...")
    assert statutory_rules.is_superpriority("TAX LIEN") is True
    assert statutory_rules.is_superpriority("MORTGAGE") is False
    
    expired, reason = statutory_rules.is_expired("MECHANIC LIEN", date(2020, 1, 1))
    assert expired is True
    assert "Mechanic" in reason
    print("Statutory rules passed.")

def test_priority():
    print("Testing priority engine...")
    target = {"recording_date": date(2010, 1, 1), "instrument": "101"}
    foreclosing = {"recording_date": date(2015, 1, 1), "instrument": "202"}
    
    seniority = priority_engine.determine_seniority(target, foreclosing)
    assert seniority == "SENIOR"
    
    junior = {"recording_date": date(2020, 1, 1)}
    seniority2 = priority_engine.determine_seniority(junior, foreclosing)
    assert seniority2 == "JUNIOR"
    print("Priority engine passed.")

def test_joinder():
    print("Testing joinder validator...")
    creditor = "WELLS FARGO BANK NA"
    defendants = ["WELLS FARGO BANK", "JOHN DOE", "JANE SMITH"]
    
    joined, name, score = joinder_validator.is_joined(creditor, defendants)
    assert joined is True
    assert name == "WELLS FARGO BANK"
    print("Joinder validator passed.")

def test_service():
    print("Testing survival service...")
    service = SurvivalService("TEST_PARCEL")
    
    encumbrances = [
        {"id": 1, "encumbrance_type": "MORTGAGE", "recording_date": date(2010, 1, 1), "creditor": "BANK A"},
        {"id": 2, "encumbrance_type": "JUDGMENT", "recording_date": date(2020, 1, 1), "creditor": "CITIZEN B"},
        {"id": 3, "encumbrance_type": "TAX LIEN", "recording_date": date(2023, 1, 1), "creditor": "COUNTY"}
    ]
    
    judgment_data = {
        "plaintiff": "BANK A",
        "foreclosure_type": "MORTGAGE",
        "lis_pendens_date": date(2022, 1, 1),
        "defendants": ["JOHN DOE", "CITIZEN B"],
        "foreclosing_refs": {"instrument": "M101"}
    }
    
    # Manually link foreclosing lien
    encumbrances[0]['instrument'] = "M101"
    
    analysis = service.analyze(encumbrances, judgment_data, [], None)
    
    results = analysis['results']
    assert len(results['foreclosing']) == 1
    assert len(results['extinguished']) == 1 # Citizen B was joined
    assert len(results['survived']) == 1 # Tax Lien
    print("Survival service passed.")

if __name__ == "__main__":
    try:
        test_statutory()
        test_priority()
        test_joinder()
        test_service()
        print("\nALL SANITY TESTS PASSED!")
    except Exception as e:
        print(f"\nTEST FAILED: {e}")
        import traceback
        traceback.print_exc()
