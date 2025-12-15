from src.utils.legal_description import build_ori_search_terms

def test_builder():
    raw_legal = "TOUCHSTONE PHASE 2 LOT 4 BLOCK 8"
    print(f"Input: {raw_legal}")
    
    terms = build_ori_search_terms(raw_legal)
    print("Generated Terms:")
    for t in terms:
        print(f"  - {t}")

if __name__ == "__main__":
    test_builder()
