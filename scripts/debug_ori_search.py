from src.scrapers.ori_api_scraper import ORIApiScraper

def test_search():
    scraper = ORIApiScraper()
    
    # Target: TOUCHSTONE PHASE 2 LOT 4 BLOCK 8
    # Actual legal from bulk: "TOUCHSTONE PHASE 2 LOT 4 BLOCK 8"
    
    # 1. Test what search terms are generated
    raw_legal = "TOUCHSTONE PHASE 2 LOT 4 BLOCK 8"
    print(f"\n--- Testing Search Term Generation for: '{raw_legal}' ---")
    
    legal_parts = raw_legal.upper().split()
    subdivision_parts = []
    for _i, part in enumerate(legal_parts):
        if part in ("LOT", "BLOCK", "UNIT", "PH", "PHASE"):
            break
        subdivision_parts.append(part)
        
    sub_name = " ".join(subdivision_parts)
    print(f"Extracted Subdivision: '{sub_name}'")
    
    # 2. Test actual API calls
    print("\n--- Testing API Searches ---")
    
    terms_to_test = [
        "TOUCHSTONE PHASE 2",
        "TOUCHSTONE*",
        "TOUCHSTONE PHASE*",
        "TOUCHSTONE PH 2",
        "TOUCHSTONE",  # Too broad?
        "L 4 B 8 TOUCHSTONE*"
    ]
    
    if sub_name:
        terms_to_test.insert(0, sub_name)
    
    for term in terms_to_test:
        if not term: continue
        print(f"Searching for: '{term}'...")
        try:
            results = scraper.search_by_legal(term)
            print(f"  Result count: {len(results)}")
            if results:
                print(f"  First result: {results[0].get('Legal')}")
        except Exception as e:
            print(f"  Error: {e}")

if __name__ == "__main__":
    test_search()
