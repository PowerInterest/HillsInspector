from src.pipeline import PipelineDB

def check_analysis():
    db = PipelineDB()
    folio = '193206C07000000005160U'
    has_analysis = db.folio_has_survival_analysis(folio)
    print(f"Has Analysis: {has_analysis}")
    
    conn = db.connect()
    res = conn.execute("SELECT survival_status FROM encumbrances WHERE folio = ?", [folio]).fetchall()
    print("Statuses:", res)

    # Check last_case logic
    last_case = db.get_last_analyzed_case(folio)
    case_number = '292023CA015282A001HC'
    print(f"Last Case: {last_case}")
    print(f"Current Case: {case_number}")
    
    if has_analysis and last_case == case_number:
        print("PIPELINE WOULD SKIP")
    else:
        print("PIPELINE WOULD PROCESS")

if __name__ == "__main__":
    check_analysis()
