"""
Save the 12 ORI documents for the TUSCANY property to the database.
Folio/PIN: 19272689C000000001980A
Address: 7870 TUSCANY WOODS DR, TAMPA, FL 33647
"""
from src.db.operations import PropertyDB
from datetime import datetime

# The 12 documents from the legal search for L 198 TUSCANY*
folio = '19272689C000000001980A'

documents = [
    {
        'instrument_number': '2024340948',
        'document_type': '(D) DEED',
        'recording_date': datetime(2024, 7, 18).date(),
        'book': '34020',
        'page': '1168',
        'party1': 'BICKNELL ROBERT S; WILSON DIANA L',
        'party2': 'BOAKYE ERNEST',
        'legal_description': 'L 198 TUSCANY SUBD AT TAMPA PALMS'
    },
    {
        'instrument_number': '2024340947',
        'document_type': '(MTG) MORTGAGE',
        'recording_date': datetime(2024, 7, 18).date(),
        'book': '34020',
        'page': '1155',
        'party1': 'BOAKYE ERNEST',
        'party2': 'MORTGAGE ELECTRONIC REGISTRATION SYSTEMS INC',
        'legal_description': 'L 198 TUSCANY SUBD AT TAMPA PALMS'
    },
    {
        'instrument_number': '2019204847',
        'document_type': '(LN) LIEN',
        'recording_date': datetime(2019, 4, 30).date(),
        'book': '29543',
        'page': '1737',
        'party1': 'BICKNELL ROBERT',
        'party2': 'TUSCANY AT TAMPA PALMS HOMEOWNERS ASSOCIATION INC',
        'legal_description': 'L 198 TUSCANY SUBD TAMPA PALMS'
    },
    {
        'instrument_number': '2018444946',
        'document_type': '(MTG) MORTGAGE',
        'recording_date': datetime(2018, 12, 7).date(),
        'book': '28917',
        'page': '1259',
        'party1': 'BICKNELL ROBERT S; WILSON DIANA L',
        'party2': 'MORTGAGE ELECTRONIC REGISTRATION SYSTEMS INC',
        'legal_description': 'L 198 TUSCANY SUBD AT TAMPA PALMS'
    },
    {
        'instrument_number': '2018444945',
        'document_type': '(D) DEED',
        'recording_date': datetime(2018, 12, 7).date(),
        'book': '28917',
        'page': '1256',
        'party1': 'ROLLISON DANA E; ROLLISON SHARON D',
        'party2': 'BICKNELL ROBERT S; WILSON DIANA L',
        'legal_description': 'L 198 TUSCANY SUBD AT TAMPA PALMS'
    },
    {
        'instrument_number': '2015290188',
        'document_type': '(D) DEED',
        'recording_date': datetime(2015, 7, 8).date(),
        'book': '26260',
        'page': '89',
        'party1': 'ROLLISON DANA E',
        'party2': 'ROLLISON DANA E; ROLLISON SHARON D',
        'legal_description': 'L 198 TUSCANY SUBD AT TAMPA PALMS'
    },
    {
        'instrument_number': '2011113109',
        'document_type': '(NOC) NOTICE OF COMMENCEMENT',
        'recording_date': datetime(2011, 3, 18).date(),
        'book': '23264',
        'page': '1348',
        'party1': 'ROLLISON DANA',
        'party2': 'POOLS BY BRADLEY INC',
        'legal_description': 'L 198 TUSCANY SUB AT TAMPA PALMS'
    },
    {
        'instrument_number': '2010434206',
        'document_type': '(AFF) AFFIDAVIT',
        'recording_date': datetime(2010, 12, 30).date(),
        'book': '23078',
        'page': '1505',
        'party1': 'ROLLISON DANA E; ROLLISON SHARON D',
        'party2': '',
        'legal_description': 'L 198 TUSCANY AT TAMPA PALMS'
    },
    {
        'instrument_number': '2010434205',
        'document_type': '(MTG) MORTGAGE',
        'recording_date': datetime(2010, 12, 30).date(),
        'book': '23078',
        'page': '1494',
        'party1': 'ROLLISON DANA E',
        'party2': 'MORTGAGE ELECTRONIC REGISTRATION SYSTEMS INC',
        'legal_description': 'L 198 TUSCANY AT TAMPA PALMS'
    },
    {
        'instrument_number': '2010434204',
        'document_type': '(D) DEED',
        'recording_date': datetime(2010, 12, 30).date(),
        'book': '23078',
        'page': '1490',
        'party1': 'STANDARD PACIFIC OF FLORIDA GP INC',
        'party2': 'ROLLISON DANA E',
        'legal_description': 'L 198 TUSCANY AT TAMPA PALMS'
    },
    {
        'instrument_number': '2010326174',
        'document_type': '(PR) PARTIAL RELEASE',
        'recording_date': datetime(2010, 9, 17).date(),
        'book': '22830',
        'page': '1102',
        'party1': 'JPMORGAN CHASE BANK NATIONAL ASSOCIATION',
        'party2': 'STANDARD PACIFIC OF FLORIDA GP',
        'legal_description': 'L 198 TUSCANY SUBD TAMPA PALMS'
    },
    {
        'instrument_number': '2018287696',
        'document_type': '(RELLP) RELEASE LIS PENDENS',
        'recording_date': datetime(2018, 7, 10).date(),
        'book': '28515',
        'page': '768',
        'party1': 'TUSCANY AT TAMPA PALMS HOMEOWNERS ASSOCIATION INC',
        'party2': 'ROLLISON DANA E; ROLLISON SHARON D',
        'legal_description': 'L 198 TUSCANY SUBD AT TAMPA PALMS'
    }
]

def main():
    db = PropertyDB()
    db.connect()

    # Check/create documents table
    try:
        result = db.conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'documents'").fetchall()
        print('Documents table columns:', [r[0] for r in result])
    except Exception as e:
        print(f'Creating documents table...')
        db.conn.execute('''
            CREATE TABLE IF NOT EXISTS documents (
                id INTEGER PRIMARY KEY,
                folio VARCHAR,
                case_number VARCHAR,
                document_type VARCHAR,
                file_path VARCHAR,
                ocr_text VARCHAR,
                extracted_data VARCHAR,
                recording_date DATE,
                book VARCHAR,
                page VARCHAR,
                instrument_number VARCHAR,
                party1 VARCHAR,
                party2 VARCHAR,
                legal_description VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        print('Created documents table')

    saved = 0
    for doc in documents:
        try:
            doc_id = db.save_document(folio, doc)
            print(f'Saved: {doc["document_type"]:35} | Inst: {doc["instrument_number"]} (ID: {doc_id})')
            saved += 1
        except Exception as e:
            print(f'Error saving {doc["instrument_number"]}: {e}')

    print(f'\nTotal saved: {saved} of {len(documents)} documents')

    # Verify saved documents
    count = db.conn.execute(f"SELECT COUNT(*) FROM documents WHERE folio = '{folio}'").fetchone()[0]
    print(f'Total documents in DB for this folio: {count}')

    # Show unique legal descriptions stored
    legals = db.conn.execute(f"SELECT DISTINCT legal_description FROM documents WHERE folio = '{folio}'").fetchall()
    print(f'\nUnique legal description variations stored:')
    for l in legals:
        print(f'  - {l[0]}')

    db.close()

if __name__ == "__main__":
    main()
