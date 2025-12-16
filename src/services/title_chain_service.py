from typing import List, Dict, Optional, Tuple, Any
from datetime import UTC, datetime
from dataclasses import dataclass
import re
import json

from src.services.institutional_names import get_searchable_party_name


@dataclass
class ChainGap:
    """Represents a gap in the chain of title that may need party name search."""
    position: int  # Position in chain (index of deed AFTER the gap)
    prev_grantee: str  # Expected grantor (previous owner)
    curr_grantor: str  # Actual grantor on the deed
    prev_date: Optional[str]  # Date of previous deed
    curr_date: Optional[str]  # Date of current deed
    searchable_names: List[str]  # Non-institutional names that can be searched


class TitleChainService:
    """
    Service to analyze a list of official records and build a Chain of Title
    and identify surviving encumbrances.
    """
    
    def __init__(self):
        # Regex for finding Book/Page references
        # Matches: "Book 123 Page 456", "Bk 123 Pg 456", "B: 123 P: 456"
        self.bk_pg_regex = re.compile(r'(?:BK|B|BOOK)\W+(\d+)\W+(?:PG|P|PAGE)\W+(\d+)', re.IGNORECASE)
        # Matches: "Instrument 2020123456", "Inst # 2020123456"
        self.inst_regex = re.compile(r'(?:INST|INSTRUMENT)\W+(?:NO|#)?\W*(\d+)', re.IGNORECASE)

    def build_chain_and_analyze(self, documents: List[Dict]) -> Dict:
        """
        Main entry point. Takes raw documents and returns analysis.
        """
        # 1. Sort documents by date (oldest to newest)
        sorted_docs = sorted(documents, key=lambda x: self._parse_date(x.get('recording_date', '')))

        # 2. Separate into categories
        # Support both 'doc_type' (raw ORI) and 'document_type' (mapped schema)
        def get_doc_type(d):
            return (d.get('doc_type') or d.get('document_type', '')).upper()

        deeds = [d for d in sorted_docs if 'DEED' in get_doc_type(d) or 'CERTIFICATE OF TITLE' in get_doc_type(d)]

        # Encumbrances: Mortgages, Liens, Judgments, Lis Pendens
        # Exclude Satisfactions from this list initially
        encumbrance_keywords = ['MORTGAGE', 'LIEN', 'JUDGMENT', 'LIS PENDENS', 'TAX']
        satisfaction_keywords = ['SATISFACTION', 'RELEASE', 'RECONVEYANCE', 'DISCHARGE']
        # Partial releases should NOT count as full satisfaction
        partial_keywords = ['PARTIAL']
        modification_keywords = ['MODIFICATION', 'ASSIGNMENT', 'AMENDMENT']
        restriction_keywords = ['EASEMENT', 'RESTRICTION', 'COVENANT', 'DECLARATION', 'PLAT']

        potential_encumbrances = []
        satisfactions = []
        nocs = []
        modifications = []
        restrictions = []

        for d in sorted_docs:
            doc_type = get_doc_type(d)
            text_blob = " ".join([
                str(d.get('legal_description', '')),
                str(d.get('ocr_text', '')),
                str(d.get('notes', ''))
            ]).upper()
            
            if 'NOTICE OF COMMENCEMENT' in doc_type or 'NOC' in doc_type:
                nocs.append(d)
            elif any(k in doc_type for k in partial_keywords):
                # Treat partials as modifications/notes, NOT full satisfactions
                modifications.append(d)
            elif any(k in doc_type for k in satisfaction_keywords):
                satisfactions.append(d)
            elif any(k in doc_type for k in modification_keywords):
                modifications.append(d)
            elif any(k in doc_type for k in restriction_keywords) or any(k in text_blob for k in restriction_keywords):
                restrictions.append(d)
            elif any(k in doc_type for k in encumbrance_keywords):
                potential_encumbrances.append(d)
        
        # 3. Build Chain of Title
        chain, gaps = self._build_deed_chain(deeds)

        # 4. Analyze Encumbrances (Mortgages & Liens)
        encumbrances = self._analyze_encumbrances(potential_encumbrances, satisfactions)

        return {
            'chain': chain,
            'gaps': gaps,  # List of ChainGap objects for gap-filling
            'encumbrances': encumbrances,
            'nocs': nocs,
            'modifications': modifications,
            'restrictions': restrictions,
            'tax_liens': [e for e in encumbrances if 'TAX' in str(e.get('type', '')).upper()],
            'summary': {
                'total_deeds': len(deeds),
                'active_liens': len([e for e in encumbrances if e['status'] == 'OPEN']),
                'total_nocs': len(nocs),
                'restrictions_count': len(restrictions),
                'gaps_found': len(gaps),
                'current_owner': chain[-1]['grantee'] if chain else "Unknown"
            }
        }

    def _build_deed_chain(self, deeds: List[Dict]) -> Tuple[List[Dict], List[ChainGap]]:
        """
        Link deeds together to form a chain of ownership.

        Returns:
            Tuple of (chain entries, list of gaps found)
        """
        chain = []
        gaps = []

        for i, deed in enumerate(deeds):
            # Support both field naming conventions
            doc_type = deed.get('doc_type') or deed.get('document_type', '')
            entry = {
                'date': deed.get('recording_date'),
                'grantor': deed.get('party1', '').strip(),  # Usually Grantor
                'grantee': deed.get('party2', '').strip(),  # Usually Grantee
                'doc_type': doc_type,
                'book_page': f"{deed.get('book')}/{deed.get('page')}",
                'instrument': deed.get('instrument_number'),
                'notes': []
            }

            # Check for gaps
            if i > 0:
                prev_grantee = chain[-1]['grantee']
                curr_grantor = entry['grantor']

                # Simple fuzzy check (names can be messy)
                if not self._names_match(prev_grantee, curr_grantor):
                    entry['notes'].append(f"GAP IN TITLE? Previous owner was {prev_grantee}, but Grantor is {curr_grantor}")

                    # Build list of searchable names (non-institutional)
                    searchable_names = []

                    # Try prev_grantee (the expected grantor)
                    clean_name = get_searchable_party_name(prev_grantee)
                    if clean_name:
                        searchable_names.append(clean_name)

                    # Try curr_grantor (actual grantor - might find linking docs)
                    clean_name = get_searchable_party_name(curr_grantor)
                    if clean_name and clean_name not in searchable_names:
                        searchable_names.append(clean_name)

                    if searchable_names:
                        gaps.append(ChainGap(
                            position=i,
                            prev_grantee=prev_grantee,
                            curr_grantor=curr_grantor,
                            prev_date=chain[-1].get('date'),
                            curr_date=entry.get('date'),
                            searchable_names=searchable_names
                        ))

            chain.append(entry)
        return chain, gaps

    def _extract_refs(self, text: str) -> Tuple[List[Tuple[str, str]], List[str]]:
        """Extract Book/Page and Instrument pairs from text."""
        if not text:
            return [], []
        bk_pgs = self.bk_pg_regex.findall(text)
        insts = self.inst_regex.findall(text)
        return bk_pgs, insts

    def _analyze_encumbrances(self, encumbrances: List[Dict], satisfactions: List[Dict]) -> List[Dict]:
        """
        Determine which encumbrances are still active.
        """
        results = []
        
        for enc in encumbrances:
            status = 'OPEN'
            matched_satisfaction = None
            match_method = None
            
            enc_date = self._parse_date(enc.get('recording_date'))
            enc_book = str(enc.get('book', '')).strip()
            enc_page = str(enc.get('page', '')).strip()
            enc_inst = str(enc.get('instrument_number', '')).strip()
            enc_type = enc.get('doc_type') or enc.get('document_type', '')
            
            # Determine Creditor/Debtor based on Document Type
            # Mortgages: Party 1 = Borrower (Debtor), Party 2 = Lender (Creditor)
            # Liens/LP/Judgments: Party 1 = Claimant (Creditor), Party 2 = Debtor (Owner)
            is_mortgage = 'MORTGAGE' in str(enc_type).upper() or 'MTG' in str(enc_type).upper()
            
            if is_mortgage:
                enc_lender = enc.get('party2', '').strip() 
                enc_debtor = enc.get('party1', '').strip()
            else:
                enc_lender = enc.get('party1', '').strip()
                enc_debtor = enc.get('party2', '').strip()

            # Extract amount from JSON if not at top level
            amount_val = enc.get('amount')
            if not amount_val or amount_val == 'Unknown':
                try:
                    # Check both fields: 'extracted_data' (from DB) or 'vision_extracted_data' (in-memory)
                    ext_data = enc.get('vision_extracted_data') or enc.get('extracted_data')
                    
                    if ext_data:
                        if isinstance(ext_data, str):
                            ext_data = json.loads(ext_data)
                        
                        # Try common amount fields
                        amount_val = (
                            ext_data.get('amount') or 
                            ext_data.get('total_judgment_amount') or 
                            ext_data.get('principal_amount') or 
                            ext_data.get('sales_price')
                        )
                except Exception:
                    pass

            for sat in satisfactions:
                sat_date = self._parse_date(sat.get('recording_date'))
                if sat_date <= enc_date:
                    continue
                
                # 1. Check Specific Reference (Book/Page or Instrument)
                # Check legal description or notes for references
                sat_text = (sat.get('legal_description') or '') + " " + (sat.get('notes') or '')
                ref_bk_pgs, ref_insts = self._extract_refs(sat_text)
                
                # Check Book/Page match
                if enc_book and enc_page:
                    for ref_bk, ref_pg in ref_bk_pgs:
                        if ref_bk == enc_book and ref_pg == enc_page:
                            status = 'SATISFIED'
                            matched_satisfaction = sat
                            match_method = 'BOOK_PAGE_REF'
                            break
                
                if status == 'SATISFIED': break

                # Check Instrument match
                if enc_inst and enc_inst in ref_insts:
                    status = 'SATISFIED'
                    matched_satisfaction = sat
                    match_method = 'INSTRUMENT_REF'
                    break
                
                # 2. Fallback: Check Names + Logic
                # In a Satisfaction, Party 1 is usually the Bank/Lienor (releasing)
                sat_releasor = sat.get('party1', '').strip()
                
                if self._names_match(enc_lender, sat_releasor):
                    # Only match if we haven't found a specific ref yet
                    # And maybe check amounts if available? (TODO)
                    status = 'SATISFIED'
                    matched_satisfaction = sat
                    match_method = 'NAME_MATCH'
                    break
            
            # Support both field naming conventions
            results.append({
                'type': enc_type,
                'date': enc.get('recording_date'),
                'amount': amount_val or 0.0,
                'creditor': enc_lender,
                'debtor': enc_debtor,
                'book_page': f"{enc.get('book')}/{enc.get('page')}",
                'status': status,
                'satisfaction_ref': f"{matched_satisfaction.get('book')}/{matched_satisfaction.get('page')}" if matched_satisfaction else None,
                'match_method': match_method
            })
            
        return results

    def _names_match(self, name1: str, name2: str) -> bool:
        """
        Fuzzy name matching.
        """
        if not name1 or not name2:
            return False
            
        # Tokenize and clean
        def clean_tokens(n):
            return set(n.upper().replace(',', '').replace('.', '').split())
            
        t1 = clean_tokens(name1)
        t2 = clean_tokens(name2)
        
        # Intersection of tokens
        common = t1.intersection(t2)
        
        # If they share significant words (ignoring common stopwords like LLC, INC, BANK)
        # If they share significant words (ignoring common stopwords like LLC, INC, BANK)
        stopwords = {
            'LLC', 'INC', 'CORP', 'COMPANY', 'BANK', 'NA', 'ASSOCIATION', 'THE', 'OF', 'AND',
            'SECRETARY', 'DEPARTMENT', 'HOUSING', 'URBAN', 'DEVELOPMENT', 'USA', 'UNITED', 'STATES',
            'TRUST', 'FSB', 'NATIONAL', 'SYSTEMS', 'ELECTRONIC', 'REGISTRATION', 'FINANCIAL',
            'GROUP', 'HOLDINGS', 'LTD', 'LP', 'PA', 'PARTNERS'
        }
        significant_common = common - stopwords
        
        # If they share at least one significant word (e.g. "WELLS" in "WELLS FARGO")
        # This is loose, but better than just first word.
        return bool(significant_common)

    def _parse_date(self, date_val: Any) -> datetime:
        if not date_val:
            return datetime.min.replace(tzinfo=UTC)
        if isinstance(date_val, datetime):
            return date_val
        if hasattr(date_val, 'timetuple'): # duck typing for date
             return datetime.combine(date_val, datetime.min.time()).replace(tzinfo=UTC)
            
        try:
            return datetime.strptime(str(date_val), "%Y-%m-%d")
        except ValueError:
            try:
                # Fallback for old format or user entered
                return datetime.strptime(str(date_val), "%m/%d/%Y")
            except ValueError:
                return datetime.min.replace(tzinfo=UTC)
