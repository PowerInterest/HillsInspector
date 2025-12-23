from typing import List, Dict, Optional, Tuple, Any
from datetime import UTC, datetime, date
from dataclasses import dataclass
from loguru import logger
from src.utils.time import now_utc
import re
import json

from src.services.institutional_names import get_searchable_party_name
from src.utils.name_matcher import NameMatcher


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

        transfer_docs = [d for d in sorted_docs if self._is_transfer_doc(get_doc_type(d))]

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
        
        # 3. Build Chain of Title (Anchor & Fill)
        support_docs_for_inference = list(sorted_docs)
        chain, gaps = self._build_title_chain(transfer_docs, support_docs_for_inference)

        # MRTA Check (30 years)
        mrta_status = "INSUFFICIENT"
        years_covered = 0
        if chain:
            oldest_date = self._parse_date(chain[0].get("date"))
            if oldest_date != datetime.min.replace(tzinfo=UTC):
                years_covered = (now_utc() - oldest_date).days / 365.25
                if years_covered >= 30:
                    mrta_status = "SATISFIED"
                else:
                    mrta_status = f"PARTIAL ({int(years_covered)} years)"

        # 4. Analyze Encumbrances (Mortgages & Liens)
        # Pass modifications (which includes assignments) to track creditor changes
        encumbrances = self._analyze_encumbrances(potential_encumbrances, satisfactions, modifications)

        ownership_timeline = self._chain_to_ownership_timeline(chain)

        return {
            'chain': chain,
            'ownership_timeline': ownership_timeline,
            'gaps': gaps,
            'encumbrances': encumbrances,
            'nocs': nocs,
            'modifications': modifications,
            'restrictions': restrictions,
            'tax_liens': [e for e in encumbrances if 'TAX' in str(e.get('type', '')).upper()],
            'mrta_status': mrta_status,
            'years_covered': round(years_covered, 1),
            'summary': {
                'total_deeds': len(transfer_docs),
                'active_liens': len([e for e in encumbrances if e['status'] == 'OPEN']),
                'total_nocs': len(nocs),
                'restrictions_count': len(restrictions),
                'gaps_found': len(gaps),
                'current_owner': ownership_timeline[-1]['owner'] if ownership_timeline else "Unknown",
                'mrta_status': mrta_status
            }
        }

    def _is_transfer_doc(self, doc_type: str) -> bool:
        """
        Determine if a document should be treated as a transfer "anchor" in the chain.
        """
        dt = (doc_type or "").upper()
        if not dt:
            return False

        # Exclusions (mortgage-like)
        if "DEED OF TRUST" in dt or "TRUST DEED" in dt:
            return False

        # Core anchors
        if "CERTIFICATE OF TITLE" in dt:
            return True
        if "TAX DEED" in dt:
            return True

        # Many transfer docs include "DEED" as a substring
        if "DEED" in dt:
            return True

        # Non-standard transfers / equitable title signals
        if "CONTRACT FOR DEED" in dt or ("AGREEMENT" in dt and "DEED" in dt):
            return True
        if "AGREEMENT FOR DEED" in dt or "AGD" in dt:
            return True
        if "PERSONAL REPRESENTATIVE" in dt and "DEED" in dt:
            return True
        
        # Probate transfers
        return (
            ("PROBATE" in dt or "SUMMARY ADMINISTRATION" in dt or "ORDER OF SUMMARY" in dt)
            and ("DEED" in dt or "ORDER" in dt or "ADMINISTRATION" in dt)
        )

    def _build_title_chain(
        self, transfer_docs: List[Dict], support_docs: List[Dict]
    ) -> Tuple[List[Dict], List[ChainGap]]:
        """
        Build a chain of ownership from transfer docs and fill gaps using support docs.

        Returns:
            Tuple of (chain entries, list of gaps found)
        """
        transfer_docs_sorted = sorted(
            transfer_docs, key=lambda d: self._parse_date(d.get("recording_date"))
        )
        support_docs_sorted = sorted(
            support_docs, key=lambda d: self._parse_date(d.get("recording_date"))
        )

        # If we have no transfer docs, infer a minimal chain from support docs.
        if not transfer_docs_sorted:
            inferred_chain = self._infer_chain_from_support_docs(support_docs_sorted)
            return inferred_chain, []

        deed_entries: List[Dict[str, Any]] = []
        for transfer in transfer_docs_sorted:
            doc_type = transfer.get("doc_type") or transfer.get("document_type", "")
            deed_entries.append(
                {
                    "date": transfer.get("recording_date"),
                    "grantor": (transfer.get("party1") or "").strip(),
                    "grantee": (transfer.get("party2") or "").strip(),
                    "doc_type": doc_type,
                    "book_page": f"{transfer.get('book')}/{transfer.get('page')}",
                    "instrument": transfer.get("instrument_number"),
                    "sales_price": transfer.get("sales_price"),
                    "link_status": None,
                    "confidence_score": None,
                    "notes": [],
                }
            )

        # Build a single best-linked chain path to avoid false breaks from unrelated deeds.
        deed_entries = self._select_best_deed_path(deed_entries)

        chain: List[Dict[str, Any]] = []
        gaps: List[ChainGap] = []

        for entry in deed_entries:
            entry["link_status"] = "VERIFIED" if not chain else None
            entry["confidence_score"] = 1.0 if not chain else None

            if chain:
                prev_owner = chain[-1].get("grantee") or ""
                curr_grantor = entry.get("grantor") or ""

                # If the previous deed is missing a grantee, we don't have an owner
                # anchor to compare against.
                if not prev_owner:
                    entry["link_status"] = "INCOMPLETE"
                    entry["confidence_score"] = 0.4
                    entry["notes"].append("Previous deed missing grantee; cannot verify link")
                    chain.append(entry)
                    continue

                # If the current deed is missing a grantor, we can't verify linkage.
                # Treat as an incomplete (but not broken) link and move on.
                if not curr_grantor:
                    entry["link_status"] = "INCOMPLETE"
                    entry["confidence_score"] = 0.4
                    entry["notes"].append("Missing grantor in indexed parties; cannot verify link")
                    chain.append(entry)
                    continue

                match_type, score = NameMatcher.match(prev_owner, curr_grantor)
                if match_type != "NONE" and score >= 0.8:
                    entry["link_status"] = "VERIFIED" if score >= 0.95 else "FUZZY"
                    entry["confidence_score"] = score
                else:
                    # Attempt to fill gap: infer that current grantor acquired via missing deed
                    implied = self._infer_owner_in_interval(
                        start=self._parse_date(chain[-1].get("date")),
                        end=self._parse_date(entry.get("date")),
                        support_docs=support_docs_sorted,
                        desired_owner=curr_grantor,
                    )
                    if implied:
                        implied_entry = {
                            "date": implied.get("date"),
                            "grantor": prev_owner,
                            "grantee": implied.get("owner"),
                            "doc_type": "IMPLIED",
                            "book_page": implied.get("book_page"),
                            "instrument": implied.get("instrument"),
                            "sales_price": None,
                            "link_status": "IMPLIED",
                            "confidence_score": implied.get("confidence", 0.7),
                            "notes": [
                                f"Implied ownership from support doc: {implied.get('source_type')} {implied.get('instrument') or implied.get('book_page')}".strip()
                            ],
                        }
                        chain.append(implied_entry)

                        grantee_val = implied_entry.get("grantee")
                        grantee_str = grantee_val if isinstance(grantee_val, str) else ""
                        match2, score2 = NameMatcher.match(grantee_str, curr_grantor)
                        if match2 != "NONE" and score2 >= 0.8:
                            entry["link_status"] = "VERIFIED" if score2 >= 0.95 else "FUZZY"
                            entry["confidence_score"] = score2
                        else:
                            entry["link_status"] = "INCOMPLETE"
                            entry["confidence_score"] = 0.3
                            entry["notes"].append(
                                f"GAP IN TITLE: previous owner {prev_owner} does not link to grantor {curr_grantor}"
                            )
                            self._append_gap(gaps, len(chain), prev_owner, curr_grantor, chain[-2], entry)
                    else:
                        entry["link_status"] = "INCOMPLETE"
                        entry["confidence_score"] = 0.3
                        entry["notes"].append(
                            f"GAP IN TITLE: previous owner {prev_owner} does not link to grantor {curr_grantor}"
                        )
                        self._append_gap(gaps, len(chain), prev_owner, curr_grantor, chain[-1], entry)

            chain.append(entry)

        # Post-loop: Check for implied ownership AFTER the last recorded deed (Tail Inference)
        # This handles cases where the last deed is old (e.g. 1978) but a new owner appears
        # in a mortgage later (e.g. 2001) without a recorded deed.
        if chain:
            last_entry = chain[-1]
            last_date = self._parse_date(last_entry.get("date"))
            last_owner = last_entry.get("grantee") or ""
            
            # Look for signals from last_date to NOW
            # (We use datetime.max as end date to scan everything after)
            tail_implied = self._infer_owner_in_interval(
                start=last_date,
                end=datetime.max.replace(tzinfo=UTC),
                support_docs=support_docs_sorted,
                desired_owner="" # We don't know who we're looking for, just ANY new owner
            )
            
            if tail_implied:
                new_owner = tail_implied.get("owner")
                # Only add if it's actually a NEW owner
                match_type, score = NameMatcher.match(last_owner, new_owner or "")
                if match_type == "NONE" and score < 0.8 and new_owner:
                    implied_entry = {
                        "date": tail_implied.get("date"),
                        "grantor": last_owner,
                        "grantee": new_owner,
                        "doc_type": "IMPLIED",
                        "book_page": tail_implied.get("book_page"),
                        "instrument": tail_implied.get("instrument"),
                        "sales_price": None,
                        "link_status": "IMPLIED",
                        "confidence_score": tail_implied.get("confidence", 0.6),
                        "notes": [
                            f"Implied ownership from support doc (Tail): {tail_implied.get('source_type')} {tail_implied.get('instrument') or tail_implied.get('book_page')}".strip()
                        ],
                    }
                    chain.append(implied_entry)

                    # Add gap tracking
                    self._append_gap(gaps, len(chain), last_owner, new_owner, last_entry, implied_entry)

        return chain, gaps

    def _select_best_deed_path(self, deed_entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Select a single best deed chain by walking backwards from the most recent deed.

        ORI searches can return unrelated deeds for the same subdivision/legal snippet.
        Including all deeds in strict chronological order creates false "broken" links.
        This method keeps only the best-linked path to the most recent owner.
        """
        if not deed_entries:
            return []

        entries = sorted(deed_entries, key=lambda d: self._parse_date(d.get("date")))
        anchor = entries[-1]
        used: set[int] = {id(anchor)}
        path_rev: List[Dict[str, Any]] = [anchor]
        current = anchor

        while True:
            current_date = self._parse_date(current.get("date"))
            current_grantor = (current.get("grantor") or "").strip()
            if not current_grantor or current_date == datetime.min.replace(tzinfo=UTC):
                break

            best: Dict[str, Any] | None = None
            best_score = 0.0
            best_date = datetime.min.replace(tzinfo=UTC)

            for cand in entries:
                if id(cand) in used:
                    continue
                cand_date = self._parse_date(cand.get("date"))
                if cand_date == datetime.min.replace(tzinfo=UTC) or cand_date >= current_date:
                    continue
                cand_grantee = (cand.get("grantee") or "").strip()
                if not cand_grantee:
                    continue

                match_type, score = NameMatcher.match(cand_grantee, current_grantor)
                if match_type == "NONE" or score < 0.8:
                    continue

                if score > best_score or (score == best_score and cand_date > best_date):
                    best = cand
                    best_score = score
                    best_date = cand_date

            if not best:
                break

            used.add(id(best))
            path_rev.append(best)
            current = best

        return list(reversed(path_rev))

    def _append_gap(
        self,
        gaps: List[ChainGap],
        position: int,
        prev_owner: str,
        curr_grantor: str,
        prev_entry: Dict[str, Any],
        curr_entry: Dict[str, Any],
    ) -> None:
        searchable_names: List[str] = []
        for nm in (prev_owner, curr_grantor):
            clean = get_searchable_party_name(nm)
            if clean and clean not in searchable_names:
                searchable_names.append(clean)

        if searchable_names:
            gaps.append(
                ChainGap(
                    position=position,
                    prev_grantee=prev_owner,
                    curr_grantor=curr_grantor,
                    prev_date=prev_entry.get("date"),
                    curr_date=curr_entry.get("date"),
                    searchable_names=searchable_names,
                )
            )

    def _infer_owner_in_interval(
        self,
        start: datetime,
        end: datetime,
        support_docs: List[Dict],
        desired_owner: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Find the most plausible owner signal in (start, end) from support docs.

        Returns dict with keys: owner, date, instrument, book_page, source_type, confidence.
        """
        if start == datetime.min.replace(tzinfo=UTC) or end == datetime.min.replace(tzinfo=UTC):
            return None
        if end <= start:
            return None

        # Collect candidates inside interval.
        candidates: List[Dict[str, Any]] = []
        for d in support_docs:
            d_date = self._parse_date(d.get("recording_date"))
            if d_date <= start or d_date >= end:
                continue
            owner = self._owner_candidate_from_support_doc(d)
            if not owner:
                continue
            candidates.append(
                {
                    "owner": owner,
                    "date": d.get("recording_date"),
                    "instrument": d.get("instrument_number"),
                    "book_page": f"{d.get('book')}/{d.get('page')}",
                    "source_type": d.get("doc_type") or d.get("document_type"),
                }
            )

        if not candidates:
            return None

        # Prefer a candidate that matches the deed grantor (strongest evidence).
        for c in candidates:
            match_type, score = NameMatcher.match(c["owner"], desired_owner)
            if match_type != "NONE" and score >= 0.8:
                c["confidence"] = max(0.7, score)
                return c

        # Otherwise, take the most frequent owner candidate (by normalized tokens).
        buckets: Dict[str, List[Dict[str, Any]]] = {}
        for c in candidates:
            # Use NameMatcher.normalize to get a stable key
            key = " ".join(sorted(NameMatcher.normalize(c["owner"])))
            buckets.setdefault(key, []).append(c)

        best_key = max(buckets, key=lambda k: len(buckets[k]))
        best = sorted(buckets[best_key], key=lambda x: self._parse_date(x["date"]))[0]
        best["confidence"] = 0.6
        return best

    def _infer_chain_from_support_docs(self, support_docs: List[Dict]) -> List[Dict[str, Any]]:
        """
        Fallback when we have documents but no transfer anchors.

        Build a minimal, low-confidence chain using ownership signals (mortgage/NOC/etc).
        """
        inferred: List[Dict[str, Any]] = []
        last_owner: Optional[str] = None

        for d in support_docs:
            owner = self._owner_candidate_from_support_doc(d)
            if not owner:
                continue

            if last_owner:
                match_type, score = NameMatcher.match(last_owner, owner)
                if match_type != "NONE" and score >= 0.85:
                    continue

            inferred.append(
                {
                    "date": d.get("recording_date"),
                    "grantor": last_owner or "",
                    "grantee": owner,
                    "doc_type": "INFERRED",
                    "book_page": f"{d.get('book')}/{d.get('page')}",
                    "instrument": d.get("instrument_number"),
                    "sales_price": None,
                    "link_status": "IMPLIED",
                    "confidence_score": 0.55 if last_owner else 0.6,
                    "notes": [
                        f"Inferred owner from support doc: {(d.get('doc_type') or d.get('document_type') or '').strip()}"
                    ],
                }
            )
            last_owner = owner

        if not inferred and support_docs:
            # We have documents but no reliable ownership signals; create a placeholder segment
            # so the caller can avoid "missing chain" records.
            first = support_docs[0]
            inferred.append(
                {
                    "date": first.get("recording_date"),
                    "grantor": "",
                    "grantee": "Unknown (No Ownership Signals)",
                    "doc_type": "INFERRED",
                    "book_page": f"{first.get('book')}/{first.get('page')}",
                    "instrument": first.get("instrument_number"),
                    "sales_price": None,
                    "link_status": "INFERRED",
                    "confidence_score": 0.0,
                    "notes": [
                        "No transfer docs and no owner-signature support docs found; placeholder chain segment.",
                    ],
                }
            )

        return inferred

    def _owner_candidate_from_support_doc(self, doc: Dict) -> Optional[str]:
        """
        Extract the best-guess owner name from a non-transfer document.

        This is used only for *inference*; it should be conservative.
        """
        doc_type = (doc.get("doc_type") or doc.get("document_type") or "").upper()
        if not doc_type:
            return None

        party1 = (doc.get("party1") or "").strip()
        party2 = (doc.get("party2") or "").strip()

        # Mortgages: owner/borrower is typically party1
        if "MORTGAGE" in doc_type or doc_type.startswith("(MTG)") or "HELOC" in doc_type:
            return party1 or None

        # NOC: signed by owner (often party1)
        if "NOTICE OF COMMENCEMENT" in doc_type or "NOC" in doc_type:
            return party1 or None

        # Lis Pendens: defendant is typically party2
        if "LIS PENDENS" in doc_type:
            return party2 or None

        # HOA/COA liens: debtor/owner is often party2; claimant is party1
        if "HOA" in doc_type or "ASSOCIATION" in doc_type or "CONDO" in doc_type:
            return party2 or None

        # Generic liens/judgments: debtor is often party2
        if "LIEN" in doc_type or "JUDGMENT" in doc_type:
            return party2 or None

        # Affidavits are often filed by/for owners; treat as a weak ownership signal.
        if "AFFIDAVIT" in doc_type:
            return party1 or None

        return None

    def _chain_to_ownership_timeline(self, chain: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        timeline: List[Dict[str, Any]] = []
        for i, entry in enumerate(chain):
            next_date = chain[i + 1].get("date") if i < len(chain) - 1 else None
            timeline.append(
                {
                    "owner": entry.get("grantee") or "Unknown",
                    "acquired_from": entry.get("grantor") or None,
                    "acquisition_date": entry.get("date"),
                    "disposition_date": next_date,
                    "acquisition_instrument": entry.get("instrument"),
                    "acquisition_doc_type": entry.get("doc_type"),
                    "acquisition_price": entry.get("sales_price"),
                    "link_status": entry.get("link_status"),
                    "confidence_score": entry.get("confidence_score"),
                }
            )
        return timeline

    def _extract_refs(self, text: str) -> Tuple[List[Tuple[str, str]], List[str]]:
        """Extract Book/Page and Instrument pairs from text."""
        if not text:
            return [], []
        bk_pgs = self.bk_pg_regex.findall(text)
        insts = self.inst_regex.findall(text)
        return bk_pgs, insts

    def _analyze_encumbrances(
        self,
        encumbrances: List[Dict],
        satisfactions: List[Dict],
        assignments: List[Dict] | None = None,
    ) -> List[Dict]:
        """
        Determine which encumbrances are still active, tracking assignments.
        """
        results = []
        assignments = assignments or []
        
        # 1. Initialize Encumbrance Objects
        # We wrap them in a mutable dict to track current status and creditor
        active_map = {} # Key: Instrument -> Encumbrance Obj
        
        for enc in encumbrances:
            enc_type = enc.get('doc_type') or enc.get('document_type', '')
            
            # Determine initial Creditor/Debtor
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
                    ext_data = enc.get('vision_extracted_data') or enc.get('extracted_data')
                    if ext_data:
                        if isinstance(ext_data, str): ext_data = json.loads(ext_data)
                        amount_val = (
                            ext_data.get('amount') or 
                            ext_data.get('total_judgment_amount') or 
                            ext_data.get('principal_amount') or 
                            ext_data.get('sales_price')
                        )
                except Exception as exc:
                    logger.debug(f"Failed to parse encumbrance amount: {exc}")

            instrument = str(enc.get('instrument_number', '')).strip()
            book = str(enc.get('book', '')).strip()
            page = str(enc.get('page', '')).strip()
            
            enc_obj = {
                'type': enc_type,
                'date': enc.get('recording_date'),
                'amount': amount_val or 0.0,
                'original_creditor': enc_lender,
                'current_creditor': enc_lender, # Will be updated by assignments
                'debtor': enc_debtor,
                'book_page': f"{book}/{page}",
                'instrument': instrument,
                'book': book,
                'page': page,
                'status': 'OPEN',
                'satisfaction_ref': None,
                'match_method': None,
                'assignments': []
            }
            results.append(enc_obj)
            
            # Map for quick lookup
            if instrument:
                active_map[instrument] = enc_obj
            if book and page:
                active_map[f"{book}/{page}"] = enc_obj

        # 2. Process Assignments & Satisfactions Chronologically
        # Combine and sort by date
        events = []
        for a in assignments:
            events.append({**a, '_event_type': 'ASSIGNMENT'})
        for s in satisfactions:
            events.append({**s, '_event_type': 'SATISFACTION'})
            
        events.sort(key=lambda x: self._parse_date(x.get('recording_date')))

        for event in events:
            event_date = self._parse_date(event.get('recording_date'))
            event_text = (event.get('legal_description') or '') + " " + (event.get('notes') or '')
            
            # Find target encumbrance
            target_enc = None
            
            # A. Check Specific References
            ref_bk_pgs, ref_insts = self._extract_refs(event_text)
            
            # Check Instrument match
            for ref_inst in ref_insts:
                if ref_inst in active_map:
                    target_enc = active_map[ref_inst]
                    break
            
            # Check Book/Page match
            if not target_enc:
                for ref_bk, ref_pg in ref_bk_pgs:
                    key = f"{ref_bk}/{ref_pg}"
                    if key in active_map:
                        target_enc = active_map[key]
                        break
            
            # B. Check Name Match (Fallback for Satisfactions only)
            # Only if we haven't found a target yet
            if not target_enc and event['_event_type'] == 'SATISFACTION':
                releasor = event.get('party1', '').strip()
                # Iterate all OPEN encumbrances to find a match
                # This is O(N*M) but N is small
                for enc in results:
                    if enc['status'] == 'OPEN' and NameMatcher.are_linked(
                        enc['current_creditor'],
                        releasor,
                        threshold=0.85,
                    ):
                        target_enc = enc
                        enc['match_method'] = 'NAME_MATCH'
                        break

            # Apply Event
            if target_enc:
                # Ignore events recorded BEFORE the encumbrance (bad data/OCR)
                enc_dt = self._parse_date(target_enc['date'])
                if event_date < enc_dt:
                    continue

                if event['_event_type'] == 'ASSIGNMENT':
                    # Update Creditor
                    # Assignments: Party 1 = Assignor (Old), Party 2 = Assignee (New)
                    new_creditor = event.get('party2', '').strip()
                    if new_creditor:
                        target_enc['current_creditor'] = new_creditor
                        target_enc['assignments'].append({
                            'date': event.get('recording_date'),
                            'assignee': new_creditor,
                            'instrument': event.get('instrument_number')
                        })
                        
                elif event['_event_type'] == 'SATISFACTION':
                    # Mark Satisfied
                    target_enc['status'] = 'SATISFIED'
                    target_enc['satisfaction_ref'] = event.get('instrument_number')
                    if not target_enc['match_method']:
                        target_enc['match_method'] = 'REF_MATCH'

        # Remap output to expected format
        final_output = []
        for enc in results:
            final_output.append({
                'type': enc['type'],
                'date': enc['date'],
                'amount': enc['amount'],
                'creditor': enc['current_creditor'], # Return the UPDATED creditor
                'original_creditor': enc['original_creditor'],
                'debtor': enc['debtor'],
                'book_page': enc['book_page'],
                'instrument': enc['instrument'],
                'status': enc['status'],
                'satisfaction_ref': enc['satisfaction_ref'],
                'match_method': enc['match_method'],
                'assignments': enc['assignments']
            })
            
        return final_output

    def _parse_date(self, date_val: Any) -> datetime:
        if not date_val:
            return datetime.min.replace(tzinfo=UTC)
        if isinstance(date_val, datetime):
            return date_val.replace(tzinfo=UTC) if date_val.tzinfo is None else date_val
        if isinstance(date_val, date):
            return datetime.combine(date_val, datetime.min.time()).replace(tzinfo=UTC)
        if hasattr(date_val, 'timetuple'): # duck typing for date
             return datetime.combine(date_val, datetime.min.time()).replace(tzinfo=UTC)
            
        try:
            dt = datetime.strptime(str(date_val), "%Y-%m-%d")
            return dt.replace(tzinfo=UTC)
        except ValueError:
            try:
                # Fallback for old format or user entered
                dt = datetime.strptime(str(date_val), "%m/%d/%Y")
                return dt.replace(tzinfo=UTC)
            except ValueError:
                return datetime.min.replace(tzinfo=UTC)
