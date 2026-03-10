"""Triage existing final judgment extractions for data quality.

Three layers of detection, strongest first:

1. **Internal consistency** — Do the amounts add up?  Does the case number
   match the directory?  Is the judgment date before the sale date?  Does not
   need any external data — the extraction must be self-consistent.

2. **Property identity** — Does the extracted subdivision/lot/block/address
   match the HCPA parcel record for this strap?  This is the strongest
   external cross-check because HCPA data is independently sourced (county
   appraiser, not the extraction pipeline).

3. **Cross-document consistency** — Does the judgment plaintiff match the ORI
   lis pendens party?  Does the foreclosed mortgage instrument number match
   an ORI encumbrance?  Uses PG but compares across independently-sourced
   documents, not against data we stored from the same extraction.

Output: ranked list, worst to best, with specific failure reasons.
"""

import json
import re
import sys
from pathlib import Path

import psycopg
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.models.extraction_base import normalize_party_name
from src.models.judgment_extraction import (
    JudgmentExtraction,
    extract_credit_adjustments,
)

PG_DSN = "host=localhost port=5433 dbname=hills_sunbiz user=hills password=hills_dev"
DATA_ROOT = Path("data/Foreclosure")


# ---------------------------------------------------------------------------
# PG data loaders — independent sources only
# ---------------------------------------------------------------------------

def load_hcpa_parcels(conn) -> dict[str, dict]:
    """HCPA parcel data keyed by strap.  Independent of extraction pipeline."""
    rows = conn.execute("""
        SELECT strap, folio, property_address, raw_sub,
               raw_legal1, raw_legal2, raw_legal3, raw_legal4, owner_name
        FROM hcpa_bulk_parcels
    """).fetchall()
    parcels = {}
    for r in rows:
        legal = " ".join(filter(None, [r[4], r[5], r[6], r[7]])).strip()
        parcels[r[0]] = {
            "strap": r[0],
            "folio": r[1],
            "address": (r[2] or "").strip(),
            "address_key": re.sub(r"[^A-Z0-9]", "", (r[2] or "").upper()),
            "raw_sub": (r[3] or "").strip(),
            "legal": legal,
            "owner": (r[8] or "").strip(),
        }
    return parcels


def load_ori_data(conn) -> dict[str, list[dict]]:
    """ORI encumbrances keyed by strap.  Independent of extraction pipeline."""
    rows = conn.execute("""
        SELECT strap, instrument_number, book, page, encumbrance_type,
               party1, party2, current_holder, raw_document_type
        FROM ori_encumbrances
        WHERE strap IS NOT NULL
    """).fetchall()
    by_strap: dict[str, list[dict]] = {}
    for r in rows:
        by_strap.setdefault(r[0], []).append({
            "instrument": r[1],
            "book": r[2],
            "page": r[3],
            "type": r[4],
            "party1": r[5],
            "party2": r[6],
            "holder": r[7],
            "doc_type": r[8],
        })
    return by_strap


def load_foreclosure_index(conn) -> dict[str, dict]:
    """Foreclosure case index — case_number_raw → strap/folio mapping."""
    rows = conn.execute("""
        SELECT case_number_raw, case_number_norm, strap, folio,
               property_address, auction_date, archived_at
        FROM foreclosures
    """).fetchall()
    index = {}
    for r in rows:
        index[r[0]] = {
            "case_number_raw": r[0],
            "case_number_norm": r[1],
            "strap": r[2],
            "folio": r[3],
            "address": (r[4] or "").strip(),
            "auction_date": str(r[5]) if r[5] else None,
            "archived": r[6] is not None,
        }
    return index


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_sub(s: str | None) -> str:
    """Normalize subdivision name for comparison."""
    if not s:
        return ""
    return re.sub(r"[^A-Z0-9]", "", s.upper())


def address_tokens(s: str | None) -> set[str]:
    """Extract significant tokens from an address for comparison."""
    if not s:
        return set()
    upper = s.upper()
    # Remove unit/apt/suite qualifiers
    upper = re.sub(r"\b(APT|UNIT|STE|SUITE|#)\s*\S*", "", upper)
    # Keep only alphanum tokens, drop FL/FLORIDA/TAMPA common words
    tokens = set(re.findall(r"[A-Z0-9]+", upper))
    tokens -= {"FL", "FLORIDA", "TAMPA", "HILLSBOROUGH", "ST", "AVE",
               "DR", "LN", "CT", "CIR", "BLVD", "RD", "PL", "WAY"}
    return tokens


def normalize_address_key(s: str | None) -> str:
    if not s:
        return ""
    return re.sub(r"[^A-Z0-9]", "", s.upper())


def normalize_free_text(s: str | None) -> str:
    if not s:
        return ""
    return re.sub(r"[^A-Z0-9]+", " ", s.upper()).strip()


def raw_text_contains(raw_text: str | None, phrase: str | None) -> bool:
    if not raw_text or not phrase:
        return False
    return normalize_free_text(phrase) in normalize_free_text(raw_text)


def entity_similarity(a: str | None, b: str | None) -> float:
    """Quick token-overlap similarity between two party names."""
    if not a or not b:
        return 0.0
    na_text = normalize_party_name(a)
    nb_text = normalize_party_name(b)
    if na_text == nb_text:
        return 1.0

    shorter_text, longer_text = sorted((na_text, nb_text), key=len)
    shorter_tokens = set(shorter_text.replace(",", " ").split())
    longer_tokens = set(longer_text.replace(",", " ").split())
    if len(shorter_tokens) >= 2 and shorter_tokens.issubset(longer_tokens):
        return 0.8

    na = set(na_text.split())
    nb = set(nb_text.split())
    if not na or not nb:
        return 0.0
    return len(na & nb) / max(len(na), len(nb))


def extract_case_parts(s: str) -> tuple[str, str, str]:
    """Extract (year, type, number) from any case number format.

    Handles: '22-CA-007345', '292022CA007345A001HC', '22-CA-7345', '25-CC-19119'
    """
    s = re.sub(r"[^A-Z0-9]", "", s.upper().strip())
    # Raw pipeline format: 29YYYYTTNNNNNN...
    m = re.match(r"29(\d{4})(CA|CC|DR|SC)(\d{6})", s)
    if m:
        return (m.group(1)[-2:], m.group(2), str(int(m.group(3))))
    # Norm format: YYTTNNNNNN / YYTTNNN
    m = re.match(r"(\d{2})(CA|CC|DR|SC)(\d+)", s)
    if m:
        return (m.group(1), m.group(2), str(int(m.group(3))))
    # Bare format without 29 prefix: YYYYTTNNNNNN
    m = re.match(r"(\d{4})(CA|CC|DR|SC)(\d+)", s)
    if m:
        return (m.group(1)[-2:], m.group(2), str(int(m.group(3))))
    return ("", "", "")


def has_document_grounded_identity(raw: dict, case_dir: str) -> bool:
    """True when the extracted property identity is clearly present in the PDF text."""
    raw_text = raw.get("raw_text") or ""
    if not raw_text or not case_numbers_match(raw.get("case_number", ""), case_dir, None):
        return False

    hits = 0
    if raw_text_contains(raw_text, raw.get("property_address")):
        hits += 1
    if raw_text_contains(raw_text, raw.get("subdivision")):
        hits += 1
    lot = raw.get("lot")
    if lot and raw_text_contains(raw_text, f"LOT {lot}"):
        hits += 1
    block = raw.get("block")
    if block and raw_text_contains(raw_text, f"BLOCK {block}"):
        hits += 1

    return hits >= 2


def find_alternate_hcpa_parcel(
    raw: dict,
    current_strap: str | None,
    hcpa: dict[str, dict],
) -> dict | None:
    """Look for an HCPA parcel that exactly matches the extracted address."""
    address_key = normalize_address_key(raw.get("property_address"))
    if not address_key:
        return None

    for parcel in hcpa.values():
        if parcel.get("strap") == current_strap:
            continue
        if parcel.get("address_key") == address_key:
            return parcel
    return None


def case_numbers_match(extracted: str, case_dir: str, pg_norm: str | None) -> bool:
    """Flexible case number comparison across formats."""
    if not extracted:
        return False
    e_parts = extract_case_parts(extracted)
    d_parts = extract_case_parts(case_dir)
    p_parts = extract_case_parts(pg_norm or "")

    if e_parts[0] and d_parts[0] and e_parts == d_parts:
        return True
    return bool(e_parts[0] and p_parts[0] and e_parts == p_parts)


# ---------------------------------------------------------------------------
# V1 → V2 schema patching
# ---------------------------------------------------------------------------

def patch_v1_schema(raw: dict) -> dict:
    """Patch old v1 cache format to pass current Pydantic schema."""
    patched = dict(raw)
    patched.pop("_metadata", None)
    patched.pop("_validation", None)

    # Remove any extra keys the old model had that new schema forbids
    known_fields = set(JudgmentExtraction.model_fields.keys())
    extra_keys = set(patched.keys()) - known_fields - {"raw_text"}
    for k in extra_keys:
        patched.pop(k)

    # Add missing keys
    for field_name in known_fields:
        if field_name not in patched and field_name != "raw_text":
            patched[field_name] = None

    # List fields
    for lf in ("defendants", "red_flags", "unclear_sections"):
        if patched.get(lf) is None:
            patched[lf] = []

    # Boolean fields
    for bf in ("is_condo", "is_online_sale", "service_by_publication",
               "hoa_safe_harbor_mentioned"):
        if patched.get(bf) is None:
            patched[bf] = False

    # Defendant sub-dicts
    for d in patched.get("defendants", []):
        if isinstance(d, dict):
            d.setdefault("lien_recording_reference", None)
            d.setdefault("is_deceased", False)
            d.setdefault("is_federal_entity", False)

    # Foreclosed mortgage sub-dict
    fm = patched.get("foreclosed_mortgage")
    if isinstance(fm, dict):
        fm.setdefault("current_holder", None)
        fm.setdefault("original_lender", None)
        fm.setdefault("instrument_number", None)
        fm.setdefault("recording_book", fm.pop("book", None))
        fm.setdefault("recording_page", fm.pop("page", None))
        fm.setdefault("recording_date", None)

    # Lis pendens sub-dict
    lp = patched.get("lis_pendens")
    if isinstance(lp, dict):
        lp.setdefault("instrument_number", None)
        lp.setdefault("recording_book", lp.pop("book", None))
        lp.setdefault("recording_page", lp.pop("page", None))
        lp.setdefault("recording_date", None)

    # Enum coercion
    if isinstance(patched.get("plaintiff_type"), str):
        valid_pt = {"bank", "servicer", "trust", "gse", "hoa", "private_lender", "other"}
        if patched["plaintiff_type"] not in valid_pt:
            patched["plaintiff_type"] = "other"
    for d in patched.get("defendants", []):
        if isinstance(d, dict) and isinstance(d.get("party_type"), str):
            valid_dt = {"borrower", "co_borrower", "spouse", "second_mortgage_holder",
                        "judgment_creditor", "hoa", "condo_association", "irs",
                        "federal_agency", "municipality", "tenant", "unknown"}
            pt = d["party_type"]
            if "|" in pt:
                for part in pt.split("|"):
                    if part.strip() in valid_dt:
                        d["party_type"] = part.strip()
                        break
                else:
                    d["party_type"] = "unknown"
            elif pt not in valid_dt:
                d["party_type"] = "unknown"
    for rf in patched.get("red_flags", []):
        if isinstance(rf, dict) and isinstance(rf.get("flag_type"), str):
            valid_rf = {"federal_defendant", "lost_note", "deceased_borrower",
                        "service_issue", "missing_hoa_defendant"}
            if rf["flag_type"] not in valid_rf:
                rf["flag_type"] = "service_issue"  # generic fallback
        if isinstance(rf, dict) and isinstance(rf.get("severity"), str):
            valid_sev = {"critical", "high", "medium", "low"}
            if rf["severity"] not in valid_sev:
                rf["severity"] = "medium"

    return patched


# ---------------------------------------------------------------------------
# Triage checks
# ---------------------------------------------------------------------------

def check_internal_consistency(raw: dict, case_dir: str) -> tuple[list[str], list[str]]:
    """Layer 1: Internal consistency checks.  No external data needed."""
    failures = []
    warnings = []

    # -- Case number vs directory --
    ext_case = raw.get("case_number", "")
    if ext_case and not case_numbers_match(ext_case, case_dir, None):
        failures.append(
            f"IDENTITY: Case number '{ext_case}' doesn't match directory '{case_dir}'"
        )

    # -- Amounts --
    total = raw.get("total_judgment_amount")
    if total is not None and total > 0:
        known = [
            raw.get("principal_amount"),
            raw.get("interest_amount"),
            raw.get("per_diem_interest"),
            raw.get("late_charges"),
            raw.get("escrow_advances"),
            raw.get("title_search_costs"),
            raw.get("court_costs"),
            raw.get("attorney_fees"),
        ]
        non_null = [c for c in known if c is not None and c != 0]
        if len(non_null) >= 3:
            known_sum = sum(non_null)
            credit_adjustments = extract_credit_adjustments(raw.get("raw_text", ""))
            adjusted_known_sum = known_sum - credit_adjustments
            if adjusted_known_sum > total * 1.01:  # items exceed total by >1%
                diff = adjusted_known_sum - total
                failures.append(
                    f"AMOUNTS: Known items ${known_sum:,.2f} exceed total ${total:,.2f} by ${diff:,.2f}"
                )
            # Check principal is reasonable fraction of total
            principal = raw.get("principal_amount")
            if principal and principal > total and credit_adjustments == 0:
                failures.append(
                    f"AMOUNTS: Principal ${principal:,.2f} exceeds total ${total:,.2f}"
                )
    elif total is None:
        warnings.append("AMOUNTS: No total_judgment_amount extracted")

    # -- Date ordering --
    jd = raw.get("judgment_date")
    sd = raw.get("foreclosure_sale_date")
    if jd and sd and jd > sd:
        warnings.append(f"DATES: Judgment date {jd} after sale date {sd}")

    # -- Missing critical fields --
    if not raw.get("plaintiff"):
        failures.append("MISSING: No plaintiff extracted")
    if not raw.get("defendants"):
        failures.append("MISSING: No defendants extracted")
    if not raw.get("legal_description") and not raw.get("property_address"):
        warnings.append("MISSING: No legal description or address")

    # -- Confidence sanity --
    conf = raw.get("confidence_score", raw.get("confidence"))
    unclear = raw.get("unclear_sections", [])
    if conf and conf >= 0.95 and len(unclear) >= 3:
        warnings.append(f"CONFIDENCE: Score {conf} but {len(unclear)} unclear sections")

    return failures, warnings


def check_property_identity(
    raw: dict,
    case_dir: str,
    fc_index: dict[str, dict],
    hcpa: dict[str, dict],
) -> tuple[list[str], list[str]]:
    """Layer 2: Property identity against HCPA parcel data."""
    failures = []
    warnings = []

    fc = fc_index.get(case_dir)
    if not fc:
        warnings.append("PROPERTY: No foreclosure row — cannot cross-check parcel")
        return failures, warnings

    strap = fc.get("strap")
    if not strap:
        warnings.append("PROPERTY: Foreclosure has no strap — cannot cross-check parcel")
        return failures, warnings

    parcel = hcpa.get(strap)
    if not parcel:
        warnings.append(f"PROPERTY: Strap {strap} not found in HCPA parcels")
        return failures, warnings

    if has_document_grounded_identity(raw, case_dir):
        alternate_parcel = find_alternate_hcpa_parcel(raw, strap, hcpa)
        if alternate_parcel:
            warnings.append(
                "PROPERTY: document-grounded identity matches alternate HCPA parcel "
                f"{alternate_parcel.get('strap')} ({alternate_parcel.get('address')}) rather "
                f"than foreclosure strap {strap}; foreclosure linkage is likely wrong"
            )
        else:
            warnings.append(
                "PROPERTY: document-grounded identity in the PDF conflicts with "
                f"foreclosure strap {strap} / HCPA parcel '{parcel.get('address')}'. "
                "Treat this as a linkage-data problem, not a bad extraction."
            )
        return failures, warnings

    # -- Subdivision match --
    # HCPA raw_sub is a 3-char code (e.g. "3ZM"), not a readable name.
    # Compare extracted subdivision against HCPA legal text instead.
    ext_sub = raw.get("subdivision")
    hcpa_legal = parcel.get("legal", "").upper()
    if ext_sub and hcpa_legal and len(hcpa_legal) > 10:
        # Extract significant words from subdivision name (skip noise)
        sub_words = set(re.findall(r"[A-Z]{3,}", ext_sub.upper()))
        sub_words -= {"THE", "ACCORDING", "PLAT", "THEREOF", "RECORDED",
                      "PUBLIC", "RECORDS", "HILLSBOROUGH", "COUNTY", "FLORIDA",
                      "SECTION", "PHASE", "UNIT", "BOOK", "PAGE", "ADDITION"}
        if sub_words:
            matches = sum(1 for w in sub_words if w in hcpa_legal)
            if matches == 0:
                failures.append(
                    f"PROPERTY: Subdivision '{ext_sub}' not found in HCPA legal: "
                    f"'{hcpa_legal[:100]}'"
                )

    # -- Address match --
    ext_addr_tokens = address_tokens(raw.get("property_address"))
    hcpa_addr_tokens = address_tokens(parcel.get("address"))
    if ext_addr_tokens and hcpa_addr_tokens:
        overlap = ext_addr_tokens & hcpa_addr_tokens
        if len(overlap) < 2 and len(ext_addr_tokens) >= 3:
            failures.append(
                f"PROPERTY: Address mismatch: extracted '{raw.get('property_address')}' "
                f"vs HCPA '{parcel.get('address')}'"
            )
        elif len(overlap) < 3 and len(ext_addr_tokens) >= 4:
            warnings.append(
                f"PROPERTY: Address weak match: extracted '{raw.get('property_address')}' "
                f"vs HCPA '{parcel.get('address')}'"
            )

    # -- Legal description: check lot/block appear in HCPA legal --
    hcpa_legal = parcel.get("legal", "").upper()
    ext_lot = str(raw.get("lot") or "").strip()
    ext_block = str(raw.get("block") or "").strip()
    if ext_lot and hcpa_legal:
        # Check if lot number appears in HCPA legal text
        lot_patterns = [f"LOT {ext_lot}", f"LOT{ext_lot}"]
        lot_found = any(p in hcpa_legal for p in lot_patterns)
        if not lot_found and ext_lot.isdigit():
            lot_found = f" {ext_lot} " in f" {hcpa_legal} "
        if not lot_found:
            warnings.append(
                f"PROPERTY: Lot '{ext_lot}' not found in HCPA legal: '{hcpa_legal[:80]}'"
            )
    if ext_block and hcpa_legal:
        block_patterns = [f"BLOCK {ext_block}", f"BLK {ext_block}", f"BLOCK{ext_block}"]
        block_found = any(p in hcpa_legal for p in block_patterns)
        if not block_found:
            warnings.append(
                f"PROPERTY: Block '{ext_block}' not found in HCPA legal: '{hcpa_legal[:80]}'"
            )

    return failures, warnings


def check_cross_document(
    raw: dict,
    case_dir: str,
    fc_index: dict[str, dict],
    ori_data: dict[str, list[dict]],
) -> tuple[list[str], list[str]]:
    """Layer 3: Cross-document consistency against ORI records."""
    failures = []
    warnings = []

    fc = fc_index.get(case_dir)
    if not fc:
        return failures, warnings

    strap = fc.get("strap")
    if not strap:
        return failures, warnings

    encumbrances = ori_data.get(strap, [])
    if not encumbrances:
        return failures, warnings

    # -- Plaintiff vs ORI lis pendens party1 --
    ext_plaintiff = raw.get("plaintiff")
    if ext_plaintiff:
        lp_records = [e for e in encumbrances if e.get("type") == "lis_pendens"]
        if lp_records:
            best_lp_sim = max(
                entity_similarity(ext_plaintiff, lp.get("party1"))
                for lp in lp_records
            )
            if best_lp_sim < 0.2:
                lp_parties = [lp.get("party1", "?")[:50] for lp in lp_records[:3]]
                failures.append(
                    f"CROSS-DOC: Plaintiff '{ext_plaintiff[:50]}' doesn't match "
                    f"any ORI lis pendens party: {lp_parties}"
                )
            elif best_lp_sim < 0.4:
                warnings.append(
                    f"CROSS-DOC: Plaintiff '{ext_plaintiff[:50]}' weak match to ORI LP "
                    f"(best sim={best_lp_sim:.2f})"
                )

    # -- Foreclosed mortgage instrument vs ORI mortgage records --
    fm = raw.get("foreclosed_mortgage")
    if isinstance(fm, dict):
        fm_inst = fm.get("instrument_number")
        fm_book = str(fm.get("recording_book") or fm.get("book") or "")
        fm_page = str(fm.get("recording_page") or fm.get("page") or "")

        if fm_inst or (fm_book and fm_page):
            mortgage_records = [e for e in encumbrances if e.get("type") == "mortgage"]
            if mortgage_records:
                matched = False
                for m in mortgage_records:
                    if fm_inst and m.get("instrument") and fm_inst in m["instrument"]:
                        matched = True
                        break
                    if fm_book and fm_page and m.get("book") == fm_book and m.get("page") == fm_page:
                        matched = True
                        break
                if not matched:
                    warnings.append(
                        f"CROSS-DOC: Foreclosed mortgage ref (inst={fm_inst}, "
                        f"book={fm_book}, page={fm_page}) not found in "
                        f"{len(mortgage_records)} ORI mortgage records"
                    )

    return failures, warnings


# ---------------------------------------------------------------------------
# Main triage
# ---------------------------------------------------------------------------

def triage_one(
    json_path: Path,
    fc_index: dict[str, dict],
    hcpa: dict[str, dict],
    ori_data: dict[str, list[dict]],
) -> dict:
    """Triage a single extracted JSON file."""
    case_dir = json_path.parent.parent.name
    result = {
        "path": str(json_path),
        "case_dir": case_dir,
        "status": "UNKNOWN",
        "score": 0,
        "issues": [],
        "summary": {},
    }

    # Load JSON
    try:
        with open(json_path) as f:
            raw = json.load(f)
    except Exception as e:
        result["status"] = "CORRUPT"
        result["issues"].append(f"[CORRUPT] Cannot parse JSON: {e}")
        return result

    result["summary"] = {
        "plaintiff": raw.get("plaintiff"),
        "case_number": raw.get("case_number"),
        "total": raw.get("total_judgment_amount"),
        "confidence": raw.get("confidence_score", raw.get("confidence")),
        "defendants": len(raw.get("defendants", [])),
        "judge": raw.get("judge_name"),
    }

    if "plaintiff" not in raw and "borrower" in raw:
        result["status"] = "NOT_JUDGMENT"
        result["issues"].append("[SCHEMA] Mortgage extraction, not judgment")
        return result

    # -- Layer 0: Schema validation --
    schema_issues = []
    patched = patch_v1_schema(raw)
    pydantic_obj = None
    try:
        pydantic_obj = JudgmentExtraction.model_validate(patched)
    except Exception as e:
        err_str = str(e)
        if len(err_str) > 300:
            err_str = err_str[:300] + "..."
        schema_issues.append(f"Pydantic: {err_str}")

    validation_failures = []
    validation_warnings = []
    if pydantic_obj:
        try:
            validation_failures, validation_warnings = pydantic_obj.validate_extraction()
        except Exception as e:
            schema_issues.append(f"validate_extraction() crashed: {e}")

    # -- Layer 1: Internal consistency --
    int_failures, int_warnings = check_internal_consistency(raw, case_dir)

    # -- Layer 2: Property identity vs HCPA --
    prop_failures, prop_warnings = check_property_identity(raw, case_dir, fc_index, hcpa)

    # -- Layer 3: Cross-document vs ORI --
    xdoc_failures, xdoc_warnings = check_cross_document(raw, case_dir, fc_index, ori_data)

    # -- Score --
    score = 100

    # Schema
    if schema_issues:
        score -= 40

    # Internal consistency (strongest signal)
    score -= 25 * len(int_failures)
    score -= 5 * len(int_warnings)

    # Property identity (very strong)
    score -= 30 * len(prop_failures)
    score -= 5 * len(prop_warnings)

    # Cross-document
    score -= 20 * len(xdoc_failures)
    score -= 5 * len(xdoc_warnings)

    # Validation gates
    score -= 15 * len(validation_failures)
    score -= 3 * len(validation_warnings)

    # Archived / orphaned
    fc = fc_index.get(case_dir)
    if not fc:
        score -= 10
    elif fc.get("archived"):
        score -= 3

    score = max(0, min(100, score))

    # Aggregate issues
    all_issues = []
    for i in schema_issues:
        all_issues.append(f"[SCHEMA] {i}")
    for i in int_failures:
        all_issues.append(f"[INTERNAL] {i}")
    for i in int_warnings:
        all_issues.append(f"[INTERNAL-WARN] {i}")
    for i in prop_failures:
        all_issues.append(f"[PROPERTY] {i}")
    for i in prop_warnings:
        all_issues.append(f"[PROPERTY-WARN] {i}")
    for i in xdoc_failures:
        all_issues.append(f"[CROSS-DOC] {i}")
    for i in xdoc_warnings:
        all_issues.append(f"[CROSS-DOC-WARN] {i}")
    for i in validation_failures:
        all_issues.append(f"[HARD_GATE] {i}")
    for i in validation_warnings:
        all_issues.append(f"[SOFT_GATE] {i}")
    if fc and fc.get("archived"):
        all_issues.append("[INFO] Foreclosure is archived")
    elif not fc:
        all_issues.append("[INFO] No foreclosure row for this case directory")

    result["issues"] = all_issues
    result["score"] = score

    if score >= 80:
        result["status"] = "GOOD"
    elif score >= 50:
        result["status"] = "SUSPECT"
    elif score > 0:
        result["status"] = "BAD"
    else:
        result["status"] = "CRITICAL"

    return result


def main():
    json_files = sorted(DATA_ROOT.glob("*/documents/final_judgment_*_extracted.json"))
    logger.info(f"Found {len(json_files)} final judgment extraction files")

    with psycopg.connect(PG_DSN) as conn:
        fc_index = load_foreclosure_index(conn)
        logger.info(f"Loaded {len(fc_index)} foreclosure rows")
        hcpa = load_hcpa_parcels(conn)
        logger.info(f"Loaded {len(hcpa)} HCPA parcel records")
        ori_data = load_ori_data(conn)
        logger.info(f"Loaded ORI data for {len(ori_data)} straps")

    results = []
    for jf in json_files:
        r = triage_one(jf, fc_index, hcpa, ori_data)
        results.append(r)

    results.sort(key=lambda r: r["score"])

    by_status: dict[str, list] = {}
    for r in results:
        by_status.setdefault(r["status"], []).append(r)

    print("\n" + "=" * 80)
    print("JUDGMENT EXTRACTION TRIAGE REPORT")
    print("=" * 80)
    print(f"\nTotal files triaged: {len(results)}")
    for status in ("CRITICAL", "BAD", "SUSPECT", "GOOD", "CORRUPT", "NOT_JUDGMENT"):
        count = len(by_status.get(status, []))
        if count:
            print(f"  {status:14s}: {count}")

    # Print non-GOOD
    bad_results = [r for r in results if r["status"] not in ("GOOD",)]
    if bad_results:
        print(f"\n{'─' * 80}")
        print(f"ISSUES ({len(bad_results)} files)")
        print(f"{'─' * 80}")
        for r in bad_results:
            print(f"\n[{r['status']}] score={r['score']} | {r['case_dir']}")
            s = r["summary"]
            if s:
                print(f"  plaintiff: {(s.get('plaintiff') or '?')[:70]}")
                print(f"  case: {s.get('case_number', '?')} | total: ${s.get('total', 0) or 0:,.2f} | conf: {s.get('confidence', '?')}")
            for issue in r["issues"]:
                print(f"  {issue}")

    # GOOD summary
    good = by_status.get("GOOD", [])
    if good:
        print(f"\n{'─' * 80}")
        print(f"GOOD ({len(good)} files)")
        print(f"{'─' * 80}")
        for r in good[:5]:
            s = r["summary"]
            print(f"  score={r['score']} | {r['case_dir']} | {(s.get('plaintiff') or '?')[:45]} | ${s.get('total', 0) or 0:,.2f}")
        if len(good) > 5:
            print(f"  ... and {len(good) - 5} more")

    out_path = Path("data/judgment_triage_results.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nFull results: {out_path}")

    reextract = [r for r in results if r["status"] in ("CRITICAL", "BAD")]
    if reextract:
        print(f"\n{'=' * 80}")
        print(f"RE-EXTRACTION PRIORITY: {len(reextract)} files")
        print(f"Estimated time: ~{len(reextract) * 3.3:.0f} min ({len(reextract) * 3.3 / 60:.1f} hrs)")
        print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
