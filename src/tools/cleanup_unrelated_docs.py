"""
Cleanup tool: Remove unrelated documents ingested via adjacent-instrument contamination.

For each folio:
1. Get known lot/block from parcels table
2. Check each document's legal_description and party names against property's known data
3. Remove documents that don't match on either criterion
4. Cascade: delete encumbrances that reference removed documents

Usage:
    uv run python -m src.tools.cleanup_unrelated_docs [--folio FOLIO] [--dry-run]
"""

import argparse
import re

from loguru import logger

from src.db.operations import PropertyDB
from src.services.step4v2.name_matcher import NameMatcher


def parse_lot_block(legal: str) -> tuple[str | None, str | None]:
    """Extract lot and block from a legal description string."""
    lot_match = re.search(r"\bLOT\s+(\d+[A-Z]?)", legal, re.IGNORECASE)
    block_match = re.search(r"\bBL(?:OC)?K?\s+(\d+[A-Z]?)", legal, re.IGNORECASE)
    lot = lot_match.group(1) if lot_match else None
    block = block_match.group(1) if block_match else None
    return lot, block


def cleanup_folio(conn, folio: str, name_matcher: NameMatcher, dry_run: bool = True) -> dict:
    """Remove unrelated documents for a single folio."""
    # Get known legal description
    parcel = conn.execute(
        "SELECT legal_description, owner_name FROM parcels WHERE folio = ?", [folio]
    ).fetchone()
    if not parcel:
        return {"folio": folio, "skipped": "no parcel data"}

    parcel = dict(parcel)
    known_legal = parcel.get("legal_description") or ""
    known_lot, known_block = parse_lot_block(known_legal)

    if not known_lot and not known_block:
        return {"folio": folio, "skipped": "no lot/block in legal description"}

    # Get known party names
    party_rows = conn.execute(
        "SELECT DISTINCT party_name_normalized FROM property_parties WHERE folio = ? AND is_generic = 0",
        [folio],
    ).fetchall()
    known_parties = {dict(r)["party_name_normalized"] for r in party_rows if dict(r).get("party_name_normalized")}

    # Also include chain owners and judgment parties
    chain_rows = conn.execute(
        "SELECT DISTINCT owner_name FROM chain_of_title WHERE folio = ?", [folio]
    ).fetchall()
    for r in chain_rows:
        owner = dict(r).get("owner_name")
        if owner:
            known_parties.add(name_matcher.normalize(owner))

    # Get all documents for folio
    docs = conn.execute(
        "SELECT id, document_type, legal_description, party1, party2, instrument_number FROM documents WHERE folio = ?",
        [folio],
    ).fetchall()

    to_remove = []
    for doc in docs:
        doc = dict(doc)
        doc_legal = doc.get("legal_description") or ""
        doc_type = (doc.get("document_type") or "").upper()

        # Skip judgment docs — always keep
        if "JUD" in doc_type or "JUDGMENT" in doc_type:
            continue

        # Check legal description match
        if doc_legal:
            doc_lot, doc_block = parse_lot_block(doc_legal)
            if doc_lot and known_lot and doc_lot.upper() != known_lot.upper():
                to_remove.append(doc["id"])
                continue
            if doc_block and known_block and doc_block.upper() != known_block.upper():
                to_remove.append(doc["id"])
                continue
            # Legal matched or couldn't be compared — keep
            continue

        # No legal description — check party overlap
        if known_parties:
            p1 = name_matcher.normalize(doc.get("party1") or "")
            p2 = name_matcher.normalize(doc.get("party2") or "")
            if p1 not in known_parties and p2 not in known_parties:
                to_remove.append(doc["id"])

    # Remove documents and cascade to encumbrances
    removed_docs = 0
    removed_encs = 0
    if to_remove:
        if dry_run:
            logger.info(f"[DRY RUN] Would remove {len(to_remove)} docs for {folio}")
        else:
            for doc_id in to_remove:
                # Delete encumbrances referencing this document's instrument
                doc_row = conn.execute(
                    "SELECT instrument_number FROM documents WHERE id = ?", [doc_id]
                ).fetchone()
                if doc_row:
                    inst = dict(doc_row).get("instrument_number")
                    if inst:
                        cursor = conn.execute(
                            "DELETE FROM encumbrances WHERE folio = ? AND instrument = ?",
                            [folio, inst],
                        )
                        removed_encs += cursor.rowcount

                conn.execute("DELETE FROM documents WHERE id = ?", [doc_id])
                removed_docs += 1

            conn.commit()
            logger.info(f"Removed {removed_docs} docs, {removed_encs} encumbrances for {folio}")

    return {
        "folio": folio,
        "docs_checked": len(docs),
        "docs_removed": len(to_remove) if dry_run else removed_docs,
        "encs_removed": removed_encs,
        "dry_run": dry_run,
    }


def main():
    parser = argparse.ArgumentParser(description="Cleanup unrelated documents")
    parser.add_argument("--folio", help="Process a single folio")
    parser.add_argument("--dry-run", action="store_true", default=True, help="Show what would be removed (default)")
    parser.add_argument("--execute", action="store_true", help="Actually delete unrelated documents")
    args = parser.parse_args()

    dry_run = not args.execute

    db = PropertyDB()
    conn = db.connect()
    name_matcher = NameMatcher(conn)

    if args.folio:
        folios = [args.folio]
    else:
        # Get all folios with documents
        rows = conn.execute("SELECT DISTINCT folio FROM documents WHERE folio IS NOT NULL").fetchall()
        folios = [dict(r)["folio"] for r in rows]

    logger.info(f"Processing {len(folios)} folios (dry_run={dry_run})")

    total_docs_removed = 0
    total_encs_removed = 0
    for folio in folios:
        result = cleanup_folio(conn, folio, name_matcher, dry_run=dry_run)
        total_docs_removed += result.get("docs_removed", 0)
        total_encs_removed += result.get("encs_removed", 0)

    logger.info(f"Total: {total_docs_removed} docs, {total_encs_removed} encumbrances {'would be' if dry_run else ''} removed")


if __name__ == "__main__":
    main()
