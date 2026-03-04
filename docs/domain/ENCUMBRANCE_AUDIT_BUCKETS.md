# Encumbrance Audit Buckets

This document defines the audit taxonomy for encumbrance completeness on active
foreclosures. The goal is to stop treating every missing title issue as a
generic "missing lien" problem and instead separate:

1. recorded official-record discovery gaps,
2. survival-risk gaps that may not live in official records,
3. identity and title-break gaps that block correct linkage.

This taxonomy is meant to drive:
- PostgreSQL audit queries,
- targeted recovery tools,
- conservative buyer-facing language,
- cross-source search escalation.

## Scope

These buckets are primarily for active foreclosures with judgment or other
strong foreclosure evidence. They should be evaluated against the current owner
window and the current foreclosure case, not against the full historical parcel
record in the abstract.

## Audit Lanes

### 1. Recorded ORI Discovery Gaps

These are cases where a recorded document probably exists in official records,
but our current search, matching, or persistence logic failed to capture it.

Core buckets:
- `lp_missing`
  An LP-required foreclosure case has no persisted lis pendens.
- `judgment_missing`
  The foreclosure case lacks a persisted final judgment or equivalent judgment
  extraction anchor.
- `foreclosing_lien_missing`
  The case exists, but the foreclosing mortgage or lien is not reflected in
  `ori_encumbrances`.
- `plaintiff_chain_gap`
  The judgment plaintiff does not line up with any mortgage, assignment, merger,
  or successor evidence.
- `cc_lien_gap`
  A lien-style county-civil case such as `CC Enforce Lien` exists, but no lien
  row is present.
- `association_lien_gap`
  HOA or COA foreclosure signals exist, but no association lien was found.
- `sat_parent_gap`
  A satisfaction or release exists, but the parent mortgage or lien is missing
  or unresolved.
- `lifecycle_base_gap`
  A lifecycle/supporting document exists (`MOD`, `SUB`, `NCL`, `CTF`,
  assignment, release), but the base encumbrance it depends on is missing.
- `base_without_lifecycle`
  A mortgage or lien exists, but surrounding lifecycle evidence is absent where
  it should likely exist.

### 2. Survival-Risk Gaps

These are not always ORI misses. They are risk states that matter to a buyer
even if the underlying obligation is unrecorded, partially recorded, or tracked
in another system.

Core buckets:
- `superpriority_non_ori_risk`
  Utility, code, PACE, CDD, tax, or similar risk is likely, but the evidence is
  not limited to official records.
- `code_enforcement_risk`
  Municipal or county code-enforcement signals exist, but the lien or amount is
  not fully resolved.
- `utility_survival_risk`
  Vacancy or municipal-service nonpayment signals suggest utility debt that may
  survive without a recorded ORI instrument.
- `tax_overlap_risk`
  The property is in both mortgage-foreclosure and tax-sale or tax-deed orbit.
- `irs_redemption_risk`
  A federal party or IRS lien signal exists, creating a likely redemption window
  issue even if the lien itself is later extinguished.
- `construction_lien_risk`
  NOC, permit, contractor, or recent work signals exist, but no mechanic's or
  construction-lien family evidence has been found.
- `historical_window_gap`
  Encumbrances exist, but current-owner-period coverage is weak or everything is
  pre-acquisition and likely historical.

### 3. Identity And Title-Break Gaps

These are linkage failures where the document may exist, but the party, parcel,
or ownership identity is broken.

Core buckets:
- `trustee_trust_gap`
  The property is held in trust form, but search only found one side of the
  trustee or trust naming pattern.
- `merger_successor_gap`
  A plaintiff or secured party appears to be a successor entity, but the merger,
  name change, or DBA bridge is missing.
- `probate_gap`
  Owner death or estate facts suggest an unlinked probate or administration
  event.
- `corrective_deed_gap`
  Parcel or party evidence implies a corrective instrument or scrivener-error
  fix that has not been linked.
- `wild_deed_gap`
  Parcel-index or geographic indexing likely contains relevant instruments that
  name-based search missed.
- `parcel_identity_gap`
  Strap, folio, unit, lot-block, or parcel merge-split issues prevent reliable
  cross-linking.

## Lis Pendens And Final Judgment Signals

Lis pendens and final judgment should be treated as two structured snapshots of
the same foreclosure action. Comparing them produces higher-signal audit leads
than either document alone.

### LP-Only Signals

Use the lis pendens for:
- earliest plaintiff identity,
- earliest defendant and joined-party set,
- earliest property description,
- earliest case posture,
- early evidence that the action is mortgage foreclosure, lien enforcement,
  association foreclosure, or another type.

LP-driven buckets:
- `lp_party_gap`
  A significant LP party has no matching encumbrance, plaintiff-chain, or title
  evidence.
- `lp_property_scope_gap`
  The LP references parcels, units, or legal tokens not reflected in the saved
  encumbrance set.

### Final-Judgment-Only Signals

Use the final judgment for:
- final plaintiff identity,
- adjudicated party universe,
- joined or defaulted lienholders,
- more precise foreclosed instrument description,
- final relief and sale framing.

Judgment-driven buckets:
- `judgment_joined_party_gap`
  A joined or adjudicated party in the final judgment is not reflected in
  encumbrance discovery.
- `judgment_instrument_gap`
  The judgment describes the foreclosed mortgage or lien more precisely than the
  saved encumbrance set.

### LP-To-Judgment Delta Signals

Compare the LP to the judgment for:
- plaintiff changes,
- party additions or removals,
- property-description changes,
- instrument-detail refinement,
- long litigation windows with no supporting lifecycle evidence.

Delta-driven buckets:
- `lp_to_judgment_plaintiff_change`
  Likely assignment, merger, substitution, or successor event missing from the
  chain.
- `lp_to_judgment_party_expansion`
  New parties appear by judgment, often indicating junior lienholders,
  associations, or government claimants.
- `lp_to_judgment_property_change`
  Parcel scope, unit, legal description, or address changed between filing and
  judgment.
- `long_case_interim_risk`
  A long LP-to-judgment span exists without supporting encumbrance lifecycle
  evidence in between.

## PostgreSQL Corroboration Sources

Before widening live clerk search, use PostgreSQL to decide which bucket a case
belongs to and which recovery path is justified.

Primary tables:
- `foreclosures`
  Case number, plaintiff, owner, strap, folio, address, filing date, judgment
  data, and operational status.
- `ori_encumbrances`
  Persisted mortgages, liens, assignments, satisfactions, releases, LPs, NOCs,
  and lifecycle/supporting documents.
- `official_records_daily_instruments`
  Seed source for raw document families that may be dropped, underused, or worth
  targeted recovery.
- `foreclosure_title_events`
  Timeline context for permits, tax, market, sales, and title events.
- `hcpa_bulk_parcels`
  Parcel-level owner and address identity.
- `hcpa_allsales`
  Acquisition-date and grantor-grantee context.
- `clerk_civil_cases`
  Civil case style, case type, filing date, and related context.
- `clerk_civil_parties`
  Plaintiff, defendant, association, and creditor names for targeted expansion.
- `sunbiz` and related entity tables
  Merger, DBA, successor, and entity continuity evidence.

High-value PG-first cohorts:
- `mortgage present, no lien`
- `CC Enforce Lien, no lien`
- `SAT/REL present, parent unresolved`
- `lifecycle doc present, base encumbrance unresolved`
- `recent permit or NOC signal, no construction lien evidence`
- `plaintiff in judgment, but no assignment or successor chain`

## Search Escalation Rules

We should use exact and corroborated search first, then only use fuzzy search as
lead generation.

Order of operations:
1. exact joins by case number, strap, folio, normalized address, and explicit
   instrument reference;
2. current-owner-window checks using acquisition date and title timeline;
3. alias and entity expansion using trustee forms, Sunbiz successors, DBAs, and
   grantor-grantee history;
4. tight fuzzy search as lead generation only.

Tight fuzzy search should be bounded by:
- address or legal corroboration,
- case-type context,
- date window,
- entity type,
- document family.

Do not persist a new encumbrance row from fuzzy evidence alone. Require at least
one second corroborator such as:
- parcel or address match,
- case-number match,
- book-page or instrument reference,
- ownership-window fit,
- contractor or NOC linkage,
- plaintiff or joined-party confirmation from the judgment.

## Recommended Initial Active-Foreclosure Audit Buckets

If we need a practical first implementation, start with these buckets:

1. `plaintiff_chain_gap`
2. `cc_lien_gap`
3. `construction_lien_risk`
4. `sat_parent_gap`
5. `superpriority_non_ori_risk`
6. `historical_window_gap`
7. `identity_break_gap`
8. `judgment_joined_party_gap`
9. `lp_to_judgment_plaintiff_change`
10. `lp_to_judgment_party_expansion`

These buckets are specific enough to drive recovery tools, but broad enough to
cover the main reasons encumbrance completeness fails in practice.
