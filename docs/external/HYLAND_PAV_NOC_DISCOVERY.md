# Hyland PAV NOC Discovery Notes

## Purpose

This note records the live behavior we observed while investigating why active
foreclosures with recent permit activity still lacked persisted Notice of
Commencement (NOC) rows in `ori_encumbrances`.

The goal is not to restate the ORI pipeline. The goal is to preserve the
practical search behavior of the HillsClerk public-access stack so future work
does not regress back to legal-only NOC discovery.

## Backend Identification

The HillsClerk Public Access site at
`https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html` identifies
itself in the browser console as:

- `Hyland.Web.PublicAccess.Client 22.1.11.1000`

The SPA also exposes JSON APIs under:

- `/PAVDirectSearch/api/CustomQuery`
- `/PAVDirectSearch/api/Keywords`
- `/PAVDirectSearch/api/DocumentType`
- `/PAVDirectSearch/api/CustomQuery/KeywordSearch`
- `/PAVDirectSearch/api/DocumentType/FullTextSearch`

This is consistent with a Hyland Public Access / OnBase-style deployment where
the public UI is a configurable front-end over keyword-indexed and full-text
search services.

## Useful Live Query Metadata

Live API metadata from the HillsClerk deployment showed these relevant Custom
Query IDs:

| Query ID | Name | Use |
|----------|------|-----|
| `319` | `ORI-Book/Page` | Exact official-record lookups |
| `320` | `ORI-Instrument #` | Exact official-record lookups |
| `321` | `ORI-Legal Search` | Keyword-indexed legal-description searches |
| `326` | `ORI-Party Name` | Keyword-indexed party-name searches |
| `350` | `ORI-Case #` | Case-number searches |
| `128` | `ORI-Full Text` | Full-text search surface in the client |

Relevant keyword IDs from `/api/Keywords`:

### Query `321` (`ORI-Legal Search`)

| Keyword ID | Name |
|------------|------|
| `1011` | `Legal Description` |
| `1285` | `ORI - Doc Type` |
| `1634` | `Recording Date Time` |

### Query `326` (`ORI-Party Name`)

| Keyword ID | Name |
|------------|------|
| `486` | `Name` |
| `1285` | `ORI - Doc Type` |
| `1634` | `Recording Date Time` |
| `1661` | `ORI - Person Type` |

Relevant document-type metadata from `/api/DocumentType`:

| Doc Type ID | Name |
|-------------|------|
| `1138` | `Notice of Commencement - ORI` |

## What Actually Works For NOCs

### 1. Legal-description search should not rely on street address alone

For known-positive NOCs, exact address terms in query `321` often returned no
results even when the document existed in the live system.

Example pattern:

- `Legal Description = "4922 S 82ND ST"`
- `ORI - Doc Type = "(NOC) NOTICE OF COMMENCEMENT"`
- Result: no rows

This means the legal keyword index is often not storing the street address in a
way that is recoverable through `ORI-Legal Search`.

### 2. Subdivision / lot-block terms work better than street address in legal search

Known-positive NOCs were recoverable from query `321` when the legal search used
subdivision-style terms instead of the postal address.

Practical implication:

- For legal search, generate subdivision / plat / lot-block terms first.
- Treat exact street address in legal search as low-confidence.

### 3. Party-name search is essential for NOCs

Query `326` with an explicit NOC doc-type filter found real NOCs that legal
search missed.

Confirmed live example:

- Foreclosure `15332`
- Property: `10731 BANFIELD DR`
- Party search: `BING THERESA`
- Doc type: `(NOC) NOTICE OF COMMENCEMENT`
- Result: instrument `2024339003`

The returned summary text contained the target property address, which confirmed
that this was a real NOC for the foreclosure property and not a neighboring lot.

Practical implication:

- NOC discovery should always try party-name search with a doc-type filter.
- Party terms should include current owner and recent chain/builder names.

### 4. Full-text search is the best fallback for high-signal cases

Direct POSTs to `/api/DocumentType/FullTextSearch` with:

- `DocTypeID = 1138`
- `SearchText = "<exact street address>"`

found exact-property NOCs that keyword search missed.

Confirmed live example:

- `SearchText = "10731 BANFIELD DR"`
- `DocTypeID = 1138`
- Result: NOC instrument `2024339003`

This is the strongest evidence that the remaining NOC problem is not only local
matching. The document exists in live PAV and is retrievable, but was absent
from the current seed path.

### 5. Full-text search is too noisy for broad default use

For several other addresses, full-text search returned:

- neighboring homes in the same subdivision
- nearby addresses with similar street names
- high-scoring but irrelevant summaries

Practical implication:

- use full-text exact-address search only as a fallback
- restrict it to high-signal cases, such as recent high-value county permits
- require exact-address or strong street-token verification before accepting a hit

## Source-Coverage Limitation

The local `official_records_daily_instruments` source currently contains NOC
rows only from:

- `2021-11-04` through `2026-02-20`

That creates two separate NOC failure modes:

1. **Historical coverage gap**
   Older NOCs are simply outside the current local seed dataset.
2. **Recent source miss**
   A live PAV NOC may still be absent from `official_records_daily_instruments`.

Confirmed live/source mismatch:

- instrument `2024339003` exists in live PAV for `10731 BANFIELD DR`
- `official_records_daily_instruments` had `0` rows for `2024339003`

So the local daily feed cannot be treated as a complete authority for recent
NOCs either.

## Current Investigation Outcome

For active foreclosures lacking persisted NOCs:

- most are not strong evidence of a remaining search bug because they have no
  permit coverage at all, or only older permit history
- the suspicious bucket is the set with recent county permit signal

The strongest likely remaining discovery gaps are recent residential
construction cases, especially:

- `15248` — `5406 LIMELIGHT DR` — `HC-BLD-22-0028588`
- `15272` — `16838 DELIA ST` — `HC-BLD-21-0027277`
- `15294` — `3135 MARINE GRASS DR` — `HC-BLD-22-0028560`
- `15328` — `17038 WAVE TRESSLE PL` — `HC-BLD-21-0011880`

One confirmed live miss:

- `15332` — `10731 BANFIELD DR` — live PAV NOC found via party/full-text search

## Recommended Search Order

For NOC discovery, prefer this order:

1. Exact instrument / book-page lookups when already known.
2. Legal search (`321`) with NOC doc-type filter using subdivision / lot-block /
   plat terms.
3. Party search (`326`) with NOC doc-type filter using owner plus recent chain /
   builder names.
4. Full-text exact-address fallback for recent high-signal permit cases only.

Acceptance rule:

- Never accept a NOC on owner-name overlap alone.
- When a NOC result shows an explicit street address, require street-token
  overlap with the target property.

## Operational Implication

If we want reliable NOC coverage for active foreclosures, we should treat live
PAV probing as part of the discovery strategy for recent permit-backed no-NOC
cases. The local daily-instruments seed is useful, but it is not sufficient as
the sole NOC source.
