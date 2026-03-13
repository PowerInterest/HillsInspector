# Missing Party Analysis

## Why This Doc Exists

As of March 12, 2026, the pipeline's title-chain hard gates are passing, but a
meaningful share of active foreclosure chains are still classified as
`BROKEN`. This document captures what `MISSING_PARTY` means in the current
controller, how often it appears, why Sunbiz is not currently helping, and
where normalization tooling is likely to help or not help.

This is not a generic title-gap note. It is a concrete snapshot of the current
production dataset and current code paths in:

- `src/services/pg_title_chain_controller.py`
- `src/services/pg_title_break_service.py`
- `src/db/migrations/create_foreclosures.py`

## Current Title Snapshot

At the time of this analysis:

- Active foreclosures with title summaries: `107`
- Complete chains: `40`
- Broken chains: `67`
- Missing-folio chains: `0`
- No-sales chains: `0`

Broken-chain mix:

- `39` `folio_only_gaps`
- `15` mixed `missing_party + folio` gaps
- `11` pure `missing_party` gaps
- `2` `no_name_links_anywhere`

`MISSING_PARTY` is real, but it is not the dominant broken-chain bucket. The
largest bucket is still `CHAINED_BY_FOLIO`.

## What `MISSING_PARTY` Means In Code

The sale-link scorer in `TitleChainController` marks a sale event as
`MISSING_PARTY` when either side of the owner-to-owner comparison is unusable:

- current sale `grantor`
- previous sale `grantee`

In `src/services/pg_title_chain_controller.py`, the current logic is:

1. Order `SALE` events by `event_date, id`
2. For each sale after the first, compare `grantor` to the prior sale's
   `grantee`
3. If either side normalizes to `NULL`, assign `MISSING_PARTY`
4. Otherwise try exact match, then trigram similarity, else
   `CHAINED_BY_FOLIO`

So `MISSING_PARTY` is not a fuzzy-match failure. It means the chain builder did
not have usable party text on one or both sides of a deed hop.

## Full `MISSING_PARTY` Counts

For active foreclosures with non-null `judgment_data`:

- Cases with at least one `MISSING_PARTY` row: `26`
- Total `MISSING_PARTY` rows: `38`

Root-cause buckets:

- `25` `prev_grantee_blank`
- `13` `both_blank`
- `0` `grantor_blank`
- `0` `grantor_unusable`
- `0` `prev_grantee_unusable`
- `0` `both_unusable`

That is the key result: the current `MISSING_PARTY` set is almost entirely
historical-sale data loss, not bad parsing of present strings.

## Structural Pattern

### Where The Gaps Occur

- `26` rows occur at title-chain sequence `2`
- `12` rows occur at title-chain sequence `3`

### Age Of The Missing Rows

- `24` rows are on pre-1990 sales
- `11` rows are on 1990s sales
- `3` rows are on 2000s sales

This is strongly concentrated in the earliest historical deed hops.

### Instrument Availability

`both_blank` rows:

- `13 / 13` have no current `instrument_number`

`prev_grantee_blank` rows:

- `25 / 25` have a current `instrument_number`
- `25 / 25` have a prior sale row with no prior `instrument_number`

This matters operationally:

- when both names are blank and there is no current deed number, recovery is
  hard
- when the current deed exists but the prior deed row has no instrument number,
  the chain fails because the previous owner record is under-specified

## Recovery Status

All `26` `MISSING_PARTY` cases already have `ORI_DEED_SEARCH` retry history
with `SEARCH_NO_RESULT` sentinels.

That means the current recovery loop has already attempted to repair them and
did not find a deed under the present search strategy.

This does not prove the deed does not exist. It proves the current recovery
strategy could not find it from the available party/date inputs.

## Why Sunbiz Has Not Been Used

The short answer is: because the title-chain and title-break code paths do not
join `sunbiz_entity_*` at all today.

Current behavior:

- `pg_title_chain_controller.py` uses sale `grantor` and prior sale `grantee`
  only
- `pg_title_break_service.py` searches ORI using raw party strings and local
  ORI overlays
- `create_foreclosures.py` provides generic SQL helpers like
  `normalize_party_name()`, `entity_match_score()`, and `is_same_entity()`
  without any Sunbiz alias expansion

Sunbiz is loaded by the pipeline and queried elsewhere in the app, but it is
not part of title-chain or title-break resolution today.

## Why Sunbiz Would Not Clear Most Of The Current `MISSING_PARTY` Set Anyway

Even though Sunbiz is not wired into title repair, it is also not the main fix
for this specific bucket.

In the current `MISSING_PARTY` population:

- only `5` of `38` rows contain an entity-style name at all
- those `5` rows span `5` cases
- `9` rows contain multi-party semicolon-delimited names

The main problem is still blank prior-owner fields, not unresolved entity
aliases.

There is also a dataset-size issue in the current DB snapshot:

- `sunbiz_entity_filings = 81`
- `sunbiz_entity_parties = 120`
- `sunbiz_entity_events = 0`

This is not a full corporate-identity universe. It is too sparse to serve as a
general-purpose alias resolver today.

For the `5` entity-style `MISSING_PARTY` rows, none of the names matched the
currently loaded `sunbiz_entity_*` snapshot using the project's own
normalization or `is_same_entity()` helper.

Entity-style `MISSING_PARTY` rows:

- `292019CC066831A001HC#2` `GENERAL HOMES CORP`
- `292020CA004142A001HC#2` `CORY LAKES LTD; NORTHEAST DEVELOPMENT CO`
- `292025CA007991A001HC#3` `NICARAGUAN FREEDOM CORP`
- `292025CA008140A001HC#2` `FIVE STAR HOMES INC`
- `292025CC032673A001HC#2` `CORY LAKES LTD`

## Where Sunbiz Probably Helps More

Sunbiz is more likely to help the `CHAINED_BY_FOLIO` bucket than the current
`MISSING_PARTY` bucket.

In the current `CHAINED_BY_FOLIO` population:

- total `CHAINED_BY_FOLIO` rows: `86`
- rows with entity/trust-style names: `25`
- affected cases: `17`

That is the better place to apply:

- corporate alias expansion
- trust-name normalization
- related-entity matching
- officer / party-name backreferences

The missing-party bucket is usually failing before that stage because one side
of the deed hop is absent.

## Full Case Inventory

Format:

- `case_number`
- `missing_party_rows`
- `both_blank_rows`
- `prev_grantee_blank_rows`
- `title_summary.gap_count`

| Case | Missing Party Rows | Both Blank | Prev Grantee Blank | Gap Count |
| --- | ---: | ---: | ---: | ---: |
| `292019CC066831A001HC` | 1 | 0 | 1 | 1 |
| `292020CA004142A001HC` | 1 | 0 | 1 | 1 |
| `292021CA008197A001HC` | 1 | 0 | 1 | 3 |
| `292021CA010036A001HC` | 1 | 0 | 1 | 3 |
| `292023CA012693A001HC` | 1 | 0 | 1 | 1 |
| `292023CA013106A001HC` | 2 | 1 | 1 | 2 |
| `292023CA013557A001HC` | 1 | 0 | 1 | 5 |
| `292023CA013582A001HC` | 2 | 2 | 0 | 2 |
| `292024CA000010A001HC` | 2 | 1 | 1 | 3 |
| `292024CA000333A001HC` | 2 | 1 | 1 | 3 |
| `292024CA001772A001HC` | 1 | 0 | 1 | 2 |
| `292024CA003253A001HC` | 2 | 1 | 1 | 2 |
| `292024CA005958A001HC` | 2 | 1 | 1 | 3 |
| `292024CA006767A001HC` | 2 | 1 | 1 | 4 |
| `292024CC067263A001HC` | 2 | 1 | 1 | 3 |
| `292025CA004839A001HC` | 2 | 1 | 1 | 4 |
| `292025CA006599A001HC` | 1 | 0 | 1 | 2 |
| `292025CA007403A001HC` | 1 | 0 | 1 | 1 |
| `292025CA007991A001HC` | 2 | 1 | 1 | 2 |
| `292025CA008140A001HC` | 1 | 0 | 1 | 2 |
| `292025CA008465A001HC` | 2 | 1 | 1 | 2 |
| `292025CA008518A001HC` | 2 | 1 | 1 | 3 |
| `292025CA012216A001HC` | 1 | 0 | 1 | 2 |
| `292025CC025710A001HC` | 1 | 0 | 1 | 1 |
| `292025CC032673A001HC` | 1 | 0 | 1 | 1 |
| `292025CC054920A001HC` | 1 | 0 | 1 | 2 |

## Row-Level Inventory

Format:

- `case_number#sequence`
- `sale_date`
- `instrument_number`
- `cause_bucket`
- `grantor`
- `prev_grantee`
- `current grantee`

| Row | Sale Date | Instrument | Cause | Grantor | Prev Grantee | Current Grantee |
| --- | --- | --- | --- | --- | --- | --- |
| `292025CA006599A001HC#2` | `1994-09-01` | `94237836` | `prev_grantee_blank` | `CATO MILLAGE R` |  | `BAILEY BERNICE C; CATO MILLAGE R; CATO MINNIE L` |
| `292025CC032673A001HC#2` | `2002-07-03` | `2002226249` | `prev_grantee_blank` | `CORY LAKES LTD` |  | `WINDWARD HOMES INC` |
| `292025CA007991A001HC#2` | `1982-11-01` |  | `both_blank` |  |  |  |
| `292025CA007991A001HC#3` | `1986-08-01` | `86184838` | `prev_grantee_blank` | `NICARAGUAN FREEDOM CORP` |  | `WILSON EVA M` |
| `292025CC025710A001HC#2` | `1996-08-01` | `96198799` | `prev_grantee_blank` | `SCARBOROUGH IRENE` |  | `SCARBOROUGH IRENE` |
| `292020CA004142A001HC#2` | `1993-05-01` | `93101762` | `prev_grantee_blank` | `CORY LAKES LTD; NORTHEAST DEVELOPMENT CO` |  | `CAMPBELL WILLIAM CRAIG; LEAVITT VIRGINIA C` |
| `292025CA004839A001HC#2` | `1985-02-01` |  | `both_blank` |  |  |  |
| `292025CA004839A001HC#3` | `1996-12-01` | `96325003` | `prev_grantee_blank` | `ROBERTS PAUL D` |  | `MILLER J TRU` |
| `292025CA008465A001HC#2` | `1984-04-01` |  | `both_blank` |  |  |  |
| `292025CA008465A001HC#3` | `1996-05-01` | `96107922` | `prev_grantee_blank` | `ROBERTS SANDRA ROHRBACK` |  | `ROHRBACK SAMUEL T JR` |
| `292025CA008518A001HC#2` | `1985-11-01` |  | `both_blank` |  |  |  |
| `292025CA008518A001HC#3` | `1987-01-01` | `87016989` | `prev_grantee_blank` | `WEISIGER JACK C` |  | `HERNDON EDWARD F; HERNDON LILLIAN M` |
| `292024CA001772A001HC#2` | `1988-04-01` | `88088169` | `prev_grantee_blank` | `LASH LAURA LYNN; LASH RONALD G` |  | `RAMSEY CURTIS R` |
| `292023CA013582A001HC#2` | `1979-06-01` |  | `both_blank` |  |  |  |
| `292023CA013582A001HC#3` | `1980-01-01` |  | `both_blank` |  |  | `PALMER CHASE E` |
| `292024CA006767A001HC#2` | `1984-08-01` |  | `both_blank` |  |  |  |
| `292024CA006767A001HC#3` | `1995-10-01` | `95255846` | `prev_grantee_blank` | `VONDERFLUE INGA; VONDERFLUE WILLIAM F` |  | `FOWLKES ANN; FOWLKES WILLIAM` |
| `292024CC067263A001HC#2` | `1981-04-01` |  | `both_blank` |  |  |  |
| `292024CC067263A001HC#3` | `1989-04-01` | `89070305` | `prev_grantee_blank` | `RANSON ELIZABETH A; RANSON JAMES E` |  | `MANGAN ELDA H; MANGAN JAMES J` |
| `292023CA013106A001HC#2` | `1982-04-01` |  | `both_blank` |  |  |  |
| `292023CA013106A001HC#3` | `1992-11-01` | `92261202` | `prev_grantee_blank` | `SMITH BEVERLY M; SMITH JAMES T` |  | `SMITH BEVERLY M` |
| `292023CA013557A001HC#2` | `1991-01-01` | `91001825` | `prev_grantee_blank` | `VEGA YVONNE E` |  | `CLARDY JUDY M; CLARDY WINSTON C` |
| `292024CA000333A001HC#2` | `1982-10-01` |  | `both_blank` |  |  |  |
| `292024CA000333A001HC#3` | `1994-05-01` | `94125901` | `prev_grantee_blank` | `BISHOP DOROTHY R; BISHOP JAMES F; BISHOP JAMES FRANK` |  | `BISHOP DOROTHY RAILEY TRU; BISHOP JAMES FRANK & DOROTHY RAILEY TRUST` |
| `292023CA012693A001HC#2` | `2002-06-25` | `2002220490` | `prev_grantee_blank` | `MITCHELL ROSE M` |  | `TRICE JAMES H` |
| `292021CA010036A001HC#2` | `1989-09-01` | `89206237` | `prev_grantee_blank` | `GONZALEZ GILDA J; RECORD BONNIE J` |  | `GONZALEZ GILDA J` |
| `292024CA003253A001HC#2` | `1984-04-01` |  | `both_blank` |  |  |  |
| `292024CA003253A001HC#3` | `1996-05-01` | `96107922` | `prev_grantee_blank` | `ROBERTS SANDRA ROHRBACK` |  | `ROHRBACK SAMUEL T JR` |
| `292019CC066831A001HC#2` | `1988-02-01` | `88035346` | `prev_grantee_blank` | `GENERAL HOMES CORP` |  | `NAMESNIK HELEN K; NAMESNIK IVAN H` |
| `292021CA008197A001HC#2` | `1990-05-01` | `90094350` | `prev_grantee_blank` | `DONALDSON THOMAS` |  | `EHLERS PATRICIA` |
| `292024CA000010A001HC#2` | `1980-11-01` |  | `both_blank` |  |  |  |
| `292024CA000010A001HC#3` | `1987-04-01` | `87084561` | `prev_grantee_blank` | `GODEN DONALD W` |  | `GODEN DEBORAH C` |
| `292025CA007403A001HC#2` | `1987-09-01` | `87221094` | `prev_grantee_blank` | `HOLSTEIN DONNA A; HOLSTEIN ENRICO M` |  | `HENRY SHARON A` |
| `292025CA008140A001HC#2` | `1987-03-01` | `87061858` | `prev_grantee_blank` | `FIVE STAR HOMES INC` |  | `HOOKER HOMES INC` |
| `292024CA005958A001HC#2` | `1981-09-01` |  | `both_blank` |  |  |  |
| `292024CA005958A001HC#3` | `2000-11-17` | `2000340451` | `prev_grantee_blank` | `CREASONN WALTER P` |  | `SCHRAMM MICHAEL C` |
| `292025CA012216A001HC#2` | `1988-03-01` | `88059691` | `prev_grantee_blank` | `KAPLAN STANLEY M` |  | `ARONOW MICHAEL` |
| `292025CC054920A001HC#2` | `1989-03-01` | `89049885` | `prev_grantee_blank` | `EVERETT LINDA C; EVERETT PAUL DAVID; FLORIDA STATE LABOR & EMPL SEC; HILLSBOROUGH COUNTY CLK` |  | `FLORIDA RETAIL FEDERATION SELF INSURERS FUND` |

## Interpretation

The current `MISSING_PARTY` inventory says:

1. The dominant defect is missing prior-owner text in the earliest deed hops
2. The missing-side problem is usually upstream of fuzzy matching
3. Many rows are too under-specified for current ORI search to recover
4. Address normalization will not solve blank prior-owner rows
5. Name normalization only helps when there is a name to normalize

This is why the current bucket is mostly a source-completeness problem, not a
matching-threshold problem.

## Potential Tooling Under Consideration

The following normalization tools are under consideration for pipeline
improvements.

### Address Normalization

1. `usaddress-scourgify`
   - Best fit for USPS-style normalization
   - Built on `usaddress`
   - Useful for standardizing street suffixes, units, and canonical address
     forms across sources
2. `usaddress`
   - NLP parser for US address strings
   - Good for breaking raw address text into components
   - Does not normalize by itself
3. `libpostal` / `pypostal`
   - Strongest general parser/normalizer
   - International and robust against messy formats
   - Heavy operational cost because it requires compiling the library and a
     large model download

### Name Normalization

1. `probablepeople`
   - Best fit for this codebase among lightweight name tools
   - Handles both people and company names
   - Good candidate for semicolon-delimited owner strings, trust/person splits,
     and structured name parsing before matching
2. `python-nameparser`
   - Simpler and rule-based
   - Best for person-name cleanup only
   - Less useful than `probablepeople` for mixed person/entity deed parties

## Fit Assessment For This Pipeline

### What These Tools Could Help

- market-data address matching
- ORI property matching across messy address strings
- normalization of semicolon-delimited deed parties
- trust / person / company token cleanup before fuzzy matching
- better `CHAINED_BY_FOLIO` matching when names exist but are formatted poorly

### What These Tools Will Not Fix By Themselves

- blank `prev_grantee` on the prior sale row
- `both_blank` historical sale rows
- missing historical deed instrument numbers
- absent source records in `hcpa_allsales`
- missing deed recoveries when search inputs are already under-specified

## Recommended Next Steps

### Highest-Value Work

1. Strengthen title-break recovery for early historical sales with missing deed
   numbers
2. Add alternate recovery paths for sale rows with blank parties:
   - ORI by folio/date window
   - ORI by book/page when present
   - deed-image OCR when metadata is incomplete
3. Preserve and expand overlay backfills in `foreclosure_title_events`

### Sunbiz Work

1. Expand the Sunbiz load so `sunbiz_entity_*` is actually broad enough to be
   useful
2. Use Sunbiz aliases in `pg_title_break_service.py` search expansion
3. Use Sunbiz-aware corporate alias normalization in
   `pg_title_chain_controller.py` for `CHAINED_BY_FOLIO`, not as the primary
   fix for current `MISSING_PARTY`

### Normalization Tooling

1. `probablepeople` is the best near-term name parser to test
2. `usaddress-scourgify` is the best near-term address normalizer to test
3. `libpostal` should be treated as a heavier second-stage option if the
   lighter tools do not materially improve recovery

## Bottom Line

The current `MISSING_PARTY` bucket is mostly not an LLC-alias problem. It is an
early-historical-deed completeness problem.

Sunbiz should still be integrated, but it is a better lever for
`CHAINED_BY_FOLIO` than for the current `MISSING_PARTY` population.

For this bucket, the main win will come from better historical deed recovery
and better overlay backfill of missing sale parties.
