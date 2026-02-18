# Web App Audit — Property Detail Page

**Audited**: 2026-02-17
**Test case**: `292024CA002613A001HC` (4604 W LOWELL AVE, TAMPA, FL 33629)
**Folio**: `1829323T7000014000030A`

---

## Summary

The property detail page has **24 issues** spanning critical data bugs, template rendering errors, and data quality problems. The most severe: the Net Equity calculation shows **-$348K** when the real answer is approximately **+$252K** — a $600K error that would mislead an investor into skipping a profitable deal.

The root causes fall into three categories:
1. **ORI ingestion pollution** — documents and encumbrances from unrelated properties are being ingested because adjacent instrument search (±5 range) sweeps in unrelated docs from the same clerk recording batch without party-name validation
2. **Discovery truncation + satisfaction matching quality** — iterative discovery hard-stops at `MAX_ITERATIONS_PER_FOLIO=50` (`config/step4v2.py:10`, `discovery.py:299`), leaving 134 searches unexecuted for this folio (50 completed, 50 ready, 84 pending). Gap-bounded searches only triggered on `"exhausted"`, not `"max_iterations"`. Additionally, satisfaction matching in `chain_builder._match_satisfactions()` fails on multi-party comma-separated names (e.g. "BORREGO HENRY W, LONGO LEONARD V" vs "BORREGO HENRY W")
3. **Template rendering bugs** — empty cards, eager tab loading, double-escaped HTML, mismatched field names

---

## CRITICAL DATA BUGS

### 1. $600K "SURVIVED" mortgage is actually SATISFIED — Net Equity is WRONG

- **What**: The Borrego/Longo $600K mortgage (2018-07-07, Inst `2018269479`) shows `SURVIVED` status
- **Why it's wrong**: This mortgage was taken by the *prior* owner (Valiente). The DB contains a satisfaction doc from Borrego (Inst `2021365109`). When Friel purchased in 2021, this mortgage was paid off at closing.
- **Impact**: Net Equity = Market ($2.236M) - Judgment ($1.984M) - Surviving Liens ($600K) = **-$348K**. Correct answer: $2.236M - $1.984M = **+$252K**. The page shows "Warning: Negative equity" for a positive-equity deal.
- **Root cause**: Two compounding issues:
  1. **Satisfaction matching fails on multi-party names**: `chain_builder._match_satisfactions()` (line 364) compares the full comma-joined creditor string "BORREGO HENRY W, LONGO LEONARD V" against satisfaction party "BORREGO HENRY W" via `_names_match()` → `NameMatcher.match()` (name_matcher.py:241), which treats the entire comma string as one name and scores too low. Satisfaction matching *does* exist in the chain builder — the problem is matching quality, not absence.
  2. **Discovery truncation hides the Valiente→Friel deed**: The iterative discovery loop hard-stops at 50 iterations (`MAX_ITERATIONS_PER_FOLIO=50`). This folio's search queue has 50 completed, 50 ready, 84 pending — the deed from Valiente→Friel is likely in the unexecuted queue. Gap-bounded searches (line 344) only triggered on `stopped_reason == "exhausted"`, not `"max_iterations"`, so they never ran. With only one chain period (Voight→Valiente), the survival engine assigns all encumbrances to the wrong ownership context.

### 2. 20 garbage encumbrance rows (IDs 1558–1577)

- **What**: 20 `judgment` type encumbrances with NO date, NO amount ($0), NO holder, all `UNCERTAIN`
- **Why**: Created from unrelated documents (STATE OF FLORIDA criminal judgments, DOMESTIC RELATIONS cases, etc.) that share adjacent instrument numbers with real property docs
- **Impact**: Liens tab is 90% noise. User sees 23 rows but only 3 are real.
- **Root cause**: `chain_builder` creates encumbrances from every document tagged to this folio, including the ~30 unrelated docs ingested by ORI

### 3. ~30 unrelated documents in Documents tab (30 of 50)

- **What**: Documents like "DIMAGGIO LISA ANN" judgment, "RIDDLESWORTH ALICIA DIANE" domestic relations, "BOARDWALK AT MORRIS BRIDGE" judgment, "4050 LOFTS" judgment, "AC&S INC" court paper, multiple "FLORIDA STATE" criminal judgments, etc.
- **Pattern**: ORI search found Inst `2015342252` (real deed) and pulled in `2015342242`–`2015342257` (the entire recording batch). Same for `2016051477`, `2018039714`, `2021365112`.
- **Impact**: 50 documents displayed, only ~14 actually relate to this property
- **Root cause**: ORI iterative discovery stores all documents from a search result page without filtering by relevance to the target property. Adjacent instrument numbers from the same clerk recording batch get swept in.

### 4. Chain of title is incomplete — missing current owner

- **What**: Only 1 chain entry: Voight→Valiente (2018, $424K). Missing Valiente→Friel (~2021, $2.315M)
- **Impact**: Current owner not in chain. All encumbrances are assigned to the single (wrong) chain period. Survival analysis runs against wrong ownership context.
- **Evidence**: DB has the Friel mortgage doc (Inst `2021365112`, 2021-07-21) and a Valiente affidavit from the same date — the deed should be nearby but the chain builder didn't find/create the link.
- **Root cause**: **Discovery truncation is the primary cause.** The iterative discovery loop stopped at 50 iterations with 134 searches still in queue (50 ready, 84 pending). The Valiente→Friel deed is likely among those unexecuted searches. Chain builder *does* require a deed to create a transfer period — but the deed was never found because discovery was truncated. Secondary issue: even when a mortgage exists for Friel (Inst `2021365112`), `_build_periods()` only creates entries from deed-type documents, so mortgage-only evidence of ownership change is missed.

### 5. Foreclosed mortgage details all null

- **What**: Despite being a FIRST MORTGAGE foreclosure by Truist Bank, extracted judgment has: `instrument_number: null`, `original_amount: 0`, `recording_date: null`, `original_date: null`
- **Impact**: "Foreclosed Mortgage" card renders completely empty. No way to cross-reference the mortgage in ORI.
- **Root cause**: Vision model (GLM-4.6V-Flash) failed to extract mortgage recording details from the judgment PDF. The judgment PDF likely states these details but the extraction prompt or parsing missed them.

### 6. `other_costs` = $1,979,255.77

- **What**: In extracted judgment JSON, `other_costs` is $1.979M — nearly the full $1.984M judgment amount
- **Impact**: Raw JSON display shows nonsensical number. If any downstream logic uses `other_costs`, calculations would be wildly wrong.
- **Root cause**: Vision extraction error — the model likely dumped the total or a subtotal into the `other_costs` catch-all field

### 7. Sales History tab shows "No sales history found"

- **What**: Sales History section returns empty, but Valuation card shows "Last Sale: $2,315,000 (2021-07-15)"
- **Impact**: Data inconsistency visible on the same page. User sees a last sale price but no sales history.
- **Root cause**: `save_sales_history_from_hcpa()` in `operations.py:1012-1013` skips any sale record where the `instrument` field is empty (`if not instrument: continue`). HCPA vision extraction may not return instrument numbers for all sales — those records are silently dropped. The "Last Sale" in valuation comes from `parcels.last_sale_price` (HCPA bulk data), which is a separate field not dependent on `sales_history` table rows.

---

## TEMPLATE / HTML BUGS

### 8. Empty "Foreclosed Mortgage" card renders with just a header

- **File**: `app/web/templates/partials/judgment.html:99`
- **What**: `{% if jd.foreclosed_mortgage %}` is `True` because the dict exists (has keys with null/0 values). All inner `{% if fm.field %}` checks fail, so only the `<h3>Foreclosed Mortgage</h3>` renders with an empty info-grid.
- **Fix**: Check if any field has meaningful data before rendering the card

### 9. All 6 HTMX tabs load eagerly on page load

- **File**: `app/web/templates/property.html` — all tab divs have `hx-trigger="load"`
- **What**: Every hidden tab fires an HTTP request immediately on page load, even though only "Basic" is visible
- **Impact**: 6 unnecessary HTTP requests + DB queries on every page view
- **Fix**: Use `hx-trigger="intersect once"` or trigger on tab click

### 10. Tab div order doesn't match button order

- **Buttons**: Basic, Judgment, **Chain**, **Liens**, Permits, Tax, Documents
- **Divs**: basic, judgment, **permits**, **liens**, tax, documents, **chain**
- **Impact**: Cosmetic, but could cause confusion in maintenance

### 11. `amount_confidence` shows "ori_api" badge on every encumbrance

- **File**: `app/web/templates/partials/lien_table.html:57`
- **What**: Condition `enc.amount_confidence != 'HIGH'` means `ori_api` (the data source) always shows. This is provenance, not confidence.
- **Impact**: Meaningless badge clutters every row

### 12. Per diem rate not shown in Judgment tab

- **File**: `app/web/templates/partials/judgment.html:89`
- **What**: Template checks `jd.per_diem_interest` but extracted data uses `per_diem_rate` (= 2.75)
- **Impact**: Per diem of $2.75/day is silently hidden

### 13. Lis Pendens date shows "-" despite data existing in nested dict

- **File**: `app/web/templates/partials/judgment.html:31`
- **What**: Template checks `jd.lis_pendens_date` (flat field) but data is at `jd.lis_pendens.recording_date` (nested dict). In this case LP recording_date is also null, but the path is wrong regardless.
- **Impact**: Would hide LP date even when present

### 14. Double-escaped HTML entities

- **What**: Documents tab shows `AC&amp;amp;S INC` and `F &amp;amp; E FLOORING CORP`
- **Root cause**: `&` stored as `&amp;` in the DB, then Jinja2 auto-escapes it again
- **Fix**: Use `|safe` filter or store raw `&` in DB

### 15. No View/Download links for ORI documents

- **What**: Only the filesystem PDF (`final_judgment_2025035846.pdf`) has a download link. The 49 ORI documents have no `file_path` so no link renders.
- **Fix**: Generate ORI viewer links from `ori_id` or `instrument_number`

### 16. Case Number link goes to clerk homepage

- **What**: `<a href="https://hover.hillsclerk.com">292024CA002613A001HC</a>` links to the generic clerk site, not the specific case
- **Fix**: Link to the actual case page or ORI search

### 17. Address has dash formatting issue

- **What**: Shows "TAMPA, FL- 33629" throughout (dash between state and zip)
- **Root cause**: Raw auction data has the dash. Should be cleaned on ingest or display.

### 18. 37/50 documents missing recording dates

- **What**: Only 13 of 50 documents have `recording_date`. The rest render empty `<span class="doc-date"></span>`.
- **Root cause**: ORI batch-neighbor documents were ingested without dates. The ORI API may not return dates for all doc types, or the ingestion doesn't parse them.

### 19. Mixed document_type formats in Documents tab

- **What**: Some normalized (`judgment`, `mortgage`, `deed`) and some raw ORI (`(DRJUD) DOMESTIC RELATIONS JUDGMENT`, `(ORD) ORDER`, `(CP) COURT PAPER`)
- **Root cause**: Type normalizer doesn't handle all ORI doc types. Known issue (see MEMORY.md).

### 20. Foreclosed mortgage card truthiness check

- **File**: `app/web/templates/partials/judgment.html:99–104`
- **What**: `{% if jd.foreclosed_mortgage %}` passes because dict exists. `{% if fm.original_amount %}` correctly hides $0.0 (falsy). But the outer check should verify at least one field has data.
- **Related to**: Issue #8

---

## DATA QUALITY ISSUES

### 21. Encumbrance #1557 has creditor/debtor swapped

- **What**: Creditor = `FRIEL ANTHONY G, FRIEL ANTONY, ... GOODLEAP LLC, LOANPAL LLC` (defendants), Debtor = `TRUIST BANK` (plaintiff). This is backwards.
- **Root cause**: chain_builder assigned party1 (Truist) as debtor and party2 (Friel et al) as creditor for the judgment doc. For judgments, party1 is the plaintiff/creditor.

### 22. Satisfaction doc not linked to mortgage it satisfies

- **What**: Satisfaction from BORREGO HENRY W (Inst `2021365109`) exists in documents table but was never matched to the $600K mortgage (Inst `2018269479`)
- **Impact**: Mortgage still shows as SURVIVED instead of SATISFIED
- **Root cause**: `chain_builder._match_satisfactions()` (line 364) *does* exist and attempts to cross-reference satisfactions against encumbrances. The failure is in `_names_match()`: the mortgage creditor is stored as `"BORREGO HENRY W, LONGO LEONARD V"` (comma-joined multi-party), and the satisfaction party1 is `"BORREGO HENRY W"`. `NameMatcher.match()` treats the full comma string as a single name, so the fuzzy score falls below the 0.85 threshold and no match is found.

### 23. Screenshots accumulate with no cleanup

- **What**: 16 HCPA/permit screenshots across 6+ pipeline runs in `screenshots/` dir (~10MB). Never displayed in UI, never pruned.
- **Impact**: Disk waste, grows with every pipeline run

### 24. 5 duplicate auction parquet files in `consumed/`

- **What**: Same auction scraped 5 times across different dates
- **Impact**: Minor disk waste

---

## DATABASE STATE SUMMARY

| Table | Count | Issues |
|-------|-------|--------|
| `encumbrances` | 23 | 20 are garbage (no date/amount/holder) |
| `documents` | 50 | ~30 are unrelated to property |
| `chain_of_title` | 1 | Missing current owner (Friel, 2021) |
| `sales_history` | 0 | Empty despite known sale in 2021 |

## WEB APP ARCHITECTURE NOTE

The web app at `app/web/database.py` opens raw `sqlite3.connect()` to the main DB instead of using `PropertyDB`. A `property_master_web.db` snapshot mechanism exists (`src/utils/db_snapshot.py`) but is not wired into the property page routes — only `history.py` has a fallback to it. The snapshot files exist on disk but are unused for the main property views.
