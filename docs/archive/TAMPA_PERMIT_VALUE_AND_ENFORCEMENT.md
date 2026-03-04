## Tampa Permit Value And Enforcement

This guide documents two Tampa Accela behaviors that affect permit-gap analysis.

### 1. Enforcement records must be retained

`tampa_accela_records` is the system of record for all Tampa Accela rows we observe for a property, not only building permits. That includes:

- `Enforcement`
- `Complaint`
- `Civil Case`
- `Code Case`

Those rows are operationally important and must remain queryable for foreclosure review.

The controller should not, however, treat enforcement rows as permit-value evidence. Many of them legitimately have no project valuation.

Current rule in `src/services/pg_pipeline_controller.py`:

- keep all Tampa rows in `tampa_accela_records`
- exclude `is_violation = true` rows from `tampa_total` and `tampa_with_value` when scoring single-pin permit candidates
- exclude Business / tax-receipt rows from those same scoring counts

That Business exclusion currently covers:

- `module = 'Business'`
- `record_number LIKE 'BTX-%'`
- `record_type ILIKE 'Tax Receipt%'`

That preserves enforcement and business-license history without letting non-permit activity crowd out true permit-coverage gaps.

### 2. Tampa valuation labels are not stable

Accela detail pages do not always present valuation under the same label. We currently treat all of these as valid value labels in `src/services/TampaPermit.py`:

- `Job Value`
- `Total Project Value`
- `Project Value`
- `Valuation`
- `Estimated Work Cost`
- `Estimated Cost`

The detail enrichment flow should:

1. Query `GlobalSearchResults.aspx` by exact record number.
2. Parse any inline detail fields if the search resolves directly to a detail page.
3. If a `CapDetail.aspx` URL is present and inline fields are incomplete, fetch that detail page and merge the missing fields.

This is necessary because some Accela record searches resolve directly to `CapDetail`, while others render an intermediate search-results page first.

### Practical interpretation

If a property has only Tampa enforcement activity:

- we keep those rows
- the property can still be a permit-gap candidate if there is no non-violation permit coverage

If a property has Tampa permit rows but missing value:

- first treat it as a detail-enrichment/parsing issue
- only after enrichment should it remain in any value-specific backlog
