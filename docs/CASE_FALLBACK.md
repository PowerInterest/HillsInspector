# Case Number Format Fallback for ORI Judgment Search

## Problem

The Hillsborough County auction website uses a long case number format:

```
292016CA007158A001HC
│ │    │  │      │
│ │    │  │      └─ Court suffix (HC = Hillsborough County)
│ │    │  └──────── Sequential number within year
│ │    └─────────── Court type (CA = Circuit Court, CC = County Court)
│ └────────────────  4-digit year
└──────────────────  County code (29 = Hillsborough)
```

The ORI (Official Records Index) at `publicaccess.hillsclerk.com` indexes documents using **two different case number formats** depending on when they were recorded:

| Format | Example | Used By |
|--------|---------|---------|
| Full auction format | `292016CA007158A001HC` | Recent filings (2024+) |
| Short clerk format | `16-CA-007158` | Older filings |

When `search_judgment_by_case_number()` searched only the full format, older cases returned only recent procedural orders (motions to reset sale, etc.) but **not** the original Final Judgment, which was indexed under the short format.

## Discovery

Case `292016CA007158A001HC` (Wells Fargo vs Bing Harmon Ross) consistently returned 4 `(ORD) ORDER` documents and no `(JUD) JUDGMENT`. OCR of the ORDER PDFs revealed the clerk uses `16-CA-007158` as the case number. Searching ORI with that short format returned 4 documents including the 15-page Final Judgment from 2017.

```
Full format  "292016CA007158A001HC" -> 4 docs: [ORD, ORD, ORD, ORD]
Short format "16-CA-007158"         -> 4 docs: [ORD, ORD, JUD, LP]
```

## Fix

Added to `src/scrapers/auction_scraper.py`:

1. **`_to_short_case_number()`** — converts `292016CA007158A001HC` to `16-CA-007158` via regex:
   ```
   Pattern: 29(\d{4})(CA|CC)(\d{6}) -> {year[2:]}-{court}-{num}
   ```

2. **Fallback in `search_judgment_by_case_number()`** — after the initial ORI search with the full case number, if no `(JUD)` or `(FJ)` document is found, automatically retries with the short format before giving up.

## Scope

This primarily affects older cases (pre-2024) where the judgment was recorded years before the current auction scheduling. The short format is the standard clerk format used throughout the Hillsborough County court system; the long format with county prefix and suffix is specific to the auction website.
