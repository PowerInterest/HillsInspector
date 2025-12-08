# Codex Fix Log

- Added restriction/easement detection helpers and taxonomy updates in title analysis so restriction docs count even when only mentioned in OCR/legal text.
- Expanded encumbrance handling to include tax liens; added tax summary helpers.
- Tax scraper now parses on-page balances into `document_type='TAX'` liens and closes the browser after scraping.
- Verified DB sample: copied `data/property_master.db` (main file was locked) and found 3 documents containing easement/restriction keywords (folios `1827349TP000000000370U` instrument `2013149721`; `202935ZZZ000002717700U` instruments `2023303440`, `2023239553`); total documents rows: 53.
