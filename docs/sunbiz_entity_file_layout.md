# Sunbiz Entity File Layout (COR + GEN)

Last verified: February 19, 2026.

## Official Sources

- Data Usage Guide: https://dos.fl.gov/sunbiz/other-services/data-downloads/data-usage-guide/
- Corporate definitions: https://dos.sunbiz.org/data-definitions/cor.html
- General partnership definitions: https://dos.sunbiz.org/data-definitions/gen.html

## Files Needed for LLCs, Partnerships, Companies

Quarterly baseline:
- `/public/doc/quarterly/Cor/cordata.zip`
- `/public/doc/quarterly/Cor/corevt.zip`
- `/public/doc/quarterly/Gen/Genfile.zip`
- `/public/doc/quarterly/Gen/Genevt.zip`
- `/public/doc/quarterly/Non-Profit/npcordata.zip` (optional if you also want nonprofit entities)

## Record Lengths

- Corporate Data (`cordata*.txt`): `1440`
- Corporate Events (`corevt.txt`): `662`
- General Partnership Data (`GENFILE.TXT`): `759`
- General Partnership Events (`GENEVT.TXT`): `910`

## Key Columns Used by Loader

### Corporate Data (selected)
- `doc_number`: start `1`, len `12`
- `entity_name`: start `13`, len `192`
- `status`: start `205`, len `1`
- `filing_type`: start `206`, len `15`
- principal address block: starts `221`
- mailing address block: starts `347`
- `filed_date`: start `473`, len `8`
- `fei_number`: start `481`, len `14`
- officer slots (6): start `669`, block size `128`

### Corporate Events (selected)
- `event_doc_number`: `1/12`
- `event_sequence`: `13/5`
- `event_code`: `18/20`
- `event_description`: `38/40`
- `event_effective_date`: `78/8`
- `event_filing_date`: `86/8`
- `event_name`: `211/192`

### General Partnership Data (selected)
- `doc_number`: `1/12`
- `status`: `13/1`
- `entity_name`: `14/192`
- `filed_date`: `206/8`
- `effective_date`: `214/8`
- `cancellation_date`: `222/8`
- `expiration_date`: `752/8`
- partner fields: start near `501` (`name` at `515/55`, `seq` at `570/5`)

### General Partnership Events (selected)
- `event_doc_number`: `1/12`
- `event_orig_doc_number`: `13/12`
- `event_sequence`: `25/5`
- `event_code`: `30/20`
- `event_description`: `50/40`
- `event_effective_date`: `95/8`
- `event_filing_date`: `103/8`
- `event_name`: `249/192`

## Database Strategy

Use multiple tables:
- `sunbiz_entity_filings` (1 row per entity filing doc)
- `sunbiz_entity_parties` (officers/partners)
- `sunbiz_entity_events` (event timeline)

This supports clean upserts, avoids denormalized duplication, and allows a separate materialized "current snapshot" later for fast UI reads.
