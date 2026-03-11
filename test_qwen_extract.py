"""Test Qwen 3.5-9B extraction with json_object mode (not guided_json).

Uses response_format=json_object which guarantees valid JSON but doesn't
enforce schema shape — Pydantic does that after.  Much cheaper than
guided_json which builds a grammar FSM from the full schema.
"""

import json
from pathlib import Path
import tempfile
import time

from openai import OpenAI

from src.models.judgment_extraction import JudgmentExtraction

client = OpenAI(
    base_url="http://192.168.86.26:6969/v1",
    api_key="dummy",
    timeout=600.0,
)

tmp_dir = Path(tempfile.gettempdir())
ocr_path = tmp_dir / "navy_fed_ocr.txt"
response_path = tmp_dir / "qwen_raw_response.txt"

# Load OCR text
with ocr_path.open() as f:
    ocr_text = f.read()

# Get schema for prompt embedding (compact)
schema = JudgmentExtraction.model_json_schema()
schema_str = json.dumps(schema, indent=None, separators=(",", ":"))

system_prompt = f"""You are a title examiner extracting structured data from a Florida Final Judgment of Foreclosure (13th Judicial Circuit, Hillsborough County).

OUTPUT: A single JSON object conforming to this schema. Include EVERY key even if the value is null.

RULES:
- Dollar amounts as numbers (123456.78), not strings
- Dates as YYYY-MM-DD
- Legal descriptions VERBATIM from document
- If unclear or absent, use null - NEVER GUESS
- CAPTURE EVERY DEFENDANT - missing one means their lien survives
- The presiding judge is NOT a defendant
- UNKNOWN TENANT entries ARE defendants (type: tenant)
- Confidence 0.0-1.0 based on OCR quality and extraction certainty
- Do NOT include any explanation, only the JSON object

JSON SCHEMA:
{schema_str}"""

print(f"System prompt: {len(system_prompt)} chars")
print(f"OCR text: {len(ocr_text)} chars")
print(f"Total input: ~{(len(system_prompt) + len(ocr_text)) // 4} tokens (approx)")
print()

print("Sending to qwen/qwen3.5-9b with response_format=json_schema...")
start = time.time()

completion = client.chat.completions.create(
    model="qwen/qwen3.5-9b",
    messages=[
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Extract structured data from this Final Judgment OCR text:\n\n{ocr_text}"},
    ],
    response_format={
        "type": "json_schema",
        "json_schema": {
            "name": "JudgmentExtraction",
            "schema": schema,
            "strict": True,
        },
    },
    temperature=0.1,
    max_tokens=8000,
)

elapsed = time.time() - start
content = completion.choices[0].message.content
print(f"Response in {elapsed:.1f}s ({len(content)} chars)")
print(f"Usage: {completion.usage}")

# Save raw response
with response_path.open("w") as f:
    f.write(content)

# Parse and validate
raw = json.loads(content)
print(f"\nRaw JSON keys ({len(raw)}): {sorted(raw.keys())[:20]}...")

try:
    obj = JudgmentExtraction.model_validate(raw)
    print("\nPydantic validation: PASSED")
except Exception as e:
    print(f"\nPydantic validation: FAILED (hard gate) - {e}")
    # Show raw values for inspection even when hard gate fires
    print("\n=== RAW VALUES (pre-validation) ===")
    for k in sorted(raw.keys()):
        v = raw[k]
        if isinstance(v, str) and len(v) > 150:
            v = v[:150] + "..."
        print(f"  {k}: {v}")
    import sys
    sys.exit(0)

failures, warnings = obj.validate_extraction()

print(f"\n=== EXTRACTION ===")
print(f"plaintiff: {obj.plaintiff}")
print(f"case_number: {obj.case_number}")
print(f"judge_name: {obj.judge_name}")
print(f"judgment_date: {obj.judgment_date}")
print(f"total_judgment_amount: {obj.total_judgment_amount}")
print(f"principal_amount: {obj.principal_amount}")
print(f"interest_amount: {obj.interest_amount}")
print(f"per_diem_rate: {obj.per_diem_rate}")
print(f"per_diem_interest: {obj.per_diem_interest}")
print(f"attorney_fees: {obj.attorney_fees}")
print(f"court_costs: {obj.court_costs}")
print(f"escrow_advances: {obj.escrow_advances}")
print(f"late_charges: {obj.late_charges}")
print(f"title_search_costs: {obj.title_search_costs}")
print(f"other_costs: {obj.other_costs}")
print(f"foreclosure_type: {obj.foreclosure_type}")
print(f"sale_date: {obj.foreclosure_sale_date}")
print(f"sale_location: {obj.sale_location}")
print(f"plaintiff_max_bid: {obj.plaintiff_maximum_bid}")
print(f"is_thin: {obj.is_thin()}")
print(f"confidence: {obj.confidence_score}")

print(f"\n--- Defendants ({len(obj.defendants)}) ---")
for d in obj.defendants:
    print(f"  {d.name} ({d.party_type}, federal={d.is_federal_entity})")

print(f"\n--- Property ---")
print(f"address: {obj.property_address}")
print(f"subdivision: {obj.subdivision}")
print(f"lot: {obj.lot}, block: {obj.block}, unit: {obj.unit}")
print(f"plat_book: {obj.plat_book}, plat_page: {obj.plat_page}")
legal = obj.legal_description or ""
print(f"legal: {legal[:200]}" + ("..." if len(legal) > 200 else ""))

print(f"\n--- Foreclosed Mortgage ---")
fm = obj.foreclosed_mortgage
if fm:
    print(f"  date: {fm.original_date}, amount: {fm.original_amount}")
    print(f"  instrument: {fm.instrument_number}, book/page: {fm.recording_book}/{fm.recording_page}")
    print(f"  lender: {fm.original_lender}, holder: {fm.current_holder}")
else:
    print("  (none)")

print(f"\n--- Lis Pendens ---")
lp = obj.lis_pendens
if lp:
    print(f"  instrument: {lp.instrument_number}, book/page: {lp.recording_book}/{lp.recording_page}, date: {lp.recording_date}")
else:
    print("  (none)")

print(f"\n--- Red Flags ({len(obj.red_flags)}) ---")
for rf in obj.red_flags:
    print(f"  [{rf.severity}] {rf.flag_type}: {rf.description}")

print(f"\n--- Validation ---")
print(f"Failures: {len(failures)}")
for f in failures:
    print(f"  FAIL: {f}")
print(f"Warnings: {len(warnings)}")
for w in warnings:
    print(f"  WARN: {w}")
