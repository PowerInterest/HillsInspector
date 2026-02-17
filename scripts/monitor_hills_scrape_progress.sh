#!/usr/bin/env bash
set -u

# Logs a progress line every N seconds for the most recent run directory.
# Usage:
#   scripts/monitor_hills_scrape_progress.sh <output_root_dir> [interval_sec=1200] [log_file]

OUTPUT_ROOT="${1:-}"
INTERVAL_SEC="${2:-1200}"
LOG_FILE="${3:-logs/hills_2y_scrape_progress.log}"

if [[ -z "$OUTPUT_ROOT" ]]; then
  echo "usage: $0 <output_root_dir> [interval_sec=1200] [log_file]" >&2
  exit 2
fi

mkdir -p "$(dirname "$LOG_FILE")"

while true; do
  ts="$(date -Is)"

  # Pick most recent run dir (timestamped child).
  RUN_DIR=$(ls -1dt "$OUTPUT_ROOT"/*/ 2>/dev/null | head -n 1 || true)
  if [[ -z "$RUN_DIR" ]]; then
    echo "$ts run_dir=(none)" | tee -a "$LOG_FILE" >/dev/null
    sleep "$INTERVAL_SEC"
    continue
  fi
  RUN_DIR=${RUN_DIR%/}

  html_count=0
  txt_count=0
  if [[ -d "$RUN_DIR/pages" ]]; then
    html_count=$(find "$RUN_DIR/pages" -maxdepth 1 -type f -name '*.html' 2>/dev/null | wc -l | tr -d ' ')
    txt_count=$(find "$RUN_DIR/pages" -maxdepth 1 -type f -name '*.txt' 2>/dev/null | wc -l | tr -d ' ')
  fi

  photo_files=0
  if [[ -d "$RUN_DIR/photos" ]]; then
    photo_files=$(find "$RUN_DIR/photos" -type f 2>/dev/null | wc -l | tr -d ' ')
  fi

  mm=$(python - <<PY
import re
from datetime import datetime
from pathlib import Path

def strip_ord(s:str)->str:
    return re.sub(r"(\b\d{1,2})(st|nd|rd|th)\b", r"\1", s)

def parse(s:str):
    s=strip_ord(s)
    return datetime.strptime(s.strip(), "%B %d, %Y").date()

dates=[]
p=Path("$RUN_DIR/pages")
if p.exists():
    for txt in p.glob('*.txt'):
        try:
            m=re.search(r"Date Of Auction:\s*(.+)", txt.read_text(encoding='utf-8', errors='ignore'))
        except Exception:
            continue
        if not m:
            continue
        try:
            dates.append(parse(m.group(1)))
        except Exception:
            pass
if dates:
    print(f"{min(dates)}..{max(dates)}")
else:
    print("(none)")
PY
)

  echo "$ts run_dir=$RUN_DIR pages_html=$html_count pages_txt=$txt_count photo_files=$photo_files auction_date_range=$mm" | tee -a "$LOG_FILE" >/dev/null
  sleep "$INTERVAL_SEC"
done
