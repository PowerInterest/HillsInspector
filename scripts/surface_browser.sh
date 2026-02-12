#!/usr/bin/env bash
set -euo pipefail

# Opens a surfaced (non-headless) Chrome window and keeps it attached to the
# current terminal session, matching the successful manual launch behavior.
URL="${1:-https://www.redfin.com}"

if command -v google-chrome >/dev/null 2>&1; then
  BROWSER="google-chrome"
elif command -v google-chrome-stable >/dev/null 2>&1; then
  BROWSER="google-chrome-stable"
elif command -v chromium >/dev/null 2>&1; then
  BROWSER="chromium"
elif command -v chromium-browser >/dev/null 2>&1; then
  BROWSER="chromium-browser"
else
  echo "No Chrome/Chromium browser binary found in PATH." >&2
  exit 1
fi

exec "$BROWSER" --new-window "$URL"
