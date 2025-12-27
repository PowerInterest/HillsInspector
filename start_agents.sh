#!/bin/bash

# Configuration
PROJECT_DIR="/home/user/code/HillsInspector"
DISTRO="Ubuntu-24.04"
WT_PATH="/mnt/c/Users/jdtru/AppData/Local/Microsoft/WindowsApps/wt.exe"
WSL_PATH="/mnt/c/WINDOWS/system32/wsl.exe"

echo "Starting Codex and Claude in new WSL terminals..."

# Launch Codex Tab
"$WT_PATH" nt -p "$DISTRO" "$WSL_PATH" -d "$DISTRO" bash -c "cd $PROJECT_DIR && source .venv/bin/activate && codex --dangerously-bypass-approvals-and-sandbox --search true; exec bash"

# Launch Claude Tab
"$WT_PATH" nt -p "$DISTRO" "$WSL_PATH" -d "$DISTRO" bash -c "cd $PROJECT_DIR && source .venv/bin/activate && claude --dangerously-skip-permissions; exec bash"

echo "Terminals launched."
