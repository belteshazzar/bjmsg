#!/usr/bin/env bash
# Shared settings for the five demo terminals. Sourced, not run.

set -euo pipefail

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$DEMO_DIR")"

BJMSG="${BJMSG:-$ROOT/bin/bjmsg}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8080}"
URL="${URL:-http://$HOST:$PORT}"
SUBJECT="${SUBJECT:-greet}"
DATA_DIR="${DATA_DIR:-$DEMO_DIR/data}"

if [ ! -x "$BJMSG" ]; then
    echo "demo: $BJMSG not built yet — run 'make' in $ROOT first." >&2
    exit 1
fi

# Colour, but only when talking to a terminal.
if [ -t 1 ]; then
    C_RESET=$'\033[0m'; C_DIM=$'\033[2m'; C_BOLD=$'\033[1m'
else
    C_RESET=''; C_DIM=''; C_BOLD=''
fi

now() { date +%H:%M:%S; }
