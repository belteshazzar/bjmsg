#!/usr/bin/env bash
# Terminal 1 — the broker.
#
# Equivalent of `nats-server`. Runs in the foreground; Ctrl-C stops it.

source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

echo "${C_BOLD}bjmsg broker${C_RESET}"
echo "${C_DIM}store: $DATA_DIR${C_RESET}"
echo "${C_DIM}(delete that directory, or run demo/reset.sh, to start clean)${C_RESET}"
echo

exec "$BJMSG" serve --host "$HOST" --port "$PORT" --dir "$DATA_DIR"
