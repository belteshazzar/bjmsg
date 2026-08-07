#!/usr/bin/env bash
# Optional terminal 6 — watch the read receipts.
#
# Only shows anything once at least one subscriber is running with
# DURABLE=1. `acked` is the last message that subscriber confirmed;
# `lag` is how many published messages it has not reached yet.

source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

echo "${C_BOLD}read receipts${C_RESET} for subject ${C_BOLD}$SUBJECT${C_RESET}"
echo "${C_DIM}refreshing every second; Ctrl-C to stop.${C_RESET}"
echo

while true; do
    printf '%s  %s\n' "$(now)" "$("$BJMSG" consumers --url "$URL" "$SUBJECT" 2>&1)"
    sleep 1
done
