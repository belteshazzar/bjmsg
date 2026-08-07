#!/usr/bin/env bash
# The body of terminals 2, 3 and 4: one named subscriber.
#
# Equivalent of `nats sub greet`. --tail matches NATS core semantics —
# only messages published while this subscriber is running are delivered,
# so the log's backlog is skipped.
#
# DURABLE=1 switches to a read-receipt subscription instead: the broker
# persists how far this subscriber has acknowledged, so stopping it and
# starting it again delivers exactly what it missed. --tail then only
# decides where it joins the very first time.
#
# usage: _subscriber.sh <name> <ansi-colour-code>

source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

NAME="${1:?usage: _subscriber.sh <name> <colour>}"
COLOUR_CODE="${2:-36}"

if [ -t 1 ]; then COLOUR=$'\033['"$COLOUR_CODE"'m'; else COLOUR=''; fi

if [ "${DURABLE:-0}" = "1" ]; then
    SUB_ARGS=(--consumer "$NAME" --tail --follow --interval 100)
    MODE="durable — receipts persisted as ${C_BOLD}$NAME${C_RESET}${C_DIM}, resumes where it left off"
else
    SUB_ARGS=(--tail --follow --interval 100)
    MODE="ephemeral — only what is published from now on"
fi

echo "${COLOUR}${C_BOLD}[$NAME]${C_RESET} $(now) Subscribing on ${C_BOLD}$SUBJECT${C_RESET} at $URL"
echo "${C_DIM}${MODE}${C_RESET}"
echo "${C_DIM}Ctrl-C to stop.${C_RESET}"

# In ephemeral mode the broker holds no subscriber state at all: the
# cursor travels in each request. In durable mode it holds one number per
# subscriber, and the ack rides along with the next poll.
"$BJMSG" sub --url "$URL" "$SUBJECT" "${SUB_ARGS[@]}" |
while IFS=$'\t' read -r index payload; do
    printf '%s%s[%s]%s %s [#%s] Received on "%s"\n  %s\n' \
        "$COLOUR" "$C_BOLD" "$NAME" "$C_RESET" "$(now)" \
        "$index" "$SUBJECT" "$payload"
done
