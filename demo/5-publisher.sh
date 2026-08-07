#!/usr/bin/env bash
# Terminal 5 — the publisher.
#
# Equivalent of `nats pub greet "Hello NATS!"`.
#
#   ./5-publisher.sh                 publish a scripted series
#   ./5-publisher.sh "some message"  publish one message
#   ./5-publisher.sh --loop          publish every 2s until Ctrl-C

source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

publish() {
    local text="$1"
    local reply
    reply="$("$BJMSG" pub --url "$URL" "$SUBJECT" "$text")"
    printf '%s %sPublished%s %d bytes to "%s"  %s%s%s\n' \
        "$(now)" "$C_BOLD" "$C_RESET" "${#text}" "$SUBJECT" \
        "$C_DIM" "$reply" "$C_RESET"
}

echo "${C_BOLD}publisher${C_RESET} -> $URL, subject ${C_BOLD}$SUBJECT${C_RESET}"
echo

case "${1:-}" in
    --loop)
        n=1
        while true; do
            publish "Hello bjmsg! #$n"
            n=$((n + 1))
            sleep 2
        done
        ;;
    "")
        for text in \
            "Hello NATS!" \
            "...except this one is binjson over HTTP/1.1" \
            "every subscriber gets its own copy" \
            "and the broker pushed it to you, you never asked" \
            "goodbye"
        do
            publish "$text"
            sleep 1.5
        done
        echo
        echo "${C_DIM}Done. Re-run, or try: ./5-publisher.sh --loop${C_RESET}"
        ;;
    *)
        publish "$*"
        ;;
esac
