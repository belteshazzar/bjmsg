#!/usr/bin/env bash
# Terminal 3 — subscriber "bob".
exec "$(dirname "${BASH_SOURCE[0]}")/_subscriber.sh" bob 35
