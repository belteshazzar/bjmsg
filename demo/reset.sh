#!/usr/bin/env bash
# Delete the demo's store so the next run starts from message 1.
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"
rm -rf "$DATA_DIR"
echo "removed $DATA_DIR"
