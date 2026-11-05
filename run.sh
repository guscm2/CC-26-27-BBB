#!/bin/sh
here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd) || exit 1
exec python3 "$here/src/main.py" "$@"
