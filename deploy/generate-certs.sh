#!/usr/bin/env bash
set -euo pipefail

echo "Deprecated: use deploy/generate.sh" >&2
exec "$(dirname "$0")/generate.sh" "$@"
