#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <access.log> [output.tsv] [--base-url URL]" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
IN_LOG="$1"
OUT_TSV="${2:-${IN_LOG}.tsv}"
shift $(( $# >= 2 ? 2 : 1 )) || true

BASE_URL=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-url) BASE_URL="${2:-}"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

exec "$PYTHON_BIN" "$SCRIPT_DIR/parse_webserver_log.py" ${BASE_URL:+--base-url "$BASE_URL"} "$IN_LOG" "$OUT_TSV"
