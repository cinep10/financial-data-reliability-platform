#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <access.log|parsed.tsv> [--base-url URL] [--db-host H] [--db-port P] [--db-user U] [--db-pass PW] [--db-name DB]" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
INPUT="$1"
shift

BASE_URL=""
DB_HOST="127.0.0.1"
DB_PORT="3306"
DB_USER="root"
DB_PASS=""
DB_NAME="test"
OUT_TSV=""
INPUT_IS_TSV=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-url) BASE_URL="${2:-}"; shift 2 ;;
    --db-host) DB_HOST="${2:-}"; shift 2 ;;
    --db-port) DB_PORT="${2:-}"; shift 2 ;;
    --db-user) DB_USER="${2:-}"; shift 2 ;;
    --db-pass) DB_PASS="${2:-}"; shift 2 ;;
    --db-name) DB_NAME="${2:-}"; shift 2 ;;
    --out) OUT_TSV="${2:-}"; shift 2 ;;
    --input-is-tsv) INPUT_IS_TSV=1; shift 1 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

if [[ "$INPUT" == *.tsv ]]; then
  INPUT_IS_TSV=1
fi

if [[ -z "$OUT_TSV" ]]; then
  if [[ $INPUT_IS_TSV -eq 1 ]]; then
    OUT_TSV="$INPUT"
  else
    OUT_TSV="${INPUT}.tsv"
  fi
fi

if [[ $INPUT_IS_TSV -eq 0 ]]; then
  "$SCRIPT_DIR/parse_webserver_log.sh" "$INPUT" "$OUT_TSV" ${BASE_URL:+--base-url "$BASE_URL"}
fi

exec "$PYTHON_BIN" "$SCRIPT_DIR/load_tsv_to_db.py" \
  --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --password "$DB_PASS" --db "$DB_NAME" \
  --table stg_webserver_log_hit \
  --tsv "$OUT_TSV" \
  --columns "dt,ts,ip,method,url_raw,url_full,url_norm,host,path,query,status,bytes,latency_ms,ref,ref_host,ua,kv_raw,uid,pcid,sid,device_type,evt,accept_lang,cc,page_type" \
  --fallback-insert
