#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

DRY_RUN=false
DIR_ARG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir) DIR_ARG="$2"; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    --help|-h)
      cat <<'USAGE'
Usage: upload-directory.sh [--dir PATH] [--dry-run]

Uploads every regular file under a directory through connector-intake POST /uploads/raw.
The directory can come from --dir or INTAKE_E2E_SOURCE_DIR.
Set INTAKE_E2E_MAX_FILES to a positive number to cap the run.
USAGE
      exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

load_env
require_datasource_credentials
require_cmd find
require_cmd sort

dir="${DIR_ARG:-${INTAKE_E2E_SOURCE_DIR:-}}"
[[ -n "$dir" ]] || die "provide --dir or set INTAKE_E2E_SOURCE_DIR"
[[ -d "$dir" ]] || die "directory does not exist: $dir"

export INTAKE_E2E_SOURCE_DIR="$dir"
max_files="${INTAKE_E2E_MAX_FILES:-0}"
count=0

info "Uploading files under $dir"
if [[ "$DRY_RUN" == "true" ]]; then
  info "Dry run: no uploads will be sent"
fi

while IFS= read -r -d '' file; do
  count=$((count + 1))
  echo
  echo "=== [$count] ${file#$dir/} ==="
  args=(--file "$file")
  if [[ "$DRY_RUN" == "true" ]]; then
    args+=(--dry-run)
  fi
  ENV_FILE="${ENV_FILE:-$DEFAULT_ENV_FILE}" "$SCRIPT_DIR/upload-one.sh" "${args[@]}"

  if [[ "$max_files" =~ ^[0-9]+$ && "$max_files" -gt 0 && "$count" -ge "$max_files" ]]; then
    info "Reached INTAKE_E2E_MAX_FILES=$max_files"
    break
  fi
done < <(find "$dir" -type f ! -path '*/.*/*' -print0 | sort -z)

echo
echo "Processed files: $count"
