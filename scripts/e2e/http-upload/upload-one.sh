#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

DRY_RUN=false
FILE_ARG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --file) FILE_ARG="$2"; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    --help|-h)
      cat <<'USAGE'
Usage: upload-one.sh [--file PATH] [--dry-run]

Uploads one file through connector-intake POST /uploads/raw.
The file path can come from --file or INTAKE_E2E_SOURCE_FILE.
USAGE
      exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

load_env
require_datasource_credentials
require_cmd curl
require_cmd awk
require_cmd sha256sum
require_cmd stat

file="${FILE_ARG:-${INTAKE_E2E_SOURCE_FILE:-}}"
[[ -n "$file" ]] || die "provide --file or set INTAKE_E2E_SOURCE_FILE"
[[ -f "$file" ]] || die "file does not exist or is not a regular file: $file"

content_type="${INTAKE_E2E_CONTENT_TYPE:-application/octet-stream}"
source_path="$(source_path_for_file "$file")"
filename="$(basename "$file")"
checksum="$(sha256sum "$file" | awk '{print $1}')"
url="$(url_base)/uploads/raw"

echo "File:          $filename"
echo "Source path:   $source_path"
echo "Size bytes:    $(stat -c%s "$file")"
echo "SHA-256:       $checksum"
echo "Endpoint:      $url"
print_selected_datasource

if [[ "$DRY_RUN" == "true" ]]; then
  echo "Dry run: upload not sent"
  exit 0
fi

curl -fsS \
  -H "x-datasource-id: $DS_ID" \
  -H "x-api-key: $API_KEY" \
  -H "x-source-path: $source_path" \
  -H "x-filename: $filename" \
  -H "x-checksum-sha256: $checksum" \
  -H "content-type: $content_type" \
  --data-binary "@$file" \
  "$url"
echo
