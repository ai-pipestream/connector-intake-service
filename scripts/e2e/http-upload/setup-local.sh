#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

FORCE=false
SOURCE_FILE=""
SOURCE_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-file) SOURCE_FILE="$2"; shift 2 ;;
    --source-dir) SOURCE_DIR="$2"; shift 2 ;;
    --force) FORCE=true; shift ;;
    --help|-h)
      cat <<'USAGE'
Usage: setup-local.sh [--source-file PATH] [--source-dir PATH] [--force]

Writes scripts/e2e/http-upload/.env.local for local HTTP upload smoke tests.

Defaults:
  DS_ID=valid-datasource
  API_KEY=valid-api-key

Use ENV_FILE=/path/to/file to write somewhere else.
If no source path is provided and stdin is interactive, the script prompts for one.
USAGE
      exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

env_file="${ENV_FILE:-$DEFAULT_ENV_FILE}"
if [[ -e "$env_file" && "$FORCE" != "true" ]]; then
  die "$env_file already exists. Pass --force to overwrite it."
fi

if [[ -z "$SOURCE_FILE" && -z "$SOURCE_DIR" && -t 0 ]]; then
  echo "Enter a source file or directory for local upload tests."
  echo "Leave blank to write credentials only."
  read -r -p "Source path: " source_path
  if [[ -n "$source_path" ]]; then
    if [[ -f "$source_path" ]]; then
      SOURCE_FILE="$source_path"
    elif [[ -d "$source_path" ]]; then
      SOURCE_DIR="$source_path"
    else
      die "source path does not exist: $source_path"
    fi
  fi
fi

if [[ -n "$SOURCE_FILE" && ! -f "$SOURCE_FILE" ]]; then
  die "source file does not exist: $SOURCE_FILE"
fi

if [[ -n "$SOURCE_DIR" && ! -d "$SOURCE_DIR" ]]; then
  die "source directory does not exist: $SOURCE_DIR"
fi

SERVICE_HOST="${SERVICE_HOST:-localhost}"
INTAKE_HTTP_PORT="${INTAKE_HTTP_PORT:-18108}"
ACCOUNT_PORT="${ACCOUNT_PORT:-18105}"
REPO_PORT="${REPO_PORT:-18102}"
ADMIN_PORT="${ADMIN_PORT:-18107}"
DS_ID="${DS_ID:-valid-datasource}"
API_KEY="${API_KEY:-valid-api-key}"
ACCOUNT_ID="${ACCOUNT_ID:-valid-account}"
DRIVE_NAME="${DRIVE_NAME:-valid-account:intake}"
LOCAL_BUCKET="${LOCAL_BUCKET:-pipestream}"
DATASOURCE_NAME="${DATASOURCE_NAME:-local-http-upload}"
CONNECTOR_ID="${CONNECTOR_ID:-a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}"
INTAKE_E2E_SOURCE_PREFIX="${INTAKE_E2E_SOURCE_PREFIX:-e2e/http-upload}"
INTAKE_E2E_CONTENT_TYPE="${INTAKE_E2E_CONTENT_TYPE:-application/octet-stream}"
INTAKE_E2E_MAX_FILES="${INTAKE_E2E_MAX_FILES:-1}"

written="$(write_private_env \
  "$(env_assignment SERVICE_HOST "$SERVICE_HOST")" \
  "$(env_assignment INTAKE_HTTP_PORT "$INTAKE_HTTP_PORT")" \
  "$(env_assignment ACCOUNT_PORT "$ACCOUNT_PORT")" \
  "$(env_assignment REPO_PORT "$REPO_PORT")" \
  "$(env_assignment ADMIN_PORT "$ADMIN_PORT")" \
  "$(env_assignment DS_ID "$DS_ID")" \
  "$(env_assignment API_KEY "$API_KEY")" \
  "$(env_assignment ACCOUNT_ID "$ACCOUNT_ID")" \
  "$(env_assignment DRIVE_NAME "$DRIVE_NAME")" \
  "$(env_assignment LOCAL_BUCKET "$LOCAL_BUCKET")" \
  "$(env_assignment DATASOURCE_NAME "$DATASOURCE_NAME")" \
  "$(env_assignment CONNECTOR_ID "$CONNECTOR_ID")" \
  "$(env_assignment INTAKE_E2E_SOURCE_FILE "$SOURCE_FILE")" \
  "$(env_assignment INTAKE_E2E_SOURCE_DIR "$SOURCE_DIR")" \
  "$(env_assignment INTAKE_E2E_SOURCE_PREFIX "$INTAKE_E2E_SOURCE_PREFIX")" \
  "$(env_assignment INTAKE_E2E_CONTENT_TYPE "$INTAKE_E2E_CONTENT_TYPE")" \
  "$(env_assignment INTAKE_E2E_MAX_FILES "$INTAKE_E2E_MAX_FILES")")"

echo "Wrote local HTTP upload env: $written"
print_selected_datasource
if [[ -n "$SOURCE_FILE" ]]; then
  echo "Source file:   $SOURCE_FILE"
fi
if [[ -n "$SOURCE_DIR" ]]; then
  echo "Source dir:    $SOURCE_DIR"
fi
echo
echo "Try one document:"
echo "  scripts/e2e/http-upload/upload-directory.sh --dry-run"
