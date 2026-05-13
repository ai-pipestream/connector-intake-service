#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

DRY_RUN=false
FORCE_CREATE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --force-create) FORCE_CREATE=true; shift ;;
    --help|-h)
      cat <<'USAGE'
Usage: prepare-datasource.sh [--dry-run] [--force-create]

Loads ENV_FILE, defaulting to scripts/e2e/http-upload/.env.local.
If DS_ID and API_KEY are already present, the script reuses them.
Otherwise it creates a sample account, bucket, and datasource using grpcurl.
USAGE
      exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

load_env

SERVICE_HOST="${SERVICE_HOST:-localhost}"
ACCOUNT_PORT="${ACCOUNT_PORT:-18105}"
REPO_PORT="${REPO_PORT:-18102}"
ADMIN_PORT="${ADMIN_PORT:-18107}"
ACCOUNT_ID="${ACCOUNT_ID:-sample-account}"
DRIVE_NAME="${DRIVE_NAME:-sample-drive}"
LOCAL_BUCKET="${LOCAL_BUCKET:-pipestream}"
DATASOURCE_NAME="${DATASOURCE_NAME:-sample-http-upload}"
CONNECTOR_ID="${CONNECTOR_ID:-a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}"

if [[ "$FORCE_CREATE" != "true" && -n "${DS_ID:-}" && -n "${API_KEY:-}" ]]; then
  info "Reusing datasource credentials from env"
  print_selected_datasource
  exit 0
fi

require_cmd grpcurl
require_cmd jq

if [[ "$DRY_RUN" == "true" ]]; then
  cat <<EOF
Would create or reuse:
  account:      $ACCOUNT_ID
  drive:        $DRIVE_NAME
  bucket:       $LOCAL_BUCKET
  datasource:   $DATASOURCE_NAME
  connector id: $CONNECTOR_ID
  services:     $SERVICE_HOST account:$ACCOUNT_PORT repo:$REPO_PORT admin:$ADMIN_PORT
EOF
  exit 0
fi

info "Creating account '$ACCOUNT_ID' if needed"
grpcurl -plaintext -d "{
  \"account_id\": \"$ACCOUNT_ID\",
  \"name\": \"$ACCOUNT_ID\",
  \"description\": \"Created by connector-intake HTTP upload E2E\",
  \"create_default_drive\": true,
  \"default_drive_config\": {
    \"drive_name\": \"$DRIVE_NAME\",
    \"bucket_name\": \"$LOCAL_BUCKET\",
    \"create_bucket\": true
  }
}" "$SERVICE_HOST:$ACCOUNT_PORT" ai.pipestream.repository.account.v1.AccountService/CreateAccount >/dev/null || true

info "Ensuring repository bucket '$LOCAL_BUCKET' exists"
grpcurl -plaintext -d "{\"bucket_name\":\"$LOCAL_BUCKET\"}" \
  "$SERVICE_HOST:$REPO_PORT" ai.pipestream.repository.filesystem.v1.FilesystemService/CreateBucket >/dev/null || true

info "Creating datasource '$DATASOURCE_NAME'"
response="$(grpcurl -plaintext -d "{
  \"account_id\": \"$ACCOUNT_ID\",
  \"connector_id\": \"$CONNECTOR_ID\",
  \"name\": \"$DATASOURCE_NAME\",
  \"drive_name\": \"$DRIVE_NAME\"
}" "$SERVICE_HOST:$ADMIN_PORT" ai.pipestream.connector.intake.v1.DataSourceAdminService/CreateDataSource 2>&1)" || {
  if [[ "$response" != *"AlreadyExists"* ]]; then
    echo "$response" >&2
    exit 1
  fi

  info "Datasource already exists; rotating a fresh local API key"
  list_response="$(grpcurl -plaintext -d "{\"account_id\":\"$ACCOUNT_ID\"}" \
    "$SERVICE_HOST:$ADMIN_PORT" ai.pipestream.connector.intake.v1.DataSourceAdminService/ListDataSources)"
  DS_ID="$(jq -r --arg name "$DATASOURCE_NAME" --arg connector "$CONNECTOR_ID" \
    '.datasources[] | select(.name == $name and .connectorId == $connector) | .datasourceId' <<<"$list_response" | head -n 1)"
  [[ -n "$DS_ID" ]] || die "existing datasource '$DATASOURCE_NAME' was not found in ListDataSources"

  rotate_response="$(grpcurl -plaintext -d "{
    \"datasource_id\": \"$DS_ID\",
    \"invalidate_old_immediately\": false
  }" "$SERVICE_HOST:$ADMIN_PORT" ai.pipestream.connector.intake.v1.DataSourceAdminService/RotateApiKey)"
  API_KEY="$(json_get "$rotate_response" '.newApiKey')"
  [[ -n "$API_KEY" ]] || die "RotateApiKey response did not include newApiKey"
  response=""
}

if [[ -n "$response" ]]; then
  DS_ID="$(json_get "$response" '.datasourceId // .datasource.datasourceId')"
  API_KEY="$(json_get "$response" '.apiKey // .newApiKey')"
fi

[[ -n "$DS_ID" ]] || die "CreateDataSource response did not include datasourceId"
[[ -n "$API_KEY" ]] || die "CreateDataSource response did not include apiKey"

env_file="$(write_private_env \
  "$(env_assignment SERVICE_HOST "$SERVICE_HOST")" \
  "$(env_assignment INTAKE_HTTP_PORT "${INTAKE_HTTP_PORT:-18108}")" \
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
  "$(env_assignment INTAKE_E2E_SOURCE_FILE "${INTAKE_E2E_SOURCE_FILE:-}")" \
  "$(env_assignment INTAKE_E2E_SOURCE_DIR "${INTAKE_E2E_SOURCE_DIR:-}")" \
  "$(env_assignment INTAKE_E2E_SOURCE_PREFIX "${INTAKE_E2E_SOURCE_PREFIX:-e2e/http-upload}")" \
  "$(env_assignment INTAKE_E2E_CONTENT_TYPE "${INTAKE_E2E_CONTENT_TYPE:-application/octet-stream}")" \
  "$(env_assignment INTAKE_E2E_MAX_FILES "${INTAKE_E2E_MAX_FILES:-0}")")"

print_selected_datasource
echo "Wrote credentials to: $env_file"
