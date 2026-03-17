#!/bin/bash
set -e

# =============================================================================
# S3 Crawl Setup Script
# Sets up an account, datasource, and triggers an S3 crawl.
# Configurable for any S3-compatible endpoint (AWS, MinIO, SeaweedFS, etc.)
#
# Usage:
#   ./setup-s3-crawl.sh                          # Interactive prompts
#   ./setup-s3-crawl.sh --config my-config.env   # Load from env file
#
# Config file format (source as env vars):
#   S3_ENDPOINT=https://s3.amazonaws.com
#   S3_ACCESS_KEY=AKIA...
#   S3_SECRET_KEY=...
#   S3_BUCKET=my-bucket
#   S3_PREFIX=documents/
#   S3_REGION=us-east-1
#   S3_PATH_STYLE=false
# =============================================================================

# Service ports (override with env vars if needed)
ACCOUNT_PORT=${ACCOUNT_PORT:-18105}
REPO_PORT=${REPO_PORT:-18102}
ADMIN_PORT=${ADMIN_PORT:-18107}
CONNECTOR_PORT=${CONNECTOR_PORT:-18120}
SERVICE_HOST=${SERVICE_HOST:-localhost}

# Pre-seeded S3 connector type ID
S3_CONNECTOR_ID="a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"

# Defaults
ACCOUNT_ID=${ACCOUNT_ID:-"default"}
DRIVE_NAME=${DRIVE_NAME:-"default-drive"}
LOCAL_BUCKET=${LOCAL_BUCKET:-"pipestream"}
DATASOURCE_NAME=${DATASOURCE_NAME:-"s3-crawl"}

# S3 source config (must be set via env or config file)
S3_ENDPOINT=${S3_ENDPOINT:-""}
S3_ACCESS_KEY=${S3_ACCESS_KEY:-""}
S3_SECRET_KEY=${S3_SECRET_KEY:-""}
S3_BUCKET=${S3_BUCKET:-""}
S3_PREFIX=${S3_PREFIX:-""}
S3_REGION=${S3_REGION:-"us-east-1"}
S3_PATH_STYLE=${S3_PATH_STYLE:-true}
S3_CREDENTIALS_TYPE=${S3_CREDENTIALS_TYPE:-"static"}

# =============================================================================
# Parse args
# =============================================================================
while [[ $# -gt 0 ]]; do
  case $1 in
    --config)
      if [ -f "$2" ]; then
        echo "Loading config from $2"
        set -a; source "$2"; set +a
      else
        echo "ERROR: Config file not found: $2" && exit 1
      fi
      shift 2 ;;
    --account)    ACCOUNT_ID="$2"; shift 2 ;;
    --drive)      DRIVE_NAME="$2"; shift 2 ;;
    --datasource) DATASOURCE_NAME="$2"; shift 2 ;;
    --bucket)     S3_BUCKET="$2"; shift 2 ;;
    --prefix)     S3_PREFIX="$2"; shift 2 ;;
    --endpoint)   S3_ENDPOINT="$2"; shift 2 ;;
    --dry-run)    DRY_RUN=true; shift ;;
    --help|-h)
      echo "Usage: $0 [options]"
      echo ""
      echo "Options:"
      echo "  --config FILE        Load S3 config from env file"
      echo "  --account ID         Account ID (default: 'default')"
      echo "  --drive NAME         Drive name (default: 'default-drive')"
      echo "  --datasource NAME    DataSource name (default: 's3-crawl')"
      echo "  --endpoint URL       S3 endpoint URL"
      echo "  --bucket NAME        S3 source bucket"
      echo "  --prefix PATH        S3 key prefix to crawl"
      echo "  --dry-run            Set up account/datasource but don't start crawl"
      echo ""
      echo "Environment variables: S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY,"
      echo "  S3_BUCKET, S3_PREFIX, S3_REGION, S3_PATH_STYLE, S3_CREDENTIALS_TYPE"
      exit 0 ;;
    *) echo "Unknown option: $1" && exit 1 ;;
  esac
done

# Validate required fields
if [ -z "$S3_ENDPOINT" ] || [ -z "$S3_BUCKET" ]; then
  echo "ERROR: S3_ENDPOINT and S3_BUCKET are required."
  echo "Set via environment variables, --config file, or --endpoint / --bucket flags."
  exit 1
fi

if [ "$S3_CREDENTIALS_TYPE" = "static" ] && { [ -z "$S3_ACCESS_KEY" ] || [ -z "$S3_SECRET_KEY" ]; }; then
  echo "ERROR: S3_ACCESS_KEY and S3_SECRET_KEY required for static credentials."
  exit 1
fi

# Check dependencies
command -v grpcurl >/dev/null 2>&1 || { echo "ERROR: grpcurl is required but not installed."; exit 1; }

echo "=== Configuration ==="
echo "  Services:    $SERVICE_HOST (account:$ACCOUNT_PORT repo:$REPO_PORT admin:$ADMIN_PORT connector:$CONNECTOR_PORT)"
echo "  Account:     $ACCOUNT_ID"
echo "  Drive:       $DRIVE_NAME"
echo "  DataSource:  $DATASOURCE_NAME"
echo "  S3 Endpoint: $S3_ENDPOINT"
echo "  S3 Bucket:   $S3_BUCKET"
echo "  S3 Prefix:   ${S3_PREFIX:-(root)}"
echo ""

# =============================================================================
# Step 1: Create Account
# =============================================================================
echo "=== 1. Creating Account '$ACCOUNT_ID' ==="
grpcurl -plaintext -d "{
  \"account_id\": \"$ACCOUNT_ID\",
  \"name\": \"$ACCOUNT_ID\",
  \"description\": \"Auto-created by setup-s3-crawl script\",
  \"create_default_drive\": true,
  \"default_drive_config\": {
    \"drive_name\": \"$DRIVE_NAME\",
    \"bucket_name\": \"$LOCAL_BUCKET\",
    \"create_bucket\": true
  }
}" "$SERVICE_HOST:$ACCOUNT_PORT" ai.pipestream.repository.account.v1.AccountService/CreateAccount

# =============================================================================
# Step 2: Ensure local storage bucket exists
# =============================================================================
echo -e "\n=== 2. Ensuring local S3 bucket '$LOCAL_BUCKET' exists ==="
grpcurl -plaintext -d "{
  \"bucket_name\": \"$LOCAL_BUCKET\"
}" "$SERVICE_HOST:$REPO_PORT" ai.pipestream.repository.filesystem.v1.FilesystemService/CreateBucket 2>&1 || true

# =============================================================================
# Step 3: Create DataSource
# =============================================================================
echo -e "\n=== 3. Creating DataSource '$DATASOURCE_NAME' ==="
RESPONSE=$(grpcurl -plaintext -d "{
  \"account_id\": \"$ACCOUNT_ID\",
  \"connector_id\": \"$S3_CONNECTOR_ID\",
  \"name\": \"$DATASOURCE_NAME\",
  \"drive_name\": \"$DRIVE_NAME\",
  \"metadata\": {
    \"s3_endpoint\": \"$S3_ENDPOINT\",
    \"s3_bucket\": \"$S3_BUCKET\",
    \"s3_prefix\": \"$S3_PREFIX\"
  }
}" "$SERVICE_HOST:$ADMIN_PORT" ai.pipestream.connector.intake.v1.DataSourceAdminService/CreateDataSource)

echo "$RESPONSE"

API_KEY=$(echo "$RESPONSE" | grep "apiKey" | head -n 1 | awk -F'"' '{print $4}')
DS_ID=$(echo "$RESPONSE" | grep "datasourceId" | head -n 1 | awk -F'"' '{print $4}')

if [ -z "$API_KEY" ] || [ -z "$DS_ID" ]; then
  echo "ERROR: Failed to extract API key or datasource ID from response"
  exit 1
fi

echo ""
echo "  Datasource ID: $DS_ID"
echo "  API Key:       $API_KEY"

# Save credentials for reuse
CREDS_FILE="/tmp/pipestream-crawl-$DS_ID.env"
cat > "$CREDS_FILE" <<EOF
DS_ID=$DS_ID
API_KEY=$API_KEY
S3_ENDPOINT=$S3_ENDPOINT
S3_BUCKET=$S3_BUCKET
S3_PREFIX=$S3_PREFIX
EOF
echo "  Credentials saved to: $CREDS_FILE"

# =============================================================================
# Step 4: Trigger Crawl
# =============================================================================
if [ "$DRY_RUN" = "true" ]; then
  echo -e "\n=== Dry run — skipping crawl ==="
  echo "To start crawl manually:"
  echo "  grpcurl -plaintext -H 'x-api-key: $API_KEY' -H 'x-datasource-id: $DS_ID' \\"
  echo "    -d '{\"datasource_id\": \"$DS_ID\", \"bucket\": \"$S3_BUCKET\", \"prefix\": \"$S3_PREFIX\", \"connection_config\": {\"endpoint_override\": \"$S3_ENDPOINT\", \"region\": \"$S3_REGION\", \"credentials_type\": \"$S3_CREDENTIALS_TYPE\", \"access_key_id\": \"$S3_ACCESS_KEY\", \"secret_access_key\": \"$S3_SECRET_KEY\", \"path_style_access\": $S3_PATH_STYLE}}' \\"
  echo "    $SERVICE_HOST:$CONNECTOR_PORT ai.pipestream.connector.s3.v1.S3ConnectorControlService/StartCrawl"
  exit 0
fi

echo -e "\n=== 4. Triggering S3 Crawl ==="
echo "Crawling $S3_ENDPOINT / $S3_BUCKET / ${S3_PREFIX:-(root)} ..."

grpcurl -plaintext \
  -H "x-api-key: $API_KEY" \
  -H "x-datasource-id: $DS_ID" \
  -d "{
    \"datasource_id\": \"$DS_ID\",
    \"bucket\": \"$S3_BUCKET\",
    \"prefix\": \"$S3_PREFIX\",
    \"connection_config\": {
      \"endpoint_override\": \"$S3_ENDPOINT\",
      \"region\": \"$S3_REGION\",
      \"credentials_type\": \"$S3_CREDENTIALS_TYPE\",
      \"access_key_id\": \"$S3_ACCESS_KEY\",
      \"secret_access_key\": \"$S3_SECRET_KEY\",
      \"path_style_access\": $S3_PATH_STYLE
    }
  }" "$SERVICE_HOST:$CONNECTOR_PORT" ai.pipestream.connector.s3.v1.S3ConnectorControlService/StartCrawl

echo -e "\n=== Done ==="
echo "Monitor crawl progress in the s3-connector logs."
