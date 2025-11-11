#!/bin/bash

# Connector Intake Service Startup Script
# Port: 38108 (Core Service)
# This script helps start the connector intake service in development mode
# with proper environment variable detection, helper bootstrapping, and instance management.
#
# Bootstrap Design: Like Gradle Wrapper
# - Automatically downloads helper scripts from GitHub if not available locally
# - Works OOTB on Windows/Mac/Linux right after checkout
# - No manual dev-assets setup required

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/.."

# ============================================================================
# Bootstrap Helper Scripts from GitHub (like gradlew)
# ============================================================================

DEV_ASSETS_REPO="https://raw.githubusercontent.com/ai-pipestream/dev-assets/main"
HELPERS_DIR="$PROJECT_ROOT/.dev-helpers"
DEV_ASSETS_LOCATION="${DEV_ASSETS_LOCATION:-$HELPERS_DIR}"

bootstrap_helpers() {
  # Check if DEV_ASSETS_LOCATION is explicitly set by user
  if [ -n "${DEV_ASSETS_LOCATION_OVERRIDE}" ] && [ -f "${DEV_ASSETS_LOCATION_OVERRIDE}/scripts/shared-utils.sh" ]; then
    DEV_ASSETS_LOCATION="${DEV_ASSETS_LOCATION_OVERRIDE}"
    echo "ℹ️  Using dev-assets from: $DEV_ASSETS_LOCATION"
    return 0
  fi

  # Check if already bootstrapped
  if [ -f "$HELPERS_DIR/scripts/shared-utils.sh" ]; then
    DEV_ASSETS_LOCATION="$HELPERS_DIR"
    echo "ℹ️  Using cached helper scripts from $HELPERS_DIR"
    return 0
  fi

  # Bootstrap from GitHub
  echo "🔄 Bootstrapping helper scripts from GitHub..."
  echo "   Downloading from: $DEV_ASSETS_REPO/scripts/shared-utils.sh"
  mkdir -p "$HELPERS_DIR/scripts"

  # Download shared-utils.sh
  if curl -fsSL "$DEV_ASSETS_REPO/scripts/shared-utils.sh" -o "$HELPERS_DIR/scripts/shared-utils.sh"; then
    chmod +x "$HELPERS_DIR/scripts/shared-utils.sh"
    echo "✓ Downloaded helper scripts to $HELPERS_DIR/scripts/"
    DEV_ASSETS_LOCATION="$HELPERS_DIR"
  else
    echo "⚠️  Could not download from dev-assets repo (network issue or repo not found)"
    echo "   Creating minimal fallback helpers..."
    create_minimal_helpers
    DEV_ASSETS_LOCATION="$HELPERS_DIR"
  fi
}

create_minimal_helpers() {
  # Create minimal shared-utils.sh with essential functions
  mkdir -p "$HELPERS_DIR/scripts"
  cat > "$HELPERS_DIR/scripts/shared-utils.sh" << 'EOF'
#!/bin/bash
# Minimal fallback helper functions

check_dependencies() {
  for cmd in "$@"; do
    command -v "$cmd" >/dev/null 2>&1 || { echo "ERROR: $cmd is required but not installed"; exit 1; }
  done
}

check_port() {
  lsof -i ":$1" >/dev/null 2>&1
}

kill_process_on_port() {
  local port=$1
  local pid
  pid=$(lsof -t -i ":$port" 2>/dev/null)
  [ -n "$pid" ] && kill "$pid" 2>/dev/null
}

validate_project_structure() {
  for file in "$@"; do
    [ ! -f "$file" ] && { echo "ERROR: Expected file not found: $file"; exit 1; }
  done
}

print_status() {
  local level=$1
  shift
  case "$level" in
    header) echo "==========================================" ;;
    info) echo "ℹ️  $*" ;;
    warning) echo "⚠️  $*" ;;
    error) echo "❌ $*" ;;
  esac
}

set_registration_host() {
  local service_name=$1
  local env_var=$2
  export $env_var="${!env_var:-localhost}"
}
EOF
  chmod +x "$HELPERS_DIR/scripts/shared-utils.sh"
}

# Bootstrap the helpers
bootstrap_helpers

# Source shared utilities
if [ -f "$DEV_ASSETS_LOCATION/scripts/shared-utils.sh" ]; then
  # shellcheck disable=SC1090
  source "$DEV_ASSETS_LOCATION/scripts/shared-utils.sh"
else
  echo "ERROR: Could not find shared-utils.sh at $DEV_ASSETS_LOCATION/scripts/shared-utils.sh"
  echo "Bootstrap may have failed. Check network connection."
  exit 1
fi

# Verify functions are available
if ! type check_dependencies >/dev/null 2>&1; then
  echo "ERROR: Helper functions not loaded properly"
  echo "DEV_ASSETS_LOCATION: $DEV_ASSETS_LOCATION"
  exit 1
fi

# Service configuration
SERVICE_NAME="Connector Intake Service"
SERVICE_PORT="38108"
DESCRIPTION="Connector document ingestion, authentication, metadata enrichment, and rate limiting"

# Check dependencies
check_dependencies "docker" "java"

# Validate we're in the correct directory
validate_project_structure "build.gradle" "src/main/resources/application.properties"

# Set environment variables
export QUARKUS_HTTP_PORT="$SERVICE_PORT"

# Set registration host using Docker bridge detection
set_registration_host "connector-intake" "CONNECTOR_INTAKE_SERVICE_HOST"

# Set Consul configuration (can be overridden)
export PIPELINE_CONSUL_HOST="${PIPELINE_CONSUL_HOST:-localhost}"
export PIPELINE_CONSUL_PORT="${PIPELINE_CONSUL_PORT:-8500}"

print_status "header" "Starting $SERVICE_NAME"
print_status "info" "Port: $SERVICE_PORT"
print_status "info" "Description: $DESCRIPTION"
print_status "info" "Dev Assets Location: $DEV_ASSETS_LOCATION"
print_status "info" "Configuration:"
echo "  Service Host: $CONNECTOR_INTAKE_SERVICE_HOST"
echo "  HTTP/gRPC Port: $QUARKUS_HTTP_PORT"
echo "  Consul Host: $PIPELINE_CONSUL_HOST"
echo "  Consul Port: $PIPELINE_CONSUL_PORT"
echo

# Check if already running and offer to kill
if check_port "$SERVICE_PORT" "$SERVICE_NAME"; then
  print_status "warning" "$SERVICE_NAME is already running on port $SERVICE_PORT."
  read -p "Would you like to kill the existing process and restart? (y/N) " -r response
  if [[ "$response" =~ ^[Yy]$ ]]; then
    kill_process_on_port "$SERVICE_PORT" "$SERVICE_NAME"
  else
    print_status "info" "Cancelled by user."
    exit 0
  fi
fi

print_status "info" "Starting $SERVICE_NAME in Quarkus dev mode..."
print_status "info" "DevServices will automatically start shared infrastructure (MySQL, Kafka, Consul, etc.)"
print_status "info" "Press Ctrl+C to stop"
echo

# Start using the app's own gradlew
./gradlew quarkusDev -Dservice.registration.host="$CONNECTOR_INTAKE_SERVICE_HOST"