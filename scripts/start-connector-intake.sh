#!/bin/bash

# Connector Intake Service Startup Script
# Port: 38108 (Core Service)
# This script helps start the connector intake service in development mode
# with proper environment variable detection and instance management.

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
    return 0
  fi

  # Bootstrap from GitHub
  echo "🔄 Bootstrapping helper scripts from GitHub..."
  mkdir -p "$HELPERS_DIR/scripts"
  
  if ! curl -fsSL "$DEV_ASSETS_REPO/scripts/shared-utils.sh" -o "$HELPERS_DIR/scripts/shared-utils.sh"; then
    echo "❌ ERROR: Could not download helper scripts from GitHub"
    echo "   Please check your network connection and try again"
    exit 1
  fi
  
  chmod +x "$HELPERS_DIR/scripts/shared-utils.sh"
  DEV_ASSETS_LOCATION="$HELPERS_DIR"
  echo "✓ Helper scripts downloaded to $HELPERS_DIR"
}

# Bootstrap the helpers
bootstrap_helpers

# Source shared utilities
if [ -f "$DEV_ASSETS_LOCATION/scripts/shared-utils.sh" ]; then
  source "$DEV_ASSETS_LOCATION/scripts/shared-utils.sh"
else
  echo "❌ ERROR: Could not find shared-utils.sh at $DEV_ASSETS_LOCATION/scripts/shared-utils.sh"
  exit 1
fi

# Verify functions are available
if ! type check_dependencies >/dev/null 2>&1; then
  echo "❌ ERROR: Helper functions not loaded properly"
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

# Ensure registration uses the detected host for both advertised and internal checks
export SERVICE_REGISTRATION_ADVERTISED_HOST="$CONNECTOR_INTAKE_SERVICE_HOST"
export SERVICE_REGISTRATION_INTERNAL_HOST="$CONNECTOR_INTAKE_SERVICE_HOST"

# Set Consul configuration (can be overridden)
export PIPELINE_CONSUL_HOST="${PIPELINE_CONSUL_HOST:-localhost}"
export PIPELINE_CONSUL_PORT="${PIPELINE_CONSUL_PORT:-8500}"

print_status "header" "Starting $SERVICE_NAME"
print_status "info" "Port: $SERVICE_PORT"
print_status "info" "Description: $DESCRIPTION"
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
print_status "info" "DevServices will automatically start: MySQL, Kafka, Consul, etc."
print_status "info" "Press Ctrl+C to stop"
echo

# Start using the app's own gradlew
./gradlew quarkusDev
