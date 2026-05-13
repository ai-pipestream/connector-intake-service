#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_ENV_FILE="$SCRIPT_DIR/.env.local"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

info() {
  echo "INFO: $*" >&2
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required but was not found on PATH"
}

mask_secret() {
  local value="${1:-}"
  if [[ -z "$value" ]]; then
    echo ""
  elif [[ ${#value} -le 8 ]]; then
    echo "********"
  else
    echo "${value:0:4}********${value: -4}"
  fi
}

load_env() {
  local env_file="${ENV_FILE:-$DEFAULT_ENV_FILE}"
  if [[ -f "$env_file" ]]; then
    [[ ! -L "$env_file" ]] || die "refusing to source symlinked env file: $env_file"
    # shellcheck disable=SC1090
    set -a; source "$env_file"; set +a
  else
    info "No env file found at $env_file; using current shell environment"
  fi
}

write_private_env() {
  local env_file="${ENV_FILE:-$DEFAULT_ENV_FILE}"
  umask 077
  : > "$env_file"
  chmod 600 "$env_file"
  for line in "$@"; do
    printf '%s\n' "$line" >> "$env_file"
  done
  echo "$env_file"
}

env_assignment() {
  local key="$1"
  local value="${2:-}"
  printf '%s=%q' "$key" "$value"
}

json_get() {
  local json="$1"
  local path="$2"
  jq -r "$path // empty" <<<"$json"
}

url_base() {
  local host="${SERVICE_HOST:-localhost}"
  local port="${INTAKE_HTTP_PORT:-18108}"
  echo "http://$host:$port"
}

source_path_for_file() {
  local file="$1"
  local prefix="${INTAKE_E2E_SOURCE_PREFIX:-e2e/http-upload}"
  local rel
  if [[ -n "${INTAKE_E2E_SOURCE_DIR:-}" ]]; then
    local real_dir
    local real_file
    real_dir="$(realpath "$INTAKE_E2E_SOURCE_DIR" 2>/dev/null || true)"
    real_file="$(realpath "$file" 2>/dev/null || true)"
    if [[ -n "$real_dir" && -n "$real_file" && "$real_file" == "$real_dir"/* ]]; then
      rel="$(realpath --relative-to="$real_dir" "$real_file")"
    else
      rel="$(basename "$file")"
    fi
  else
    rel="$(basename "$file")"
  fi
  rel="${rel#./}"
  echo "$prefix/$rel"
}

require_datasource_credentials() {
  [[ -n "${DS_ID:-}" ]] || die "DS_ID is required. Set it in the env file or run prepare-datasource.sh."
  [[ -n "${API_KEY:-}" ]] || die "API_KEY is required. Set it in the env file or run prepare-datasource.sh."
}

print_selected_datasource() {
  echo "Datasource ID: ${DS_ID:-}"
  echo "API Key:       $(mask_secret "${API_KEY:-}")"
}
