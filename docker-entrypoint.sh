#!/usr/bin/env bash
set -euo pipefail

: "${AUTO_CONFIG:=true}"
: "${AUTO_VALIDATE:=false}"
: "${STORAGE_TYPE:=LOCAL}"
: "${SUPERTABLE_ENV_FILE:=/config/.env}"

if [[ "${AUTO_CONFIG}" == "true" ]]; then
  ARGS=(config --storage "${STORAGE_TYPE}" --write "${SUPERTABLE_ENV_FILE}")
  if [[ "${AUTO_VALIDATE}" != "true" ]]; then ARGS+=(--no-validate); fi

  if [[ "${STORAGE_TYPE}" == "LOCAL" ]]; then
    : "${SUPERTABLE_HOME:=${LOCAL_HOME:-$HOME/supertable}}"
    ARGS+=(--local-home "${SUPERTABLE_HOME}")
    if [[ "${CREATE_LOCAL_HOME:-true}" == "true" ]]; then ARGS+=(--create-local-home); fi
    if [[ "${REDIS_WITH_LOCAL:-false}" == "true" ]]; then ARGS+=(--redis-with-local); fi
  fi

  # Redis flags
  if [[ -n "${REDIS_URL:-}" ]]; then
    ARGS+=(--redis-url "${REDIS_URL}")
  else
    [[ -n "${REDIS_HOST:-}" ]] && ARGS+=(--redis-host "${REDIS_HOST}")
    [[ -n "${REDIS_PORT:-}" ]] && ARGS+=(--redis-port "${REDIS_PORT}")
    [[ -n "${REDIS_DB:-}" ]] && ARGS+=(--redis-db "${REDIS_DB}")
    [[ -n "${REDIS_PASSWORD:-}" ]] && ARGS+=(--redis-password "${REDIS_PASSWORD}")
    if [[ -n "${REDIS_SSL:-}" ]]; then
      case "${REDIS_SSL,,}" in
        1|true|yes|on) ARGS+=(--redis-ssl) ;;
      esac
    fi
  fi

  # S3 / MinIO
  if [[ "${STORAGE_TYPE}" == "S3" || "${STORAGE_TYPE}" == "MINIO" ]]; then
    [[ -n "${AWS_ACCESS_KEY_ID:-}" ]] && ARGS+=(--aws-access-key-id "${AWS_ACCESS_KEY_ID}")
    [[ -n "${AWS_SECRET_ACCESS_KEY:-}" ]] && ARGS+=(--aws-secret-access-key "${AWS_SECRET_ACCESS_KEY}")
    [[ -n "${AWS_DEFAULT_REGION:-}" ]] && ARGS+=(--aws-region "${AWS_DEFAULT_REGION}")
    [[ -n "${AWS_S3_ENDPOINT_URL:-}" ]] && ARGS+=(--aws-endpoint-url "${AWS_S3_ENDPOINT_URL}")
    if [[ -n "${AWS_S3_FORCE_PATH_STYLE:-}" ]]; then
      ARGS+=(--aws-force-path-style "${AWS_S3_FORCE_PATH_STYLE}")
    fi
  fi

  # Azure
  if [[ "${STORAGE_TYPE}" == "AZURE" ]]; then
    [[ -n "${AZURE_STORAGE_CONNECTION_STRING:-}" ]] && ARGS+=(--azure-connection-string "${AZURE_STORAGE_CONNECTION_STRING}")
    [[ -n "${AZURE_STORAGE_ACCOUNT:-}" ]] && ARGS+=(--azure-account "${AZURE_STORAGE_ACCOUNT}")
    [[ -n "${AZURE_STORAGE_KEY:-}" ]] && ARGS+=(--azure-key "${AZURE_STORAGE_KEY}")
    [[ -n "${AZURE_SAS_TOKEN:-}" ]] && ARGS+=(--azure-sas "${AZURE_SAS_TOKEN}")
    [[ -n "${AZURE_BLOB_ENDPOINT:-}" ]] && ARGS+=(--azure-endpoint "${AZURE_BLOB_ENDPOINT}")
  fi

  # GCP
  if [[ "${STORAGE_TYPE}" == "GCP" ]]; then
    [[ -n "${GCP_PROJECT:-}" ]] && ARGS+=(--gcp-project "${GCP_PROJECT}")
    if [[ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
      ARGS+=(--gcp-credentials "${GOOGLE_APPLICATION_CREDENTIALS}")
    elif [[ -n "${GCP_SA_JSON:-}" ]]; then
      ARGS+=(--gcp-credentials "${GCP_SA_JSON}")
    fi
  fi

  echo "[entrypoint] supertable ${ARGS[*]}"
  supertable "${ARGS[@]}"
fi

exec "$@"
