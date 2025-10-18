#!/usr/bin/env bash
set -euo pipefail

: "${AUTO_CONFIG:=true}"
: "${AUTO_VALIDATE:=false}"
: "${STORAGE_TYPE:=MINIO}"
: "${SUPERTABLE_ENV_FILE:=/config/.env}"
: "${SUPERTABLE_HOME:=/data/supertable}"

mkdir -p "$(dirname "$SUPERTABLE_ENV_FILE")" "$SUPERTABLE_HOME"

if [[ "${AUTO_CONFIG}" == "true" ]]; then
  ARGS=(config --storage "${STORAGE_TYPE}" --write "${SUPERTABLE_ENV_FILE}")
  if [[ "${AUTO_VALIDATE}" != "true" ]]; then ARGS+=(--no-validate); fi

  case "${STORAGE_TYPE}" in
    LOCAL)
      : "${SUPERTABLE_HOME:=${LOCAL_HOME:-$HOME/supertable}}"
      ARGS+=(--local-home "${SUPERTABLE_HOME}")
      if [[ "${CREATE_LOCAL_HOME:-true}" == "true" ]]; then ARGS+=(--create-local-home); fi
      ;;
    MINIO)
      [[ -n "${AWS_S3_ENDPOINT_URL:-}" ]] && ARGS+=(--aws-endpoint-url "${AWS_S3_ENDPOINT_URL}")
      [[ -n "${AWS_S3_FORCE_PATH_STYLE:-}" ]] && ARGS+=(--aws-force-path-style "${AWS_S3_FORCE_PATH_STYLE}")
      [[ -n "${AWS_ACCESS_KEY_ID:-}" ]] && ARGS+=(--aws-access-key-id "${AWS_ACCESS_KEY_ID}")
      [[ -n "${AWS_SECRET_ACCESS_KEY:-}" ]] && ARGS+=(--aws-secret-access-key "${AWS_SECRET_ACCESS_KEY}")
      [[ -n "${AWS_DEFAULT_REGION:-}" ]] && ARGS+=(--aws-region "${AWS_DEFAULT_REGION}")
      ;;
    S3)
      [[ -n "${AWS_ACCESS_KEY_ID:-}" ]] && ARGS+=(--aws-access-key-id "${AWS_ACCESS_KEY_ID}")
      [[ -n "${AWS_SECRET_ACCESS_KEY:-}" ]] && ARGS+=(--aws-secret-access-key "${AWS_SECRET_ACCESS_KEY}")
      [[ -n "${AWS_DEFAULT_REGION:-}" ]] && ARGS+=(--aws-region "${AWS_DEFAULT_REGION}")
      ;;
    AZURE)
      [[ -n "${AZURE_STORAGE_CONNECTION_STRING:-}" ]] && ARGS+=(--azure-connection-string "${AZURE_STORAGE_CONNECTION_STRING}")
      [[ -n "${SUPERTABLE_HOME:-}" ]] && ARGS+=(--home "${SUPERTABLE_HOME}")
      ;;
    GCP|GCS)
      [[ -n "${GCP_PROJECT:-}" ]] && ARGS+=(--gcp-project "${GCP_PROJECT}")
      if [[ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
        ARGS+=(--gcp-credentials "${GOOGLE_APPLICATION_CREDENTIALS}")
      elif [[ -n "${GCP_SA_JSON:-}" ]]; then
        ARGS+=(--gcp-credentials "${GCP_SA_JSON}")
      fi
      ;;
  esac

  echo "[entrypoint] supertable ${ARGS[*]}"
  # best-effort: do not fail container if config tool is absent in this build
  if command -v supertable >/dev/null 2>&1; then
    supertable "${ARGS[@]}" || echo "[entrypoint] WARN: supertable config failed (continuing)"
  else
    echo "[entrypoint] INFO: 'supertable' CLI not present in this build; skipping auto-config"
  fi
fi

exec "$@"
