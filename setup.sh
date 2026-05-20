#!/bin/bash
# Creates Kubernetes secrets for:
#   - social-archive app (default namespace)
#   - Airflow git DAG bundle connection (airflow namespace)
#
# Required environment variables:
#   DATABASE_URL              - PostgreSQL connection string (postgresql+psycopg://user:pass@host:5432/db)
#   JWT_SECRET                - Long random string used to sign JWTs
#   ADMIN_USERNAME            - Initial admin username (default: admin)
#   ADMIN_PASSWORD            - Initial admin password
#
# Optional environment variables:
#   AIRFLOW_GIT_SSH_KEY_FILE  - Path to SSH private key for Airflow git DAG bundle
#                               (default: ~/.ssh/airflow)
#   GARAGE_S3_ENDPOINT        - Garage S3 endpoint for bookmark_processor
#                               (default: https://s3.wallace.network)
#   GARAGE_S3_BUCKET          - Garage S3 bucket for bookmark_processor
#                               (default: social-archive)
#
# Example:
#   export DATABASE_URL='postgresql+psycopg://app:password@host:5432/social_archive'
#   export JWT_SECRET='your-long-random-secret'
#   export ADMIN_USERNAME='admin'
#   export ADMIN_PASSWORD='your-password'
#   ./setup.sh

set -euo pipefail

: "${DATABASE_URL:?DATABASE_URL must be set}"
: "${JWT_SECRET:?JWT_SECRET must be set}"
: "${ADMIN_USERNAME:=admin}"
: "${ADMIN_PASSWORD:?ADMIN_PASSWORD must be set}"
AIRFLOW_GIT_SSH_KEY_FILE="${AIRFLOW_GIT_SSH_KEY_FILE:-$HOME/.ssh/airflow}"

# ── social-archive ────────────────────────────────────────────────────────────
kubectl delete secret social-archive-secrets -n default --ignore-not-found
kubectl create secret generic social-archive-secrets -n default \
  --from-literal=DATABASE_URL="${DATABASE_URL}" \
  --from-literal=JWT_SECRET="${JWT_SECRET}" \
  --from-literal=ADMIN_USERNAME="${ADMIN_USERNAME}" \
  --from-literal=ADMIN_PASSWORD="${ADMIN_PASSWORD}"
kubectl rollout restart deployment/social-archive-backend -n default

# ── Airflow git DAG bundle connection ─────────────────────────────────────────
# Stored as AIRFLOW_CONN_PRIVATE_DAGS so Airflow picks it up from the environment
# without needing a metadata-DB entry — survives DB wipes.
if [[ ! -f "${AIRFLOW_GIT_SSH_KEY_FILE}" ]]; then
  echo "ERROR: Airflow git SSH key not found at ${AIRFLOW_GIT_SSH_KEY_FILE}" >&2
  echo "       Set AIRFLOW_GIT_SSH_KEY_FILE to the correct path and re-run." >&2
  exit 1
fi

# The connection uses key_file (path to a volume-mounted secret) rather than
# an inline private_key, which avoids OpenSSL parsing issues with temp files.
CONN_JSON=$(AIRFLOW_GIT_SSH_KEY_FILE="${AIRFLOW_GIT_SSH_KEY_FILE}" python3 -c "
import json, os
conn = {
    'conn_type': 'git',
    'host': 'git@github.com:proffalken/private-airflow-dags.git',
    'extra': json.dumps({
        'key_file': '/opt/airflow/secrets/git-ssh/id_ed25519',
        'strict_host_key_checking': 'no'
    })
}
print(json.dumps(conn))
")

# ── Airflow social_archive DB connection ──────────────────────────────────────
# Derive from DATABASE_URL, converting the psycopg driver prefix to plain postgresql://
AIRFLOW_SOCIAL_ARCHIVE_CONN=$(echo "${DATABASE_URL}" | sed 's|postgresql+psycopg://|postgresql://|')
kubectl delete secret airflow-social-archive-db -n airflow --ignore-not-found
kubectl create secret generic airflow-social-archive-db -n airflow \
  --from-literal=AIRFLOW_CONN_SOCIAL_ARCHIVE_DB="${AIRFLOW_SOCIAL_ARCHIVE_CONN}"

kubectl delete secret airflow-git-connection -n airflow --ignore-not-found
kubectl create secret generic airflow-git-connection -n airflow \
  --from-literal=AIRFLOW_CONN_PRIVATE_DAGS="${CONN_JSON}"

kubectl delete secret airflow-git-ssh-key -n airflow --ignore-not-found
kubectl create secret generic airflow-git-ssh-key -n airflow \
  --from-file=id_ed25519="${AIRFLOW_GIT_SSH_KEY_FILE}"

# ── Airflow Variables ─────────────────────────────────────────────────────────
# Stored as AIRFLOW_VAR_* env vars so they survive metadata-DB wipes.
: "${GITHUB_TOKEN:?GITHUB_TOKEN must be set}"
: "${REDDIT_CLIENT_ID:?REDDIT_CLIENT_ID must be set}"
: "${REDDIT_CLIENT_SECRET:?REDDIT_CLIENT_SECRET must be set}"
: "${REDDIT_USER:?REDDIT_USER must be set}"
: "${REDDIT_PASSWORD:?REDDIT_PASSWORD must be set}"
: "${YOUTUBE_CLIENT_ID:?YOUTUBE_CLIENT_ID must be set}"
: "${YOUTUBE_CLIENT_SECRET:?YOUTUBE_CLIENT_SECRET must be set}"
: "${YOUTUBE_REFRESH_TOKEN:?YOUTUBE_REFRESH_TOKEN must be set}"
: "${INSTAGRAM_USERNAME:?INSTAGRAM_USERNAME must be set}"
: "${INSTAGRAM_PASSWORD:?INSTAGRAM_PASSWORD must be set}"
# Optional — bookmark_processor uses these if set
GARAGE_S3_ENDPOINT="${GARAGE_S3_ENDPOINT:-https://s3.wallace.network}"
GARAGE_S3_BUCKET="${GARAGE_S3_BUCKET:-social-archive}"

kubectl delete secret airflow-variables -n airflow --ignore-not-found
kubectl create secret generic airflow-variables -n airflow \
  --from-literal=AIRFLOW_VAR_GITHUB_TOKEN="${GITHUB_TOKEN}" \
  --from-literal=AIRFLOW_VAR_REDDIT_CLIENT_ID="${REDDIT_CLIENT_ID}" \
  --from-literal=AIRFLOW_VAR_REDDIT_CLIENT_SECRET="${REDDIT_CLIENT_SECRET}" \
  --from-literal=AIRFLOW_VAR_REDDIT_USER="${REDDIT_USER}" \
  --from-literal=AIRFLOW_VAR_REDDIT_PASSWORD="${REDDIT_PASSWORD}" \
  --from-literal=AIRFLOW_VAR_YOUTUBE_CLIENT_ID="${YOUTUBE_CLIENT_ID}" \
  --from-literal=AIRFLOW_VAR_YOUTUBE_CLIENT_SECRET="${YOUTUBE_CLIENT_SECRET}" \
  --from-literal=AIRFLOW_VAR_YOUTUBE_REFRESH_TOKEN="${YOUTUBE_REFRESH_TOKEN}" \
  --from-literal=AIRFLOW_VAR_INSTAGRAM_USERNAME="${INSTAGRAM_USERNAME}" \
  --from-literal=AIRFLOW_VAR_INSTAGRAM_PASSWORD="${INSTAGRAM_PASSWORD}" \
  --from-literal=AIRFLOW_VAR_GARAGE_S3_ENDPOINT="${GARAGE_S3_ENDPOINT}" \
  --from-literal=AIRFLOW_VAR_GARAGE_S3_BUCKET="${GARAGE_S3_BUCKET}"

echo "Secrets created. Run ./deploy.sh to apply the Helm upgrade."
