#!/bin/bash
# Creates the Kubernetes secret for the social-archive app.
#
# Required environment variables:
#   DATABASE_URL    - PostgreSQL connection string (postgresql+psycopg://user:pass@host:5432/db)
#   JWT_SECRET      - Long random string used to sign JWTs
#   ADMIN_USERNAME  - Initial admin username (default: admin)
#   ADMIN_PASSWORD  - Initial admin password
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

kubectl delete secret social-archive-secrets -n default --ignore-not-found
kubectl create secret generic social-archive-secrets -n default \
  --from-literal=DATABASE_URL="${DATABASE_URL}" \
  --from-literal=JWT_SECRET="${JWT_SECRET}" \
  --from-literal=ADMIN_USERNAME="${ADMIN_USERNAME}" \
  --from-literal=ADMIN_PASSWORD="${ADMIN_PASSWORD}"
kubectl rollout restart deployment/social-archive-backend -n default
