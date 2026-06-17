#!/bin/bash
# Deploy Airflow via Helm.
# Prerequisite: run ./setup.sh first to ensure all secrets exist.

set -euo pipefail

if ! kubectl get secret airflow-git-connection -n airflow &>/dev/null; then
  echo "ERROR: airflow-git-connection secret not found in namespace 'airflow'." >&2
  echo "       Run ./setup.sh first." >&2
  exit 1
fi

helm upgrade airflow apache-airflow/airflow -n airflow --version 1.19.0 --reuse-values -f k8s/airflow-values-otel.yaml "$@"
