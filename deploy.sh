helm upgrade airflow apache-airflow/airflow -n airflow --version 1.19.0 --reuse-values -f k8s/airflow-values-otel.yaml
