FROM apache/airflow:3.1.8

# Install additional Python packages required by DAGs in this repo.
# EDIT: Add any new packages here as new --find-links or on a new RUN line.
RUN pip install --no-cache-dir \
    praw \
    anthropic \
    openai \
    instagrapi \
    # Upgrade the entire OTel stack together so api/sdk/exporters stay version-consistent.
    # opentelemetry-instrumentation-openai-v2 needs api>=1.28.0; leaving sdk at 1.27.0
    # (Airflow's bundled version) while api jumps to 1.40.0 causes a strict version check
    # in the SDK to fail at import time, which cascades into an Airflow startup crash.
    "opentelemetry-api>=1.40.0" \
    "opentelemetry-sdk>=1.40.0" \
    "opentelemetry-proto>=1.40.0" \
    "opentelemetry-exporter-otlp>=1.40.0" \
    "opentelemetry-exporter-otlp-proto-common>=1.40.0" \
    "opentelemetry-exporter-otlp-proto-http>=1.40.0" \
    "opentelemetry-exporter-otlp-proto-grpc>=1.40.0" \
    "opentelemetry-semantic-conventions>=0.61b0" \
    opentelemetry-instrumentation-openai-v2
