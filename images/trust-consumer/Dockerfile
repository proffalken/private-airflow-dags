FROM python:3.12-slim

# Install uv, then use it to install dependencies — never use pip --break-system-packages
RUN pip install uv

WORKDIR /app
COPY consumer.py .

RUN uv pip install --system --no-cache \
    "stomp.py>=8.1" \
    "pyarrow>=18.0" \
    "boto3>=1.34" \
    "opentelemetry-sdk>=1.28" \
    "opentelemetry-exporter-otlp>=1.28" \
    "opentelemetry-exporter-otlp-proto-grpc>=1.28"

# Write a heartbeat file every 30s so k8s liveness probes can check it
HEALTHCHECK --interval=60s --timeout=5s --start-period=30s \
    CMD python -c "import os, time; f='/tmp/alive'; assert os.path.exists(f) and time.time()-os.path.getmtime(f)<120"

CMD ["python", "consumer.py"]
