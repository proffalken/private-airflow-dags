#!/usr/bin/env bash
# Garage single-node setup for Raspberry Pi (ARM64, Raspberry Pi OS 64-bit)
# Run as root. Installs Garage, configures a single-node cluster, creates the
# airflow-staging bucket and an access key, then prints the values needed for
# k8s/garage-s3-secret.yaml.
#
# Usage:
#   sudo bash setup/garage-setup.sh
#
# To pin a specific Garage version:
#   sudo GARAGE_VERSION=v1.0.1 bash setup/garage-setup.sh

set -euo pipefail

BUCKET_NAME="airflow-staging"
KEY_NAME="airflow"
S3_PORT=3900
ADMIN_PORT=3903
GARAGE_CONFIG="/etc/garage/garage.toml"
GARAGE_DATA_DIR="/var/lib/garage/data"
GARAGE_META_DIR="/var/lib/garage/meta"

# ---------------------------------------------------------------------------
# 1. Download Garage binary
# ---------------------------------------------------------------------------

if [[ -z "${GARAGE_VERSION:-}" ]]; then
    echo "==> Fetching latest Garage release version..."
    GARAGE_VERSION=$(curl -fsSL https://api.github.com/repos/deuxfleurs-org/garage/releases/latest \
        | grep '"tag_name"' | cut -d'"' -f4)
fi

echo "==> Installing Garage ${GARAGE_VERSION} (aarch64)"
curl -fsSL \
    "https://garagehq.deuxfleurs.fr/_releases/${GARAGE_VERSION}/aarch64-unknown-linux-musl/garage" \
    -o /usr/local/bin/garage
chmod +x /usr/local/bin/garage

# ---------------------------------------------------------------------------
# 2. Directories and config
# ---------------------------------------------------------------------------

mkdir -p "${GARAGE_DATA_DIR}" "${GARAGE_META_DIR}" /etc/garage

RPC_SECRET=$(openssl rand -hex 32)

cat > "${GARAGE_CONFIG}" <<EOF
metadata_dir = "${GARAGE_META_DIR}"
data_dir     = "${GARAGE_DATA_DIR}"
db_engine    = "sqlite"

replication_factor = 1
rpc_secret = "${RPC_SECRET}"

[rpc_listen_addr]
addr = "0.0.0.0:3901"

[s3_api]
s3_region    = "garage"
api_bind_addr = "0.0.0.0:${S3_PORT}"
root_domain  = ".s3.garage.local"

[admin]
api_bind_addr = "0.0.0.0:${ADMIN_PORT}"
EOF

# ---------------------------------------------------------------------------
# 3. Systemd service
# ---------------------------------------------------------------------------

cat > /etc/systemd/system/garage.service <<EOF
[Unit]
Description=Garage S3-compatible object storage
After=network.target

[Service]
ExecStart=/usr/local/bin/garage -c ${GARAGE_CONFIG} server
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now garage

# ---------------------------------------------------------------------------
# 4. Wait for Garage to become responsive
# ---------------------------------------------------------------------------

echo "==> Waiting for Garage to start..."
for i in $(seq 1 15); do
    if garage -c "${GARAGE_CONFIG}" status &>/dev/null; then
        break
    fi
    if [[ $i -eq 15 ]]; then
        echo "ERROR: Garage did not start within 30s. Check: journalctl -u garage" >&2
        exit 1
    fi
    sleep 2
done

# ---------------------------------------------------------------------------
# 5. Single-node layout
# ---------------------------------------------------------------------------

echo "==> Applying single-node layout..."
# Extract the node ID from the status output (first hex column)
NODE_ID=$(garage -c "${GARAGE_CONFIG}" status 2>&1 \
    | awk '/^[a-f0-9]{12}/ { print $1; exit }')

if [[ -z "${NODE_ID}" ]]; then
    echo "ERROR: could not determine node ID from 'garage status'" >&2
    garage -c "${GARAGE_CONFIG}" status >&2
    exit 1
fi

echo "    Node ID: ${NODE_ID}"
garage -c "${GARAGE_CONFIG}" layout assign -z dc1 -c 1G "${NODE_ID}"
garage -c "${GARAGE_CONFIG}" layout apply --version 1

# ---------------------------------------------------------------------------
# 6. Bucket and access key
# ---------------------------------------------------------------------------

echo "==> Creating bucket: ${BUCKET_NAME}"
garage -c "${GARAGE_CONFIG}" bucket create "${BUCKET_NAME}"

echo "==> Creating access key: ${KEY_NAME}"
KEY_OUTPUT=$(garage -c "${GARAGE_CONFIG}" key create "${KEY_NAME}")
ACCESS_KEY=$(echo "${KEY_OUTPUT}" | awk '/Key ID/{print $NF}')
SECRET_KEY=$(echo "${KEY_OUTPUT}" | awk '/Secret key/{print $NF}')

garage -c "${GARAGE_CONFIG}" bucket allow \
    --read --write --owner \
    "${BUCKET_NAME}" \
    --key "${ACCESS_KEY}"

# ---------------------------------------------------------------------------
# 7. Print credentials
# ---------------------------------------------------------------------------

PI_IP=$(hostname -I | awk '{print $1}')

echo ""
echo "========================================================"
echo "  Garage ${GARAGE_VERSION} running on ${PI_IP}:${S3_PORT}"
echo "========================================================"
echo ""
echo "  Bucket:     ${BUCKET_NAME}"
echo "  Access key: ${ACCESS_KEY}"
echo "  Secret key: ${SECRET_KEY}"
echo ""
echo "  Paste the following into k8s/garage-s3-secret.yaml"
echo "  replacing the AIRFLOW_CONN_GARAGE_S3 placeholder:"
echo ""
printf '  {"conn_type":"aws","login":"%s","password":"%s","extra":{"endpoint_url":"http://%s:%s","region_name":"garage"}}\n' \
    "${ACCESS_KEY}" "${SECRET_KEY}" "${PI_IP}" "${S3_PORT}"
echo ""
echo "========================================================"
