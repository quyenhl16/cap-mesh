#!/bin/bash
set -euo pipefail

# ============================================================
# Configuration
# ============================================================
SERVICE_NAME="capmesh-server"
NAMESPACE="capmesh"
VALID_DAYS=3650

CA_CERT="ca.crt"
CA_KEY="ca.key"

SERVER_KEY="server.key"
SERVER_CSR="server.csr"
SERVER_CERT="server.crt"
SERIAL_FILE="ca.cert.srl"
EXT_FILE="server.ext"

echo "=== Generate TLS certificates ==="
echo "Service  : ${SERVICE_NAME}"
echo "Namespace: ${NAMESPACE}"
echo

# ============================================================
# Clean old generated files
# ============================================================
rm -f \
    "${CA_CERT}" \
    "${CA_KEY}" \
    "${SERVER_KEY}" \
    "${SERVER_CSR}" \
    "${SERVER_CERT}" \
    "${SERIAL_FILE}" \
    "${EXT_FILE}"

# ============================================================
# 1. Generate CA private key
# ============================================================
echo "[1/4] Generate CA key..."

openssl genrsa \
    -out "${CA_KEY}" \
    2048

# ============================================================
# 2. Generate CA certificate
# ============================================================
echo "[2/4] Generate CA certificate..."

openssl req -x509 \
    -new \
    -nodes \
    -key "${CA_KEY}" \
    -sha256 \
    -days "${VALID_DAYS}" \
    -out "${CA_CERT}" \
    -subj "/C=VN/O=VHT/CN=VHT Test CA"

# ============================================================
# 3. Generate server private key + CSR
# ============================================================
echo "[3/4] Generate server key and CSR..."

openssl genrsa \
    -out "${SERVER_KEY}" \
    2048

openssl req \
    -new \
    -key "${SERVER_KEY}" \
    -out "${SERVER_CSR}" \
    -subj "/C=VN/O=VHT/CN=${SERVICE_NAME}"

# ============================================================
# 4. Generate server certificate
# ============================================================
echo "[4/4] Generate server certificate..."

cat > "${EXT_FILE}" <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:${SERVICE_NAME},DNS:${SERVICE_NAME}.${NAMESPACE},DNS:${SERVICE_NAME}.${NAMESPACE}.svc,DNS:${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local
EOF

openssl x509 \
    -req \
    -in "${SERVER_CSR}" \
    -CA "${CA_CERT}" \
    -CAkey "${CA_KEY}" \
    -CAcreateserial \
    -out "${SERVER_CERT}" \
    -days "${VALID_DAYS}" \
    -sha256 \
    -extfile "${EXT_FILE}"

# ============================================================
# Verify
# ============================================================
echo
echo "=== Verify certificate ==="

openssl verify \
    -CAfile "${CA_CERT}" \
    "${SERVER_CERT}"

echo
echo "=== Server certificate SAN ==="

openssl x509 \
    -in "${SERVER_CERT}" \
    -noout \
    -subject \
    -issuer \
    -ext subjectAltName

# ============================================================
# Remove files not required by server/client
# ============================================================
rm -f \
    "${CA_KEY}" \
    "${SERVER_CSR}" \
    "${SERIAL_FILE}" \
    "${EXT_FILE}"

# ============================================================
# Set permissions
# ============================================================
chmod 644 "${CA_CERT}" "${SERVER_CERT}"
chmod 600 "${SERVER_KEY}"

echo
echo "=== Generated files ==="
ls -lh \
    "${CA_CERT}" \
    "${SERVER_CERT}" \
    "${SERVER_KEY}"

echo
echo "Done."

