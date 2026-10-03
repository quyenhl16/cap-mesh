#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="capmesh-server"
NAMESPACE="capmesh"
VALID_DAYS=3650
SERVER_ADDRESS="capmesh-server:18443"
CONTEXT_NAME="default"
CONFIG_DIRECTORY="${CAPMESH_CONFIG_DIR:-}"
TOKEN_FILE=""

usage() {
    cat <<'EOF'
Usage: deploy/generate.sh [options]

Generate the CapMesh CA/server certificate and install a client config.

Options:
  --server ADDRESS          Client server address (default: capmesh-server:18443)
  --context NAME           Client context name (default: default)
  --config-dir DIRECTORY   Destination directory for config.yaml and ca.crt
  --token-file FILE        Token file referenced by the client context
  --service-name NAME      Kubernetes service certificate name
  --namespace NAME         Kubernetes namespace used in certificate SANs
  --valid-days DAYS        Certificate validity (default: 3650)
  -h, --help               Show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --server) SERVER_ADDRESS="${2:?missing value for --server}"; shift 2 ;;
        --context) CONTEXT_NAME="${2:?missing value for --context}"; shift 2 ;;
        --config-dir) CONFIG_DIRECTORY="${2:?missing value for --config-dir}"; shift 2 ;;
        --token-file) TOKEN_FILE="${2:?missing value for --token-file}"; shift 2 ;;
        --service-name) SERVICE_NAME="${2:?missing value for --service-name}"; shift 2 ;;
        --namespace) NAMESPACE="${2:?missing value for --namespace}"; shift 2 ;;
        --valid-days) VALID_DAYS="${2:?missing value for --valid-days}"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

if [[ ! "${CONTEXT_NAME}" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "Invalid context name: ${CONTEXT_NAME}" >&2
    exit 2
fi
if [[ ! "${VALID_DAYS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "--valid-days must be a positive integer" >&2
    exit 2
fi

if [[ -z "${CONFIG_DIRECTORY}" ]]; then
    if [[ -n "${APPDATA:-}" ]]; then
        CONFIG_DIRECTORY="${APPDATA}/capmesh"
    elif [[ "$(uname -s)" == "Darwin" ]]; then
        CONFIG_DIRECTORY="${HOME}/Library/Application Support/capmesh"
    elif [[ -n "${XDG_CONFIG_HOME:-}" ]]; then
        CONFIG_DIRECTORY="${XDG_CONFIG_HOME}/capmesh"
    else
        CONFIG_DIRECTORY="${HOME}/.config/capmesh"
    fi
fi

CA_CERT="ca.crt"
CA_KEY="ca.key"
SERVER_KEY="server.key"
SERVER_CSR="server.csr"
SERVER_CERT="server.crt"
SERIAL_FILE="ca.cert.srl"
EXT_FILE="server.ext"

echo "=== Generate TLS certificates and client config ==="
echo "Service      : ${SERVICE_NAME}"
echo "Namespace    : ${NAMESPACE}"
echo "Client server: ${SERVER_ADDRESS}"
echo "Context      : ${CONTEXT_NAME}"
echo "Config dir   : ${CONFIG_DIRECTORY}"
echo

rm -f "${CA_CERT}" "${CA_KEY}" "${SERVER_KEY}" "${SERVER_CSR}" \
    "${SERVER_CERT}" "${SERIAL_FILE}" "${EXT_FILE}"

echo "[1/5] Generate CA key and certificate..."
openssl genrsa -out "${CA_KEY}" 2048
openssl req -x509 -new -nodes -key "${CA_KEY}" -sha256 -days "${VALID_DAYS}" \
    -out "${CA_CERT}" -subj "/C=VN/O=VHT/CN=VHT Test CA"

echo "[2/5] Generate server key and CSR..."
openssl genrsa -out "${SERVER_KEY}" 2048
openssl req -new -key "${SERVER_KEY}" -out "${SERVER_CSR}" \
    -subj "/C=VN/O=VHT/CN=${SERVICE_NAME}"

echo "[3/5] Sign server certificate..."
cat > "${EXT_FILE}" <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:${SERVICE_NAME},DNS:${SERVICE_NAME}.${NAMESPACE},DNS:${SERVICE_NAME}.${NAMESPACE}.svc,DNS:${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local
EOF
openssl x509 -req -in "${SERVER_CSR}" -CA "${CA_CERT}" -CAkey "${CA_KEY}" \
    -CAcreateserial -out "${SERVER_CERT}" -days "${VALID_DAYS}" -sha256 -extfile "${EXT_FILE}"

echo "[4/5] Verify certificate..."
openssl verify -CAfile "${CA_CERT}" "${SERVER_CERT}"
openssl x509 -in "${SERVER_CERT}" -noout -subject -issuer -ext subjectAltName

echo "[5/5] Install client config..."
mkdir -p "${CONFIG_DIRECTORY}"
CONFIG_DIRECTORY="$(cd "${CONFIG_DIRECTORY}" && pwd -P)"
chmod 700 "${CONFIG_DIRECTORY}" 2>/dev/null || true
SOURCE_CA="$(pwd -P)/${CA_CERT}"
if [[ "${SOURCE_CA}" != "${CONFIG_DIRECTORY}/ca.crt" ]]; then
    cp "${CA_CERT}" "${CONFIG_DIRECTORY}/ca.crt"
fi
chmod 644 "${CONFIG_DIRECTORY}/ca.crt" 2>/dev/null || true

CONFIG_PATH="${CONFIG_DIRECTORY}/config.yaml"
if [[ -f "${CONFIG_PATH}" ]]; then
    BACKUP_PATH="${CONFIG_PATH}.bak.$(date +%Y%m%d%H%M%S)"
    cp "${CONFIG_PATH}" "${BACKUP_PATH}"
    echo "Existing client config backed up to ${BACKUP_PATH}"
fi

CA_CONFIG_PATH="${CONFIG_DIRECTORY}/ca.crt"
if command -v cygpath >/dev/null 2>&1; then
    CA_CONFIG_PATH="$(cygpath -m "${CA_CONFIG_PATH}")"
    if [[ -n "${TOKEN_FILE}" ]]; then
        TOKEN_FILE="$(cygpath -m "${TOKEN_FILE}")"
    fi
fi

escape_yaml() {
    local value="$1"
    value="${value//\\/\\\\}"
    value="${value//\"/\\\"}"
    printf '%s' "${value}"
}

{
    echo "apiVersion: capmesh.io/v1"
    echo "kind: ClientConfig"
    printf 'currentContext: "%s"\n' "$(escape_yaml "${CONTEXT_NAME}")"
    echo "contexts:"
    printf '  "%s":\n' "$(escape_yaml "${CONTEXT_NAME}")"
    printf '    server: "%s"\n' "$(escape_yaml "${SERVER_ADDRESS}")"
    echo "    insecure: false"
    printf '    tlsCA: "%s"\n' "$(escape_yaml "${CA_CONFIG_PATH}")"
    printf '    tlsServerName: "%s"\n' "$(escape_yaml "${SERVICE_NAME}")"
    if [[ -n "${TOKEN_FILE}" ]]; then
        printf '    tokenFile: "%s"\n' "$(escape_yaml "${TOKEN_FILE}")"
    fi
    echo "    defaults:"
    echo "      snaplen: 4096"
    echo "      ttl: 5m"
    echo "      reorderWindow: 300ms"
    echo "      direction: egress"
    echo "      follow: true"
    echo "      maxPods: 100"
} > "${CONFIG_PATH}"
chmod 600 "${CONFIG_PATH}" 2>/dev/null || true

rm -f "${CA_KEY}" "${SERVER_CSR}" "${SERIAL_FILE}" "${EXT_FILE}"
chmod 644 "${CA_CERT}" "${SERVER_CERT}" 2>/dev/null || true
chmod 600 "${SERVER_KEY}" 2>/dev/null || true

echo
echo "=== Generated server files ==="
ls -lh "${CA_CERT}" "${SERVER_CERT}" "${SERVER_KEY}"
echo
echo "Client config: ${CONFIG_PATH}"
echo "Done."
