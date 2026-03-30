#!/bin/bash
# init.sh — MedinovAI Harness 2.1
# Repo: medinovai-integration-gateway
# Tier: 2

set -euo pipefail

E_REPO_NAME="medinovai-integration-gateway"
mos_port="${PORT:-8000}"
mos_health_url="http://127.0.0.1:${mos_port}/health"

echo "=== MedinovAI init.sh starting ==="
echo "Repo: ${E_REPO_NAME}"
echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "[1/4] Installing dependencies..."
python3 -m pip install -r requirements.txt --quiet

echo "[2/4] Environment check (optional for local dev)..."
if [ -z "${APP_ENV:-}" ]; then
  echo "  Note: APP_ENV unset — OK for local scaffold"
fi

echo "[3/4] Starting uvicorn (background smoke test)..."
python3 -m uvicorn main:app --host 127.0.0.1 --port "${mos_port}" &
mos_server_pid=$!

cleanup() {
  if kill -0 "${mos_server_pid}" 2>/dev/null; then
    kill "${mos_server_pid}" 2>/dev/null || true
    wait "${mos_server_pid}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

echo "[4/4] Waiting for /health..."
mos_ok=0
for mos_i in $(seq 1 30); do
  if curl -sf "${mos_health_url}" >/dev/null 2>&1; then
    mos_ok=1
    break
  fi
  sleep 1
done

if [ "${mos_ok}" -ne 1 ]; then
  echo "INIT_FAILED: health check timeout"
  exit 1
fi

echo "Smoke test: GET ${mos_health_url} — OK"
echo "INIT_SUCCESS"
