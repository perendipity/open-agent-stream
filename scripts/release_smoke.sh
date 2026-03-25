#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
BIN_DIR="${ROOT_DIR}/bin"
BIN_PATH="${BIN_DIR}/oas"
HTTP_LOG="${TMP_DIR}/http.log"
HTTP_PID=""

cleanup() {
  if [[ -n "${HTTP_PID}" ]] && kill -0 "${HTTP_PID}" 2>/dev/null; then
    kill "${HTTP_PID}" 2>/dev/null || true
    wait "${HTTP_PID}" 2>/dev/null || true
  fi
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

mkdir -p "${BIN_DIR}"

cat > "${TMP_DIR}/oas-release-smoke.json" <<JSON
{
  "version": "0.1",
  "machine_id": "release-smoke-machine",
  "state_path": "${TMP_DIR}/state.db",
  "ledger_path": "${TMP_DIR}/ledger.db",
  "batch_size": 64,
  "sources": [
    {
      "instance_id": "codex-local",
      "type": "codex_local",
      "root": "${ROOT_DIR}/fixtures/sources/codex"
    }
  ],
  "sinks": [
    {
      "id": "remote-http",
      "type": "http",
      "event_spec_version": "v2",
      "settings": {
        "url": "http://127.0.0.1:18088/ingest",
        "probe_url": "http://127.0.0.1:18088/ready",
        "format": "oas_batch_json"
      },
      "delivery": {
        "max_batch_events": 1,
        "max_batch_age": "1s",
        "retry_initial_backoff": "1s",
        "retry_max_backoff": "5s",
        "poison_after_failures": 3
      }
    }
  ]
}
JSON

python3 - <<'PY' > "${HTTP_LOG}" 2>&1 &
from http.server import BaseHTTPRequestHandler, HTTPServer
class Handler(BaseHTTPRequestHandler):
    def do_HEAD(self):
        if self.path == "/ready":
            self.send_response(204)
            self.end_headers()
            return
        self.send_response(405)
        self.end_headers()
    def do_POST(self):
        if self.path != "/ingest":
            self.send_response(404)
            self.end_headers()
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        print(f"POST {self.path} bytes={len(body)}")
        self.send_response(204)
        self.end_headers()
HTTPServer(("127.0.0.1", 18088), Handler).serve_forever()
PY
HTTP_PID=$!
sleep 1

go build -o "${BIN_PATH}" ./cmd/oas
"${BIN_PATH}" version
"${BIN_PATH}" validate -config "${TMP_DIR}/oas-release-smoke.json" -root "${ROOT_DIR}"
"${BIN_PATH}" run -config "${TMP_DIR}/oas-release-smoke.json"
"${BIN_PATH}" doctor -config "${TMP_DIR}/oas-release-smoke.json"
"${BIN_PATH}" delivery status -config "${TMP_DIR}/oas-release-smoke.json"

grep -q "POST /ingest" "${HTTP_LOG}"
