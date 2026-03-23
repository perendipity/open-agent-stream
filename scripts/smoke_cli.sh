#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/oas-smoke.XXXXXX")"
keep_dir="${KEEP_SMOKE_DIR:-0}"
daemon_started=0

cleanup() {
  if [[ "$daemon_started" == "1" && -x "$binary" && -f "$config_path" ]]; then
    "$binary" daemon stop -config "$config_path" >/dev/null 2>&1 || true
  fi
  if [[ "$keep_dir" != "1" ]]; then
    rm -rf "$work_dir"
  fi
}
trap cleanup EXIT

wait_for_daemon_status() {
  local expect_live="$1"
  local expect_ready="$2"
  local attempts="${3:-20}"

  for _ in $(seq 1 "$attempts"); do
    if "$binary" daemon status -config "$config_path" -json >"$work_dir/daemon-status.json" 2>/dev/null; then
      if python3 - "$work_dir/daemon-status.json" "$expect_live" "$expect_ready" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    status = json.load(handle)

expect_live = sys.argv[2] == "true"
expect_ready = sys.argv[3] == "true"

live = bool(status.get("live"))
ready = bool(status.get("ready"))
running = bool(status.get("running"))

if live == expect_live and ready == expect_ready and running == live:
    raise SystemExit(0)
raise SystemExit(1)
PY
      then
        return 0
      fi
    fi
    sleep 0.5
  done

  return 1
}

current_instance_id() {
  python3 - "$work_dir/daemon-status.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    status = json.load(handle)

print(status.get("instance_id", ""))
PY
}

bin_dir="$work_dir/bin"
binary="$bin_dir/oas"
config_path="$work_dir/oas.json"
events_path="$work_dir/events.jsonl"

mkdir -p "$bin_dir"

cd "$repo_root"
GOBIN="$bin_dir" go install ./cmd/oas

"$binary" version >/dev/null
"$binary" config init -output "$config_path"

python3 - "$config_path" "$work_dir" "$repo_root" <<'PY'
import json
import pathlib
import sys

config_path = pathlib.Path(sys.argv[1])
work_dir = pathlib.Path(sys.argv[2])
repo_root = pathlib.Path(sys.argv[3])

config = json.loads(config_path.read_text(encoding="utf-8"))
config["machine_id"] = "smoke-machine"
config["state_path"] = str(work_dir / "state.db")
config["ledger_path"] = str(work_dir / "ledger.db")

for source in config.get("sources", []):
    if source.get("type") == "codex_local":
        source["root"] = str(repo_root / "fixtures" / "sources" / "codex")
    if source.get("type") == "claude_local":
        source["root"] = str(repo_root / "fixtures" / "sources" / "claude")

config_path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
PY

"$binary" config print -config "$config_path" >/dev/null
"$binary" config print -config "$config_path" -json >/dev/null
"$binary" validate -config "$config_path" -root "$repo_root"
"$binary" run -config "$config_path" >/dev/null
"$binary" export -config "$config_path" -output "$events_path"

event_count="$(wc -l < "$events_path" | tr -d '[:space:]')"
if [[ "$event_count" != "6" ]]; then
  echo "expected 6 exported events, got $event_count" >&2
  exit 1
fi

"$binary" summary -input "$events_path" -sort recent -limit 20 >/dev/null

session_key="$(
python3 - "$events_path" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    for line in handle:
        line = line.strip()
        if not line:
            continue
        print(json.loads(line)["session_key"])
        break
    else:
        raise SystemExit("no exported events found")
PY
)"

"$binary" inspect -input "$events_path" -session "$session_key" >/dev/null
"$binary" doctor -config "$config_path" >/dev/null
"$binary" doctor -config "$config_path" -json >/dev/null
"$binary" daemon start -config "$config_path" >/dev/null
daemon_started=1

wait_for_daemon_status true true
first_instance_id="$(current_instance_id)"
if [[ -z "$first_instance_id" ]]; then
  echo "daemon status did not report an instance_id after start" >&2
  exit 1
fi

"$binary" daemon status -config "$config_path" >/dev/null
"$binary" daemon restart -config "$config_path" >/dev/null
wait_for_daemon_status true true

second_instance_id="$(current_instance_id)"
if [[ -z "$second_instance_id" ]]; then
  echo "daemon status did not report an instance_id after restart" >&2
  exit 1
fi
if [[ "$first_instance_id" == "$second_instance_id" ]]; then
  echo "daemon restart did not rotate the instance_id" >&2
  exit 1
fi

"$binary" daemon stop -config "$config_path" >/dev/null
daemon_started=0
wait_for_daemon_status false false 10

echo "CLI smoke test passed in $work_dir"
