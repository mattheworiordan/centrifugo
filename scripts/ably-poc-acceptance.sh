#!/usr/bin/env bash
# ably-js acceptance sweep for the Ably-on-Centrifugo PoC (M9 gate).
#
# Boots the adapter on a dedicated port and runs the allowlisted ably-js
# suites (non-comet: the PoC is WebSocket-only). Suites included here are
# FULLY green; suites with categorized known-failures (realtime/message,
# realtime/presence — colon-bearing channel names, order-dependent
# presence trio, server-side filtering) are swept separately and tracked
# in .working/ably-centrifugo-poc/allowlist.json.
set -euo pipefail

PORT=8057
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ABLY_JS="$ROOT/.working/ably-js-pinned"
CONFIG="$ROOT/tmp/ably-poc-acceptance.json"

[ -d "$ABLY_JS/node_modules" ] || { echo "missing $ABLY_JS (pinned ably-js checkout with node_modules)"; exit 1; }

mkdir -p "$ROOT/tmp"
go build -o "$ROOT/tmp/centrifugo-ably" "$ROOT"

# Port-derived config: the dev profile with the acceptance port.
python3 - "$ROOT/config.ably-dev.json" "$CONFIG" "$PORT" << 'PYEOF'
import json, sys
src, dst, port = sys.argv[1], sys.argv[2], int(sys.argv[3])
c = json.load(open(src))
c["http_server"]["port"] = port
json.dump(c, open(dst, "w"), indent=2)
PYEOF

# Kill anything holding the port (pid files rot; the port is the truth).
lsof -ti tcp:$PORT | xargs kill 2>/dev/null || true
sleep 0.5

cd "$ROOT"  # relative keys_file path resolves from the repo root
./tmp/centrifugo-ably --config "$CONFIG" > tmp/ably-poc-acceptance-server.log 2>&1 &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null || true' EXIT

for i in $(seq 1 50); do
	curl -fsS -o /dev/null "http://localhost:$PORT/time" 2>/dev/null && break
	kill -0 $SERVER_PID 2>/dev/null || { echo "server exited early:"; tail -20 tmp/ably-poc-acceptance-server.log; exit 1; }
	sleep 0.2
done
curl -fsS -o /dev/null "http://localhost:$PORT/time"
echo "adapter up on :$PORT (pid $SERVER_PID)"

cd "$ABLY_JS"
export ABLY_TEST_STATIC_APP=1 ABLY_ENDPOINT=localhost ABLY_PORT=$PORT ABLY_TLS_PORT=8443 ABLY_USE_TLS=false

SUITES=(
	test/realtime/channel.test.js
	test/realtime/resume.test.js
	test/realtime/auth.test.js
	test/realtime/connection.test.js
	test/realtime/history.test.js
	test/realtime/updates-deletes.test.js
	test/rest/message.test.js
	test/rest/history.test.js
	test/rest/presence.test.js
	test/rest/time.test.js
	test/rest/request.test.js
	test/rest/updates-deletes.test.js
	test/realtime/encoding.test.js
)

FAILED=0
for suite in "${SUITES[@]}"; do
	[ -f "$suite" ] || { echo "[skip] $suite (not in pinned checkout)"; continue; }
	# Inverted filter: comet variants (WS-only PoC) everywhere; the
	# request suite additionally excludes checkput/checkpatch/checkdelete,
	# which call out to echo.ably.io (external infra, not this server).
	EXCLUDE="comet"
	case "$suite" in
		*rest/request.test.js) EXCLUDE="comet|checkput|checkpatch|checkdelete" ;;
	esac
	echo "=== $suite ==="
	if ! npx mocha "$suite" --reporter min --grep "$EXCLUDE" --invert; then
		FAILED=1
	fi
done

if [ "$FAILED" -ne 0 ]; then
	echo "ABLY-POC ACCEPTANCE: FAILED"
	exit 1
fi
echo "ABLY-POC ACCEPTANCE: GREEN"
