#!/usr/bin/env bash
# ably-js acceptance sweep for the Ably-on-Centrifugo PoC.
#
# Boots the adapter on a dedicated port and runs the allowlisted ably-js
# suites over BOTH transports (WebSocket and comet/HTTP long-polling —
# Phase 4). Suites included here are FULLY green; suites with
# categorized known-failures (realtime/message, realtime/presence —
# order-dependent presence trio, server-side filtering) are swept
# separately and tracked in .working/ably-centrifugo-poc/allowlist.json.
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
	test/realtime/crypto.test.js
	test/realtime/connectivity.test.js
	test/realtime/event_emitter.test.js
	test/realtime/init.test.js
	test/realtime/api.test.js
	test/realtime/utils.test.js
	test/realtime/reauth.test.js
	test/realtime/failure.test.js
	test/realtime/sync.test.js
	test/rest/defaults.test.js
	test/rest/api.test.js
	test/rest/bufferutils.test.js
	test/rest/status.test.js
	test/rest/batch.test.js
	test/rest/http.test.js
	test/rest/stats.test.js
	test/rest/init.test.js
	test/rest/fallbacks.test.js
	test/realtime/transports.test.js
)

FAILED=0
for suite in "${SUITES[@]}"; do
	[ -f "$suite" ] || { echo "[skip] $suite (not in pinned checkout)"; continue; }
	# Comet/HTTP-fallback variants RUN since Phase 4 (the adapter serves
	# both Ably transports). Remaining per-suite exclusions are external-
	# infra or harness-environment artifacts, each documented:
	#   - rest/request checkput/checkpatch/checkdelete and
	#     realtime/transports no_internet_connectivity call out to
	#     echo.ably.io (external infra, not this server);
	#   - rest/init "without any tls key" and rest/fallbacks "primary
	#     domain as the first attempted" assert SDK default-TLS URL
	#     construction, which the local test env must override
	#     (ABLY_USE_TLS=false / explicit port) to reach this server.
	EXCLUDE=""
	case "$suite" in
		*rest/request.test.js) EXCLUDE="checkput|checkpatch|checkdelete" ;;
		*rest/init.test.js) EXCLUDE="without any tls key" ;;
		*rest/fallbacks.test.js) EXCLUDE="primary domain as the first attempted" ;;
		*realtime/transports.test.js) EXCLUDE="no_internet_connectivity" ;;
	esac
	echo "=== $suite ==="
	GREP_ARGS=()
	if [ -n "$EXCLUDE" ]; then
		GREP_ARGS=(--grep "$EXCLUDE" --invert)
	fi
	# ${arr[@]+...}: empty-array expansion is an unbound-variable error
	# under set -u on macOS stock bash 3.2.
	if ! npx mocha "$suite" --reporter min ${GREP_ARGS[@]+"${GREP_ARGS[@]}"}; then
		FAILED=1
	fi
done

if [ "$FAILED" -ne 0 ]; then
	echo "ABLY-POC ACCEPTANCE: FAILED"
	exit 1
fi
echo "ABLY-POC ACCEPTANCE: GREEN"
