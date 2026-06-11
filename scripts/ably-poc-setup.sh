#!/usr/bin/env bash
# One-time setup for the Ably-on-Centrifugo PoC acceptance gate
# (make ably-poc-test): clones the pinned ably-js, applies the
# static-app harness patch, builds the SDK, and places the static app
# fixture where the patched harness looks for it. Idempotent — skips
# anything already in place. Needs git, node and npm.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PINNED="$ROOT/.working/ably-js-pinned"
HARNESS_SRC="$ROOT/scripts/ably-poc-harness"
HARNESS_DST="$ROOT/.working/ably-centrifugo-poc/harness"

mkdir -p "$ROOT/.working" "$HARNESS_DST"

if [ ! -d "$PINNED/.git" ]; then
	echo "==> cloning ably-js 2.22.1 (pinned)"
	git clone --branch 2.22.1 --depth 1 https://github.com/ably/ably-js "$PINNED"
else
	echo "==> ably-js pinned checkout present"
fi

# The harness patch teaches the ably-js test app manager a static-app
# mode (ABLY_TEST_STATIC_APP=1): no sandbox provisioning, the app comes
# from the fixture file below.
if ! git -C "$PINNED" diff --quiet -- test/common/modules/testapp_manager.js 2>/dev/null; then
	echo "==> harness patch already applied"
else
	echo "==> applying static-app harness patch"
	git -C "$PINNED" apply "$HARNESS_SRC/static-app-harness.diff"
fi

# The fixture lives at the path the patch resolves by default.
cp "$HARNESS_SRC/static-app.json" "$HARNESS_DST/static-app.json"

if [ ! -d "$PINNED/node_modules" ]; then
	echo "==> npm install (pinned ably-js)"
	(cd "$PINNED" && npm install)
fi
if [ ! -f "$PINNED/build/ably-node.js" ]; then
	echo "==> building ably-js node bundle"
	(cd "$PINNED" && npm run build:node)
fi

echo "==> done. Run: make ably-poc-test"
echo "    (optional) Redis engine (WP-D): docker compose -f deploy/ably-poc/docker-compose.yml up -d redis"
echo "               then: REDIS=1 ./scripts/ably-poc-acceptance.sh"
