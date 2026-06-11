#!/bin/sh
# Materialize the API keys from the ABLY_KEYS_JSON secret before starting.
# The keys NEVER live in the image or the repo: mint a fresh set with
# deploy/ably-poc/mint-keys.sh and store it with `flyctl secrets set`.
set -eu

: "${ABLY_KEYS_JSON:?set the ABLY_KEYS_JSON secret first: deploy/ably-poc/mint-keys.sh | flyctl secrets set ABLY_KEYS_JSON=- }"

umask 077
printf '%s' "$ABLY_KEYS_JSON" > /tmp/keys.json

exec /usr/local/bin/rt-server --config /etc/rt/config.json
