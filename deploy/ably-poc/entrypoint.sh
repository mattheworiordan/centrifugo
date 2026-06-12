#!/bin/sh
# Materialize the API keys from the ABLY_KEYS_JSON secret before starting.
# The keys NEVER live in the image or the repo: mint a fresh set with
# deploy/ably-poc/mint-keys.sh and store it with `flyctl secrets set`.
set -eu

: "${ABLY_KEYS_JSON:?set the ABLY_KEYS_JSON secret first: deploy/ably-poc/mint-keys.sh | flyctl secrets set ABLY_KEYS_JSON=- }"

umask 077
printf '%s' "$ABLY_KEYS_JSON" > /tmp/keys.json

# Durable storage (D6): when REDIS_URL is set, run the Redis engine so message
# history, channel serials and materialized mutable-message state survive a
# redeploy (the resilient-PoC outcome — the deployed demo no longer starts
# from a fresh world on every push). Without it the server falls back to the
# in-process memory engine (ephemeral). REDIS_URL is a redis:// (or rediss://
# for TLS, as managed Redis like Upstash uses) connection string, typically
# from `flyctl redis create`. Centrifugo env-overrides take precedence over
# config.json, so this flips the engine without editing the baked-in config.
if [ -n "${REDIS_URL:-}" ]; then
	export CENTRIFUGO_ENGINE_TYPE=redis
	export CENTRIFUGO_ENGINE_REDIS_ADDRESS="$REDIS_URL"
	echo "ably-poc: REDIS_URL set — Redis engine enabled (durable storage across redeploys)"
else
	echo "ably-poc: REDIS_URL not set — using the in-process memory engine (ephemeral; a fresh world on every deploy)"
fi

exec /usr/local/bin/rt-server --config /etc/rt/config.json
