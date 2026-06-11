#!/usr/bin/env bash
# Mint a FRESH keys document for a deployment — never deploy the
# committed test fixture (its secrets are public in the repo).
#
# Prints the keys JSON to stdout (pipe straight into a fly secret) and a
# summary of the minted API keys to stderr so you can hand them to
# demos/SDKs.
#
#   ./deploy/ably-poc/mint-keys.sh > /tmp/minted-keys.json
#   flyctl secrets set ABLY_KEYS_JSON="$(cat /tmp/minted-keys.json)"
#
# Document shape: what auth.LoadKeyStore reads (appId + keys with
# keyName/keySecret/capability; optional revocableTokens). No presence
# fixture channels — those exist for the pinned test suites only.
set -euo pipefail

rand() { openssl rand -hex "$1"; }

APP_ID="app$(rand 4)"
SECRET0="$(rand 24)"
SECRET1="$(rand 24)"
SECRET2="$(rand 24)"

cat <<EOF
{
  "_comment": "Minted $(date -u +%Y-%m-%dT%H:%M:%SZ) by mint-keys.sh — deployment keys, NOT the public test fixture.",
  "accountId": "${APP_ID}-account",
  "appId": "${APP_ID}",
  "keys": [
    {
      "id": "key0",
      "keyName": "${APP_ID}.key0",
      "keySecret": "${SECRET0}",
      "keyStr": "${APP_ID}.key0:${SECRET0}",
      "capability": "{\"*\":[\"*\"]}"
    },
    {
      "id": "key1",
      "keyName": "${APP_ID}.key1",
      "keySecret": "${SECRET1}",
      "keyStr": "${APP_ID}.key1:${SECRET1}",
      "capability": "{\"*\":[\"subscribe\"]}"
    },
    {
      "id": "key2",
      "keyName": "${APP_ID}.key2",
      "keySecret": "${SECRET2}",
      "keyStr": "${APP_ID}.key2:${SECRET2}",
      "capability": "{\"*\":[\"*\"]}",
      "revocableTokens": true
    }
  ]
}
EOF

cat >&2 <<EOF

Minted keys for app '${APP_ID}':
  full access      : ${APP_ID}.key0:${SECRET0}
  subscribe-only   : ${APP_ID}.key1:${SECRET1}
  full + revocable : ${APP_ID}.key2:${SECRET2}

Store them somewhere safe (1Password); the server only ever sees the
ABLY_KEYS_JSON secret.
EOF
