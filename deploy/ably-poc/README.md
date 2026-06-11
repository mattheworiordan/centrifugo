# Deploying the Ably-on-Centrifugo PoC

Two pieces: the **server** on fly.io (this directory) and the **AIT
use-chat demo** on Vercel pointed at it. Unmodified Ably SDKs connect to
the fly URL exactly as they would to Ably — TLS on 443, WebSocket and
REST on the same host.

## One-time prerequisites (interactive logins)

```sh
flyctl auth login     # fly.io  — currently no token on this machine
npx --yes vercel@latest login   # Vercel — the stored token has expired
```

> CLI gotcha: the repo's `.tool-versions` pins nodejs 22.19.0, where the
> global `vercel` shim isn't installed. Use `npx --yes vercel@latest …`
> (or run from a directory without the pin).

## 1. Server → fly.io

From the repo root:

```sh
# 1. Mint FRESH keys (never deploy the committed test fixture —
#    its secrets are public in the repo). Keep the stderr summary.
umask 077
./deploy/ably-poc/mint-keys.sh > /tmp/minted-keys.json

# 2. Create the app (rename in fly.toml if the name is taken).
flyctl apps create rt-poc-demo

# 3. Store the keys as a secret; the entrypoint materializes them at
#    boot. The stdin form keeps the secret out of process args and
#    shell history.
flyctl secrets set ABLY_KEYS_JSON=- -a rt-poc-demo < /tmp/minted-keys.json

# 4. Build (on fly's remote builders — no local docker needed) + deploy.
#    --ha=false is REQUIRED: fly otherwise creates a second machine for
#    high availability, and the PoC's stores are in-memory per-node —
#    two machines would round-robin requests between two separate worlds.
flyctl deploy --ha=false
```

### Verify

```sh
# liveness (no auth required, same endpoint the health check uses)
curl https://rt-poc-demo.fly.dev/time

# authenticated REST round-trip with a minted key
KEY="<appXXXX>.key0:<secret>"   # from the mint-keys stderr summary
curl -u "$KEY" -H 'Content-Type: application/json' \
  -d '{"name":"hello","data":"from fly"}' \
  https://rt-poc-demo.fly.dev/channels/persisted:smoke/messages
curl -u "$KEY" https://rt-poc-demo.fly.dev/channels/persisted:smoke/messages

# full ably-js suite against the deployment (from .working/ably-js-pinned):
# point the harness's static-app fixture at the MINTED doc so keyStr matches.
export ABLY_TEST_STATIC_APP=1 \
       ABLY_TEST_STATIC_APP_FILE=/tmp/minted-keys.json \
       ABLY_ENDPOINT=rt-poc-demo.fly.dev \
       ABLY_PORT=443 ABLY_TLS_PORT=443 ABLY_USE_TLS=true
npx mocha test/realtime/connection.test.js --reporter min
```

Notes:
- `fly.toml` keeps exactly one machine running (`auto_stop_machines =
  "off"`, `min_machines_running = 1`): all PoC stores are in-memory and
  per-node — a second machine or a restart is a fresh world. Fine for a
  demo; it is one of the documented divergences.
- Logs: `flyctl logs -a rt-poc-demo`.

## 2. Demo → Vercel

The official AIT browser demo (`ably-ai-transport-js`
`demo/vercel/react/use-chat`, Next.js + Vercel `useChat` over Ably) runs
against the PoC unmodified except for the committed env-driven endpoint
patch (`scripts/ably-poc-harness/ait-demo-local-endpoint.patch`,
already applied to `.working/ait-pinned`).

With the server on fly (TLS on 443), the demo needs **only the endpoint
hostname** — no port overrides (the patch's `ABLY_PORT` path is for
plain-HTTP local runs only):

```sh
cd .working/ait-pinned            # SDK build first: the demo depends on it via link:
corepack pnpm@11.3.0 install && corepack pnpm@11.3.0 build

cd demo/vercel/react/use-chat
npx --yes vercel@latest link      # create/link the Vercel project
for kv in \
  "ABLY_API_KEY=<appXXXX>.key0:<secret>" \
  "ABLY_ENDPOINT=rt-poc-demo.fly.dev" \
  "NEXT_PUBLIC_ABLY_ENDPOINT=rt-poc-demo.fly.dev" \
  "MOCK_LLM=1"; do
  npx --yes vercel@latest env add "${kv%%=*}" production <<< "${kv#*=}"
done
npx --yes vercel@latest deploy --prod
```

Caveats — ALL of these were needed in practice (deployed 2026-06-11 as
https://rt-poc-chat-demo.vercel.app; the demo working tree in
`.working/ait-pinned` already carries every change below):
- **`link:` dependency**: vendor the built SDK tarball — `pnpm pack` at
  the ait-pinned root, copy the `.tgz` into the demo dir, point the
  dependency at `file:./ably-ai-transport-0.2.0.tgz`, and remove the
  `prebuild` script. The demo's `vercel.json` also carried an
  `installCommand` that escapes the upload root
  (`cd ../../../.. && pnpm install …`) — replace it with
  `pnpm install --no-frozen-lockfile`.
- **`ENABLE_EXPERIMENTAL_COREPACK=1`** (Vercel env var): without it
  Vercel ignores `packageManager: pnpm@11.3.0` and picks an ancient
  pnpm that dies on every registry fetch with `ERR_INVALID_THIS`.
- **pnpm 11 build-script approval**: pnpm 11 errors in CI on ignored
  build scripts (sharp, unrs-resolver) and no longer reads the `pnpm`
  field from package.json. Add a demo-local `pnpm-workspace.yaml`:
  `allowBuilds: { sharp: true, unrs-resolver: true }` (this also bounds
  the workspace so the ait-pinned root workspace is not consulted).
- **`.vercelignore`** (`node_modules`, `.next`, test artifacts):
  guarantees the vendored tarball uploads regardless of gitignore rules.
- Regenerate the demo's `pnpm-lock.yaml` after the dependency edit
  (`corepack pnpm@11.3.0 install`) or the remote install fails on the
  stale lockfile.
- The repo's `.tool-versions` pins a node version without the global
  `vercel`; run the CLI as `npx --yes vercel@latest … --cwd <demo-dir>`
  from outside the demo (its `devEngines` also rejects plain npx inside).
- `MOCK_LLM=1` keeps the agent deterministic — no AI provider key
  involved.
- Browser origins: the server config allows `*`, so the Vercel domain
  needs no allow-list change.

## 3. What to show

1. Open the Vercel demo URL — chat streams token-by-token through the
   PoC server (watch the debug pane for raw AIT frames).
2. `curl https://rt-poc-demo.fly.dev/time` — it speaks the Ably REST
   protocol.
3. Run any allowlisted ably-js suite against it (above) — unmodified
   official SDK tests, green.
