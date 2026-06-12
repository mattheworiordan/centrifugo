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

# 3b. DURABLE STORAGE (D6) — provision managed Redis so message history,
#     channel serials and materialized mutable-message state survive a
#     redeploy. Without this the server runs the in-process memory engine
#     (a fresh world on every push). Create Upstash Redis in the SAME region
#     as the app (lhr) and wire its connection string as the REDIS_URL
#     secret — the entrypoint flips the engine to Redis when it is present
#     (rediss:// TLS strings work too).
flyctl redis create                       # Upstash; pick region lhr, eviction DISABLED
# Take the connection string it prints (redis[s]://default:<pw>@<host>:<port>):
flyctl secrets set REDIS_URL='redis://default:<pw>@<host>:<port>' -a rt-poc-demo
#
#     IMPORTANT — eviction policy: managed Redis MUST run maxmemory-policy
#     noeviction. An LRU/volatile eviction policy can silently drop a
#     centrifuge history STREAM key mid-window, falsifying the durability
#     headline (a gap with no error). `flyctl redis create` defaults to
#     eviction DISABLED — confirm with `flyctl redis status <name>` and do
#     not enable eviction. Note the persistence mode for the record.

# 4. Build (on fly's remote builders — no local docker needed) + deploy.
#    --ha=false is REQUIRED: fly otherwise creates a second machine, and the
#    adapter's cross-node coordination (serial minting, in-process presence)
#    is not multi-node-safe until Phase 6 — two machines would diverge even
#    though they share the Redis broker. Single-node + Redis gives the
#    redeploy-durability outcome; multi-node is Phase 6.
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

### Verify durability across a redeploy (with Redis)

The point of D6: a redeploy no longer starts from a fresh world.

```sh
KEY="<appXXXX>.key0:<secret>"
# Publish, then force a redeploy, then read history — the message persists.
curl -u "$KEY" -H 'Content-Type: application/json' \
  -d '{"name":"before","data":"survives redeploy"}' \
  https://rt-poc-demo.fly.dev/channels/persisted:durable-smoke/messages
flyctl deploy --ha=false                  # new machine, fresh process memory
curl -u "$KEY" https://rt-poc-demo.fly.dev/channels/persisted:durable-smoke/messages
# → still returns the "before" message (it would have vanished pre-Redis).
```

Then repeat with the AIT chat demo: start a conversation, redeploy, reload —
the conversation (mutable-message state) is still there (D4 rebuilds it from
the Redis op stream). Browser-verify via the demo URL.

Notes:
- `fly.toml` keeps exactly one machine running (`auto_stop_machines =
  "off"`, `min_machines_running = 1`) and deploys use `--ha=false`.
  Single-node is required until Phase 6: the Redis engine makes message
  history, channel serials (D3 seed-from-history) and materialized
  mutable-message state (D4) durable across a restart/redeploy, but the
  adapter's cross-node coordination is not yet multi-node-safe.
- Durability scope with Redis: history, serials and materialized state
  survive a redeploy. The live presence SET re-syncs on reconnect (D5 —
  connection-scoped by design; presence *history* is durable on the shadow
  channel); other in-process caches (nonce dedup window, stats fixtures)
  reset on restart — documented divergences, not data loss.
- **Scaling to multiple machines (Phase 6, the `count=2` step is P6.5):**
  on the Redis engine WebSocket is cross-node — message fan-out, history,
  presence (P6.1), revocation (P6.2) and serial ordering (P6.3) all work
  across nodes. **Comet MUST be pinned to a single machine**, because its
  per-key state is in-process per-node (not Redis-shared): add a
  load-balancer rule routing `/comet/*` consistently to one machine (e.g. a
  fly [[http_service]] / fly-replay or a sticky route). Without the pin a
  comet per-key request can hit the wrong machine and get a `410 GONE` (the
  SDK then reconnects — functional but churny). WebSocket needs no such
  rule. This is a documented PoC limitation (DIVERGENCES.md → comet pinned
  to a single node); the nonce replay window (P6.2b) is likewise per-node.

  ```sh
  # P6.5 — scale to two machines (needs REDIS_URL already set; see step 3b).
  flyctl scale count=2 -a rt-poc-demo
  # Browser-verify cross-node: open the demo in two tabs and confirm chat
  # streams between them; force the tabs onto DIFFERENT machines to prove the
  # cross-node path (fly routes by `fly-force-instance=<machine-id>` header,
  # or open from two regions). The local two-node validation suite
  # (TestMultiNode*_P6_* with ABLY_REDIS_TEST=1) already proves delivery,
  # presence, revocation, serial order, cross-node resume and AIT rewind.
  ```
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
