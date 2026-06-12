# The Ably-on-Centrifugo experiment

If you arrived here after playing with the demo at
[rt-poc-chat-demo.vercel.app](https://rt-poc-chat-demo.vercel.app): the
"Ably" service behind it isn't Ably. It's
[Centrifugo](https://github.com/centrifugal/centrifugo) — an open-source
realtime server — with an Ably-protocol adapter bolted on, running
Redis-backed across two small fly.io machines at `rt-poc-demo.fly.dev`.
The demo is the official AIT (Ably AI Transport) browser demo, completely
unmodified, talking to it with the stock `ably-js` SDK.

## What this was

An experiment with two questions:

1. **Build vs adapt.** In the wider conversation about whether Ably
   should build its own open-source server or lean on existing OSS,
   how far can you get by teaching an existing open-source realtime
   server (Centrifugo) to speak the Ably protocol — well enough that
   unmodified official Ably SDKs just work against it?

2. **How autonomous can the build be.** The entire adapter was written
   by an LLM (Claude) running in unattended loops — no human wrote or
   edited a line of the code. Human involvement was limited to kickoff
   prompts, a few course corrections, and trying the demo. The agent
   used the public Ably feature spec and the official SDK test suites
   as its oracle, gated every commit through an independent
   code-review agent, and went from empty branch to the deployed demo
   in about three days (the bulk of the protocol surface landed on day
   one). The work prioritised the surface AIT needs — token streaming,
   mutable messages, resume — and grew outward from there.

This is a proof of concept. It is not a product, not a roadmap
commitment, and very deliberately not production software.

## What works

Everything below is verified by *unmodified official SDK test suites*
run against this server (`make ably-poc-test` reproduces it):

- **32 ably-js test suites fully green — 481 tests — in both wire
  encodings** (JSON and msgpack) **and over both transports**
  (WebSocket and comet/HTTP long-polling): connection, channels,
  resume/recover, auth (key, Ably-JWT, requestToken, capabilities,
  reauth), history and pagination, presence and presence sync,
  encryption (rides the encoding passthrough), batch publish, token
  revocation, stats, idempotent publishing, message updates/deletes/
  appends, transport fallback, and more.
- **ably-go conformance mirrors** green.
- **The AIT SDK's own integration suites: 45/45**, plus its e2e
  token-streaming demo surviving a mid-stream transport drop with
  byte-exact reassembly.
- **A real browser demo in production**: multi-tab sync, suspend/resume
  tool calls, mid-stream cancel, branching — all over the adapter.
- REST error surface aligned with the real service (same envelope
  shape, headers, help links, and the browser courtesy page —
  `serverId`/`X-Ably-Cluster` make it obvious which stack answered).
- **Durable and multi-node** (the later hardening + Redis work): on the
  Redis engine, message history, channel serials, and materialized
  mutable-message state survive a restart/redeploy, and the adapter runs
  across multiple nodes. Cross-node messaging, presence, token
  revocation, idempotent publish, connection resume, and the AIT chat all
  work across nodes; comet's per-key requests forward to their owning node
  over the broker control channel (no load-balancer pinning needed).
  **Deployed live at two machines** and verified there: a message survived
  a full restart of every machine, a publish on one machine was read on
  the other, and a comet session was driven end-to-end across machines.
  The single-node test gate stays green (481 tests) on **both** the memory
  and Redis engines.

Numbers, spec-point citations, and the per-suite breakdown live in
[SCORECARD.md](SCORECARD.md).

## What it doesn't do

The honest list is in [DIVERGENCES.md](DIVERGENCES.md). Headlines:

- **Not production availability — this is the big one.** The Redis
  engine makes state durable and multi-node, but that proves the
  *sharding and delivery* architecture, not the Four Pillars. There is no
  failover-without-message-loss on node death, no connection rebalancing,
  no multi-region, and durability rests on Redis's own persistence
  config; a single Redis is a SPOF. This is the expensive, unproven half,
  and the real next decision for the build-vs-adapt question.
- **It's a subset of the spec** — roughly 35–45% of Ably's addressable
  surface (the audit's estimate). **No push notifications, LiveObjects,
  annotations/summaries, delta compression (vcdiff), server-side message
  filtering, or channel enumeration/metachannels.**
- Auth is verify-only (no token minting service beyond `requestToken`),
  and several recovery/identity edges are softer than the real service.
- Smaller documented divergences (each bounded, none hidden) include a
  per-node nonce-replay window, per-node stats, and cosmetic cross-node
  serial ordering — see [DIVERGENCES.md](DIVERGENCES.md).

## What we learned

On the build-vs-adapt question, the experiment is a qualified **yes for
de-risking**, with one large caveat.

- **The cheap wins stayed cheap.** Pub/sub, history, presence, and —
  critically — *cross-node messaging* came almost for free from
  Centrifugo's Redis broker. Publish on one node, subscribe on another,
  shared history, idempotent publish: all from the library, not from us.
  That is the bulk of what makes a realtime server multi-node, and it's
  the strongest signal that "adapt" is a viable way to de-risk.
- **The Ably-specific surface was net-new but bounded.** Connection
  resume, presence SYNC, capability auth, msgpack, message serials,
  mutable messages, and the comet transport are all things Centrifugo
  doesn't model. Each became an adapter layer *on top* — Centrifugo is
  unmodified apart from mounting the adapter, not forked. The serial
  design proved forward-compatible: cursors resolve by offset lookup, not
  serial arithmetic, so going multi-node needed no serial rewrite.
- **Comet was the one genuinely awkward fit, and it's instructive.** Its
  per-request state can't live where WebSocket's does. The fix reused
  Centrifugo's own emulation trick — forward the request to the owning
  node over the control channel — which is the same shape real Ably uses
  (placement encoded in an opaque connectionKey). That it could be solved
  *within the stack* and portably is a good sign.
- **An LLM built all of it, unattended.** No human wrote or edited a line
  of adapter code; the agent used the public spec and the official SDK
  suites as its oracle and gated every commit through an independent
  review agent. The hardening, durability, and multi-node phases were
  also run as unattended loops. That is a data point about *how* such a
  thing can be built, independent of whether to ship it.

**The honest verdict:** adapting Centrifugo de-risks the *protocol
fidelity* and *sharding/delivery* claims convincingly and cheaply. It does
**not** de-risk *availability* — the Four Pillars — at all; that was
deliberately out of scope and is the harder, more expensive half. The open
question this PoC sets up but does not answer: does the path from here
(≈35–45% of spec, single-Redis, demo-grade availability) to a production
service stay cheaper than a greenfield build, once the availability work
and the long tail of spec divergences are paid for?

## Where to look

| | |
|---|---|
| [SCORECARD.md](SCORECARD.md) | What passes, with numbers and spec citations |
| [DIVERGENCES.md](DIVERGENCES.md) | Current behavioral differences from the Ably service |
| [`internal/ably/`](.) | The adapter itself (~all of the PoC code lives here) |
| [`deploy/ably-poc/README.md`](../../deploy/ably-poc/README.md) | How the fly.io server + Vercel demo are deployed |

## Trying it

```sh
# one-time: fetch + patch + build the pinned ably-js test suite
./scripts/ably-poc-setup.sh

# the full acceptance gate: native Go suites + ably-go conformance +
# the 32-suite ably-js sweep (~20 min)
make ably-poc-test

# or just point any Ably SDK at the deployed instance:
curl https://rt-poc-demo.fly.dev/time

# force the browser demo onto HTTP long-polling instead of WebSocket
# (watch the Network tab fill with /comet/connect, /send and /recv):
#   https://rt-poc-chat-demo.vercel.app/?transport=xhr_polling
```

The architecture, in one paragraph: each Ably realtime connection gets
its own embedded Centrifugo client; an adapter layer translates Ably
protocol frames (ATTACH/MESSAGE/PRESENCE/AUTH/…) to and from
Centrifugo's protocol, escapes Ably channel names into Centrifugo-safe
broker names, mints Ably-style message serials atomically with broker
appends so serial order always equals stream order, and keeps adapter-
owned stores for presence, materialized mutable messages, tokens, and
stats. On the Redis engine these go multi-node: the broker fans messages
across nodes, message history / serials / materialized state become
durable, presence and token revocation are Redis-backed or broadcast over
the control channel, and comet per-key requests are forwarded to their
owning node. Centrifugo itself is unmodified apart from mounting the
adapter.
