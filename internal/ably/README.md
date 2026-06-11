# The Ably-on-Centrifugo experiment

If you arrived here after playing with the demo at
[rt-poc-chat-demo.vercel.app](https://rt-poc-chat-demo.vercel.app): the
"Ably" service behind it isn't Ably. It's
[Centrifugo](https://github.com/centrifugal/centrifugo) — an open-source
realtime server — with an Ably-protocol adapter bolted on, running on a
single small fly.io machine at `rt-poc-demo.fly.dev`. The demo is the
official AIT (Ably AI Transport) browser demo, completely unmodified,
talking to it with the stock `ably-js` SDK.

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

- **31 ably-js test suites fully green — 413 tests — in both wire
  encodings** (JSON and msgpack): connection, channels, resume/recover,
  auth (key, Ably-JWT, requestToken, capabilities, reauth), history and
  pagination, presence and presence sync, encryption (rides the
  encoding passthrough), batch publish, token revocation, stats, idempotent
  publishing, message updates/deletes/appends, and more.
- **ably-go conformance mirrors** green.
- **The AIT SDK's own integration suites: 45/45**, plus its e2e
  token-streaming demo surviving a mid-stream transport drop with
  byte-exact reassembly.
- **A real browser demo in production**: multi-tab sync, suspend/resume
  tool calls, mid-stream cancel, branching — all over the adapter.
- REST error surface aligned with the real service (same envelope
  shape, headers, help links, and the browser courtesy page —
  `serverId`/`X-Ably-Cluster` make it obvious which stack answered).

Numbers, spec-point citations, and the per-suite breakdown live in
[SCORECARD.md](SCORECARD.md).

## What it doesn't do

The honest list is in [DIVERGENCES.md](DIVERGENCES.md). Headlines:

- **WebSocket only** — no Comet/XHR transports (browser SDKs that probe
  comet get a graceful decline and keep their WebSocket).
- **Single node, in-memory** — no clustering, no durable storage; a
  restart is a fresh world. Message history retention mimics Ably's
  tiers but lives in RAM.
- **No push notifications, LiveObjects, annotations/summaries, delta
  compression (vcdiff), server-side message filtering, or channel
  enumeration/metachannels.**
- Auth is verify-only (no token minting service beyond `requestToken`),
  and several recovery/identity edges are softer than the real service.

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
# the 31-suite ably-js sweep (~15 min)
make ably-poc-test

# or just point any Ably SDK at the deployed instance:
curl https://rt-poc-demo.fly.dev/time
```

The architecture, in one paragraph: each Ably realtime connection gets
its own embedded Centrifugo client; an adapter layer translates Ably
protocol frames (ATTACH/MESSAGE/PRESENCE/AUTH/…) to and from
Centrifugo's protocol, escapes Ably channel names into Centrifugo-safe
broker names, mints Ably-style message serials atomically with broker
appends so serial order always equals stream order, and keeps adapter-
owned stores for presence, materialized mutable messages, tokens, and
stats. Centrifugo itself is unmodified apart from mounting the adapter.
