# Ably-on-Centrifugo PoC — conformance scorecard

> What this is and why it exists: see [README.md](README.md). Current
> behavioral gaps: [DIVERGENCES.md](DIVERGENCES.md).
>
> Snapshot 2026-06-11 (current). The reproducible gate is
> `make ably-poc-test` (native Go suites + ably-go conformance mirrors +
> the ably-js acceptance sweep below). All acceptance runs use
> **unmodified SDKs**: ably-js 2.22.1 (pinned), ably-go v1.4.1
> (conformance mirrors), @ably/ai-transport 0.2.0 (its own integration
> suites). Deployed live: server `rt-poc-demo.fly.dev` (now Redis-backed,
> durable, running at `count=2` multi-node), demo
> `rt-poc-chat-demo.vercel.app` (browser-verified end-to-end, including
> multi-tab sync).

## Headline

- **90 allowlisted test entries green in BOTH wire encodings** (JSON + msgpack)
  across milestones M0–M8; every attemptable pinned test passes.
- **32 ably-js suites fully green, 481 tests, BOTH transports** (the
  `make ably-poc-test` sweep — WebSocket AND comet/HTTP long-polling since
  Phase 4): realtime channel, resume, auth, connection, history,
  updates-deletes, encoding, crypto, connectivity, event_emitter, init,
  api, utils, reauth, failure, sync, transports; rest message, history,
  presence, time, request, updates-deletes, defaults, api, bufferutils,
  status, batch, http, stats, init, fallbacks.
- **The AIT SDK's own integration suites pass 45/45** (4 files) against this
  server with no AIT-specific shimming, and the e2e token-streaming demo
  survives a mid-stream transport drop with byte-exact assembly.
- **80 distinct spec points** cited by green tests (every citation verified
  against `ably/specification` features.textile; every error code verified
  against `ably-common/protocol/errors.json`).

## By milestone

| Milestone | Scope | Green entries |
|---|---|---|
| M0 | Architecture spike (Path A: per-connection centrifuge client) | 1/1 |
| M1–M2 | Wire protocol, channel lifecycle, publish/ACK, msgpack | 20/20 |
| M3 | REST surface, history + pagination, idempotency | 10/11 (1 excluded: no plain realtime-history test exists in the pinned suite) |
| M4 | Auth: keys, Ably-JWT, requestToken, capabilities | 4/4 (+5 planned exclusions: echo.ably.io embedded-JWT rest tests) |
| M5 | Presence: lifecycle, grace, REST, fixtures | 2/2 (+3 categorized: order-dependent trio, pass isolated) |
| M6 | Continuity: params/modes, channelSerial, gap replay, rewind, untilAttach, RTN16-lite recover | 22/22 — resume.test.js 13/13 |
| M7 | Reauth: AUTH frames, token expiry, RTN22 renewal prompts | 10/10 — realtime/auth 69 passing, 0 failing |
| M8 | AIT mutable messages: serials, mutation, materialized reads, AIT suites | 21/21 — updates-deletes 6/6 + 13/13, AIT 45/45 |

## Phase 3 hardening (post-DoD, 2026-06-11)

| Slice | What changed | Result |
|---|---|---|
| T1.1 | Colon channel names: bijective broker-name escape (`:`→`~1`, `~`→`~0`) at every broker boundary | realtime/message 33/9 → 41/1 |
| T1.2 | Per-channel publish lock: serial mint + broker append atomic, serial order == offset order | concurrency hammer green ×18 race runs |
| T1.3 | RSC22 batch publish, BAR1 batch presence, RSA17 token revocation, RTC8a1 reauth capability downgrade, RSC6/TS12 stats (fixtures, aggregation, Link pagination), comet soft-decline, RTP4 presence SYNC paging | rest/batch 6/6, reauth 16/16, rest/stats 9/9, rest/http 2/2, failure 18/18, sync 6/6, rest/init 5/5, fallbacks 3/3 |
| T1.4 | Acceptance gate expanded 13 → 31 suites | **413 tests GREEN** |
| T1.5 | fly.io deployment kit + Vercel demo deploy | live and browser-verified |
| Phase 4 | Comet/HTTP-fallback transport (4 commits): frameConn extraction, cometConn + /comet/connect and /recv (long-poll single-flight, heartbeat-flushed), send/close/disconnect, gate flip. Poll-only like production; JSON-only by SDK design. The reauth/resume/failure comet surfaces passed by construction — the session machinery is transport-agnostic | gate 413 → **481 tests / 32 suites**; `break_transport` un-categorized and green; browser demo verified over long-polling (`?transport=xhr_polling`) |
| Post-deploy | Fixes found by exercising the LIVE demo: persistent retention tier for `ai:`/`mutable:` channels, pagination truncation over size-evicted windows, CORS preflight 204 + exposed pagination headers (browser-only failure modes no server-side suite can see), REST error surface aligned with the real service (envelope shape, help links, serverId/cluster provenance, browser courtesy page) | multi-tab sync verified live; error responses mirror realtime.ably.io side-by-side |
| Phase 5 | **Hardening + Redis durable storage.** WP-A (CRITICAL/HIGH: comet per-key auth, publish-ack lock ordering, store bounds, token-expiry race), WP-B (mediums + concurrency incl. commit-after-publish), WP-C (Tier-1 input hardening: msgSerial validation, JSON depth, UTF-8, batch caps, inbound rate), WP-D (D1–D7: Redis as a first-class engine, serial seed-from-history, **D4 materialized durability** via op-stream rebuild, presence assessment, **D7 cross-process restart proof** + memory anti-theater control) | gate GREEN 481/481 on BOTH memory and Redis engines; AIT 45/45; D7 restart suite green; each item code-reviewer-PASS |
| Phase 6 | **Multi-node (P6.0–P6.5).** Two adapter nodes against one Redis: P6.0 harness, P6.1 cross-node presence (Redis presence manager), P6.2 cross-node revocation (broker feed + sweep) / idempotency (free) / nonce-bound (documented), P6.3 serial-order audit (offset-driven, no rewrite), P6.4 comet single-node affinity (documented), P6.5 two-node validation suite (cross-node delivery / presence / revocation / resume / AIT rewind). WebSocket fully cross-node. **Deployed live at `count=2`** and verified: a message survived a full restart of every machine (Redis durability), and a publish forced to machine A was read on machine B (cross-node shared state). Comet is NOT yet LB-pinned in production — it churns at count=2 (410→reconnect); a reliable-multi-node-comet fix is under investigation | `TestMultiNode*_P6_*` green under `ABLY_REDIS_TEST=1`; single-node gate still 481/481 both engines; live durability + cross-node confirmed on `rt-poc-demo.fly.dev`; each item code-reviewer-PASS |

## Known-failure categorizations (not regressions; tracked, never hidden)

| Pattern | Why |
|---|---|
| `Init without any tls key` (rest/init), `primary domain as the first attempted` (rest/fallbacks) | assert SDK default-TLS URL construction, which the local test env must override to reach the plain-HTTP adapter; server never contacted |
| `no_internet_connectivity` (realtime/transports) | calls echo.ably.io (external infra) for the SDK's connectivity probe; passes when online, excluded so the gate runs offline |
| `subscribes to filtered channel` | server-side message filtering not in scope |
| presence trio (`multiple_pending`, `presence_auto_reenter_different_connid`, `leave_published_for_member_missing_from_sync`) | order-dependent: pass in isolation; native repro shows correct server behavior |
| echo.ably.io embedded-JWT rest tests (5) | need external echo service token shapes (embedded x-ably-token) |
| AIT `multiple subscribers receive the same stream` | test races subscriber attach vs first publish without awaiting attachment; 5/5 green isolated |

## Reproducing

```sh
# one-time: clone + patch + build the pinned ably-js (idempotent)
./scripts/ably-poc-setup.sh

# full gate (~20 min): native Go suites + ably-go conformance +
# the 32-suite ably-js sweep
make ably-poc-test

# AIT SDK integration suites (server on :8081)
cd .working/ait-pinned && CI=true VITE_ABLY_ENV=local \
  VITE_ABLY_API_KEY="poc.key0:secret_key0_0123456789abcdef" \
  corepack pnpm@11.3.0 exec vitest run --config vitest.config.integration.ts

# e2e token-streaming demo with mid-stream reconnect
cd .working/ait-pinned && CI=true corepack pnpm@11.3.0 exec tsx e2e-centrifugo-demo.ts
```

## Spec points cited by green tests

CD2, CD2c, CHD1, RSA4a, RSA4b, RSA5, RSA6, RSA8a, RSA8c, RSA8e, RSA9d, RSC16,
RSC19, RSL1, RSL1i, RSL1k2, RSL1k5, RSL1m2, RSL1n, RSL2, RSL2b, RSL4, RSL6a1,
RSL8, RSL11, RSL14, RSL15, RSP3a1, RSP3a2, RSP3a3, RTC1a, RTC8, RTL2i, RTL4,
RTL4c, RTL4c1, RTL4d, RTL4j, RTL4k, RTL4k1, RTL4l, RTL4m, RTL6, RTL6b, RTL6c,
RTL6g1, RTL6g2, RTL6g4, RTL7c, RTL7f, RTL12, RTL15a, RTL15b, RTL16, RTL32,
RTN13, RTN14a, RTN15b, RTN15c, RTN15e, RTN15g, RTN15g1, RTN15h, RTN16, RTN16d,
RTN16f, RTN21, RTN22, RTP1–RTP16, TC1, TD5, TK2a, TK2b, TM2s, TM5, TO3l8, TR4s
(plus the AIT conformance + e2e markers; Phase 3 adds RSA17, RSC6/TS12, RSC22,
BAR1, RTC8a1, RTP4).
