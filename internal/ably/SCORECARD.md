# Ably-on-Centrifugo PoC — conformance scorecard

> Snapshot 2026-06-11. Source of truth: `.working/ably-centrifugo-poc/allowlist.json`
> (per-test tracker, git-excluded) and the milestone log in PROGRESS.md. The
> reproducible gate is `make ably-poc-test` (native Go suites + ably-go conformance
> mirrors + the ably-js acceptance sweep below). All acceptance runs use **unmodified
> SDKs**: ably-js 2.22.1 (pinned), ably-go v1.4.1 (conformance mirrors),
> @ably/ai-transport 0.2.0 (its own integration suites).

## Headline

- **90 allowlisted test entries green in BOTH wire encodings** (JSON + msgpack)
  across milestones M0–M8; every attemptable pinned test passes.
- **13 ably-js suites fully green non-comet** (the `make ably-poc-test` sweep):
  realtime channel, resume, auth, connection, history, updates-deletes;
  rest message, history, presence, time, request, updates-deletes; encoding.
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

## Known-failure categorizations (not regressions; tracked, never hidden)

| Pattern | Why |
|---|---|
| comet / non-WS transports | WS-only PoC scope |
| `publish` / `publish emoji string` (realtime/message) | test channel names embed `JSON.stringify(opts)` — `:` collides with centrifuge's namespace separator (102 unknown channel); never green; M9 divergence note |
| `subscribes to filtered channel` | server-side message filtering not in scope |
| presence trio (`multiple_pending`, `presence_auto_reenter_different_connid`, `leave_published_for_member_missing_from_sync`) | order-dependent: pass in isolation; native repro shows correct server behavior |
| echo.ably.io embedded-JWT rest tests (5) | need external echo service token shapes (embedded x-ably-token) |
| AIT `multiple subscribers receive the same stream` | test races subscriber attach vs first publish without awaiting attachment; 5/5 green isolated |

## Reproducing

```sh
# full gate (~15 min; needs node + .working/ably-js-pinned with node_modules)
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
RTN13, RTN15b, RTN15c, RTN15e, RTN15g, RTN15g1, RTN15h, RTN16, RTN16d, RTN16f,
RTN21, RTN22, RTP1–RTP16, TC1, TD5, TK2a, TK2b, TM2s, TM5, TO3l8, TR4s
(plus the AIT conformance + e2e markers).
