# Ably-on-Centrifugo PoC — known divergences from the Ably service

> Snapshot 2026-06-11. Every deliberate behavioral difference between this
> adapter and the production Ably service, with the posture taken and what
> production-hardening would change. Single-node PoC throughout: the broker
> is centrifuge's memory engine; multi-node notes name the path.

## Protocol & channels

1. **WebSocket only.** Comet/XHR transports are out of scope; comet test
   variants fail by policy.
2. **Channel names containing `:` outside known namespaces** collide with
   centrifuge's namespace separator and fail to attach (102 → channel error).
   Namespaces in the dev config: `persisted`, `mutable`, `ai`. Production
   would need a bijective channel-name encoding at every adapter boundary or
   namespace-free channel resolution.
3. **Multi-message publishes are delivered as N single-message frames**, each
   with its own channelSerial (`Message.serial` is always `<cs>:000`). Real
   Ably delivers one frame per atomic batch with `<cs>:<idx>` serials. Cursor
   semantics stay exact (serial↔publication is 1:1); mid-batch NACK leaves the
   already-published prefix unrolled (the NACK is honest; an SDK retry may
   duplicate the prefix until batch atomicity lands).
4. **Mode-name coverage:** ATTACH params.modes parsing recognizes
   presence/publish/subscribe/presence_subscribe only; annotation/object mode
   names degrade to the unrestricted default grant (fail-open; capability
   checks still gate operations).
5. **Server-side message filtering** (`subscribes to filtered channel`) is not
   implemented.

## Serials & continuity

6. **Mint and broker append are not atomic.** Near-simultaneous publishers can
   invert serial order vs offset order. Consequence honored everywhere:
   resume cursors resolve by serial→publication LOOKUP, never lexicographic
   filtering. Real Ably serializes mint+append.
7. **Cursor resolution scans retained history** (≤1000 deep) per
   cursor-bearing ATTACH — O(retention); production wants an offset index.
8. **A presence-event serial used as a resume cursor is unresolvable** (live
   presence publications are history-free): continuity is treated as lost
   (fresh attach, no RESUMED) — Ably re-syncs presence on resume anyway.
9. **attachSerial can lag after trailing presence events** (ATTACHED reads
   the newest retained MESSAGE publication).

## Recovery & auth

10. **RTN16-lite recover is unverified.** A UUID-shaped recover claim is
    adopted without a connection registry: connectionId is attribution
    metadata only (capability/clientId checks are unaffected), but a claimant
    can publish/enter presence attributed to the claimed id, suppress echo of
    it, and cancel its pending presence-grace LEAVE. Malformed claims are
    rejected with 80018 on the first CONNECTED. Cursor-less attaches on a
    recovered connection (or with ATTACH_RESUME) are granted RESUMED on the
    claim alone. Production: signed/verifiable recovery keys + connection
    state store.
11. **Token edge cases:** exp-less Ably-JWTs never expire; bare `x*` resource
    prefixes in capabilities are matched loosely; TM2h connectionKeys are
    attributed without verifying the connection exists (no per-id client
    lookup in centrifuge).
12. **RSA9d hygiene** is implemented (timestamp ±15min, nonce burn after MAC,
    24h ttl cap) but the nonce cache is in-memory per node.

## Presence

13. **The presence member store is adapter-owned, single-node** (centrifuge's
    node-level presence mutators are unexported). Multi-node needs the
    PresenceManager interface threaded through the app wiring.
14. **Presence history rides a client-unreachable shadow channel**
    (`:presence:<ch>`), REST-only, no pagination Link headers.

## Mutable messages (AIT)

15. **The mutableMessages rule is a namespace convention** (`mutable:`/`ai:`
    prefixes), standing in for the per-channel-rule flag a provisioning API
    would set. (The ably-js suites run with ABLY_TEST_STATIC_APP=1 and the
    AIT suites mint `mutable:` names natively, so no /apps provisioning shim
    was needed.)
16. **The materialized store is in-memory and never evicts.** Multi-node and
    retention would back it with centrifuge's MapBroker (keyed state +
    stream, memory/Redis) plus an append/version layer (MapBroker is
    replace-only).
17. **Append concatenates only plain strings** (both encoding chains empty);
    same-chain encoded payloads replace rather than byte-concatenate (real
    Ably appends decoded bytes). AIT only appends plain token deltas.
18. **Mutation ops commit store state before the broker publish**; a publish
    failure NACKs the client with state already moved (single-node: publish
    failures are node-shutdown-only).
19. **GET …/versions returns a single page** (no Link pagination).
20. **Concurrent mutations on one serial:** version list order = apply order
    under the store mutex; the final state's version may carry the lower
    versionSerial when racers interleave (same class as #6).

## REST & history

21. **maxMessageSize accounting is marginally stricter than Ably's** (summed
    marshaled envelopes after normalization, including ~33% base64 inflation
    for binary, vs name+data+clientId+extras).
22. **start/end history bounds are a post-filter on the fetched window** —
    filtered-out publications still consume page capacity.
23. **untilAttach pagination re-resolves from_serial per page**: if the
    attach-point publication is evicted mid-walk, pagination ends early with
    an empty page.
24. **Client-supplied non-zero publish timestamps are honored** (SDKs never
    send them); they could distort time-window rewind scans.
25. **Batch publish (`POST /messages`) msgpack bodies** normalize through a
    generic JSON round-trip: binary message data arrives as a bare base64
    string without the `"base64"` encoding segment (the single-channel path
    handles binary canonically). No SDK sends binary batch bodies today.

## Test-observability quirks (documented, not bugs)

26. **Loopback ping floor:** HEARTBEAT echoes are delayed 1ms so ably-js's
    `responseTime > 0` assertion holds on loopback (trivially true on any
    real network).
27. **ably-go quirks** (SDK-side, wire is canonical): delta-gated receive
    decode (`realtime_channel.go:1142`), RawToString msgpack decoding
    (`ablyutil/msgpack.go:16`).
28. **Order-dependent ably-js presence trio** and the AIT
    `multiple subscribers` test race are test-side timing assumptions; both
    pass in isolation (details in SCORECARD.md).
